//! Read-only decoding of V2 OrderFilled logs. Fees are actual collateral amounts,
//! not estimates from legacy fee-rate fields. Network/finality checks are performed
//! by the caller before installing decoded facts in the accounting journal.
use crate::execution::Intent;
use anyhow::{ensure, Context, Result};
use crate::abi::U256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) const EXCHANGES: [&str; 2] = [
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
];
const EVENT: &str =
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiptFill {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub order_id: String,
    pub token_id: String,
    pub action: String,
    pub shares_units: u64,
    pub cash_units: u64,
    pub fee_units: u64,
}
impl ReceiptFill {
    pub fn key(&self) -> String {
        format!("{}:{}", self.transaction_hash, self.log_index)
    }
}

pub fn decode_order_fills(
    receipt: &Value,
    wallet: &str,
    owned: &BTreeMap<String, Intent>,
) -> Result<Vec<ReceiptFill>> {
    ensure!(
        receipt["status"].as_str() == Some("0x1"),
        "Transaction did not succeed"
    );
    let tx_hash = hash(&receipt["transactionHash"])?;
    let block_hash = hash(&receipt["blockHash"])?;
    let block_number = hex_u64(&receipt["blockNumber"])?;
    let transaction_index = hex_u64(&receipt["transactionIndex"])?;
    ensure!(
        wallet.len() == 42 && wallet.starts_with("0x") && hex::decode(&wallet[2..]).is_ok(),
        "Invalid wallet address"
    );
    let wallet_topic = format!("0x{:0>64}", wallet[2..].to_lowercase());
    let topic = format!("0x{}", hex::encode(crate::abi::keccak256(EVENT)));
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for log in receipt["logs"].as_array().context("Missing receipt logs")? {
        let address = log["address"].as_str().unwrap_or("").to_lowercase();
        if !EXCHANGES.contains(&address.as_str()) {
            continue;
        }
        let topics = log["topics"]
            .as_array()
            .context("Malformed exchange log topics")?;
        if topics
            .first()
            .and_then(|v| v.as_str())
            .map(str::to_lowercase)
            != Some(topic.clone())
        {
            continue;
        }
        ensure!(
            topics.len() == 4 && log["removed"].as_bool() != Some(true),
            "Malformed or removed fill log"
        );
        let id = hash(&topics[1])?;
        let Some(order) = owned.get(&id) else {
            continue;
        };
        ensure!(
            topics[2].as_str().map(str::to_lowercase) == Some(wallet_topic.clone()),
            "Fill owner differs from wallet"
        );
        ensure!(
            hash(&log["transactionHash"])? == tx_hash && hash(&log["blockHash"])? == block_hash,
            "Receipt/log identity mismatch"
        );
        ensure!(
            hex_u64(&log["blockNumber"])? == block_number
                && hex_u64(&log["transactionIndex"])? == transaction_index,
            "Receipt/log ordering mismatch"
        );
        let data = log["data"].as_str().context("Missing fill log data")?;
        let bytes = hex::decode(data.strip_prefix("0x").context("Invalid fill encoding")?)?;
        ensure!(bytes.len() == 7 * 32, "Invalid V2 OrderFilled data length");
        let words: Vec<_> = bytes.chunks_exact(32).map(crate::abi::u256_be).collect();
        ensure!(words[0] <= U256::from(1u64), "Unknown fill action");
        let buy = words[0].is_zero();
        let action = if buy { "BUY" } else { "SELL" };
        let token = words[1].to_string();
        ensure!(
            action == order.action && token == order.token_id,
            "Receipt fill does not match signed order"
        );
        let making = small_units(words[2])?;
        let taking = small_units(words[3])?;
        let fee = small_units(words[4])?;
        let (shares, cash) = if buy {
            (taking, making)
        } else {
            (making, taking)
        };
        ensure!(
            shares > 0 && cash > 0 && (buy || fee <= cash),
            "Invalid settled amounts"
        );
        ensure!(
            within_signed_limit(buy, shares, cash, order.price),
            "Settlement violates signed limit"
        );
        ensure!(
            shares as f64 / 1e6 <= order.size + 1e-6,
            "Settlement exceeds signed quantity"
        );
        let index = hex_u64(&log["logIndex"])?;
        ensure!(seen.insert(index), "Duplicate log index in receipt");
        result.push(ReceiptFill {
            transaction_hash: tx_hash.clone(),
            block_hash: block_hash.clone(),
            block_number,
            transaction_index,
            log_index: index,
            order_id: id,
            token_id: token,
            action: action.into(),
            shares_units: shares,
            cash_units: cash,
            fee_units: fee,
        });
    }
    result.sort_by_key(|f| f.log_index);
    Ok(result)
}
/// The contracts of OrderFilled logs in `receipt` that name an order of `owned`
/// but are not EXCHANGES: decode_order_fills never takes them as fills. Named
/// when a receipt settles none of ours (execution::ReceiptWithoutOwnedFills).
pub fn untrusted_order_fills(receipt: &Value, owned: &BTreeMap<String, Intent>) -> Vec<String> {
    let topic = format!("0x{}", hex::encode(crate::abi::keccak256(EVENT)));
    let mut found: Vec<String> = receipt["logs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|log| {
            let address = log["address"].as_str()?.to_lowercase();
            let topics = log["topics"].as_array()?;
            let filled = topics.first().and_then(Value::as_str).map(str::to_lowercase) == Some(topic.clone());
            let ours = topics.get(1).and_then(|id| hash(id).ok()).is_some_and(|id| owned.contains_key(&id));
            (filled && ours && !EXCHANGES.contains(&address.as_str())).then_some(address)
        })
        .collect();
    found.sort();
    found.dedup();
    found
}
/// The signed limit in exact collateral units: a fill settles at most one
/// cash unit beyond shares × limit (a BUY pays at most one unit more, a SELL
/// receives at most one unit less). The venue rounds a partial fill's amounts
/// to whole units: a BUY at 0.73 settled 125186 shares for 91386
/// units (exact 91385.78), 1.8e-6 above the limit as a price, which the old
/// 1e-6 price tolerance refused. The limit's units are the signer's
/// (PolymarketClient::validated_amounts rounds price × 1e6 the same way).
pub(crate) fn within_signed_limit(buy: bool, shares: u64, cash: u64, limit: f64) -> bool {
    // Only a price the signer signs, 1 to 999999 units (NaN fails both bounds).
    let units = (limit * 1e6).round();
    if !(1.0..1e6).contains(&units) {
        return false;
    }
    let exact = shares as u128 * units as u128;
    let settled = cash as u128 * 1_000_000;
    if buy {
        settled <= exact + 1_000_000
    } else {
        settled + 1_000_000 >= exact
    }
}
fn small_units(n: U256) -> Result<u64> {
    // Keep integer values exactly representable in the existing f64 projection.
    ensure!(
        n <= U256::from(1_u64 << 53),
        "Amount exceeds exact accounting range"
    );
    Ok(n.to::<u64>())
}
pub(crate) fn hash(value: &Value) -> Result<String> {
    let text = value.as_str().context("Missing chain hash")?;
    ensure!(
        text.len() == 66 && text.starts_with("0x") && hex::decode(&text[2..]).is_ok(),
        "Invalid chain hash"
    );
    Ok(text.to_lowercase())
}
pub(crate) fn hex_u64(value: &Value) -> Result<u64> {
    Ok(u64::from_str_radix(
        value
            .as_str()
            .context("Missing chain index")?
            .strip_prefix("0x")
            .context("Invalid chain index")?,
        16,
    )?)
}

/// Validate every log before attributing wallet movements across contract calls.
pub fn validated_logs(receipt: &Value) -> Result<Vec<&Value>> {
    ensure!(
        receipt["status"].as_str() == Some("0x1"),
        "Transaction did not succeed"
    );
    let transaction = hash(&receipt["transactionHash"])?;
    let block = hash(&receipt["blockHash"])?;
    let number = hex_u64(&receipt["blockNumber"])?;
    let transaction_index = hex_u64(&receipt["transactionIndex"])?;
    let mut logs: Vec<_> = receipt["logs"]
        .as_array()
        .context("Missing receipt logs")?
        .iter()
        .collect();
    let mut indices = std::collections::BTreeSet::new();
    for log in &logs {
        ensure!(
            log["removed"].as_bool() != Some(true)
                && hash(&log["transactionHash"])? == transaction
                && hash(&log["blockHash"])? == block
                && hex_u64(&log["blockNumber"])? == number
                && hex_u64(&log["transactionIndex"])? == transaction_index,
            "Receipt log identity mismatch"
        );
        ensure!(
            indices.insert(hex_u64(&log["logIndex"])?),
            "Duplicate receipt log index"
        );
    }
    logs.sort_by_key(|log| hex_u64(&log["logIndex"]).unwrap());
    Ok(logs)
}

#[cfg(test)]
pub(crate) fn test_receipt(wallet: &str, fill: &ReceiptFill) -> Value {
    use serde_json::json;
    let buy = fill.action == "BUY";
    let words = [
        if buy { U256::ZERO } else { U256::from(1u64) },
        crate::abi::u256_dec(&fill.token_id).unwrap(),
        U256::from(if buy {
            fill.cash_units
        } else {
            fill.shares_units
        }),
        U256::from(if buy {
            fill.shares_units
        } else {
            fill.cash_units
        }),
        U256::from(fill.fee_units),
        U256::ZERO,
        U256::ZERO,
    ];
    let data = format!(
        "0x{}",
        words
            .iter()
            .map(|n| crate::abi::word_hex(*n))
            .collect::<String>()
    );
    let log = json!({"address": EXCHANGES[0], "topics": [format!("0x{}", hex::encode(crate::abi::keccak256(EVENT))),
        fill.order_id, format!("0x{:0>64}", wallet[2..].to_lowercase()), format!("0x{}", "00".repeat(32))],
        "data":data,"transactionHash":fill.transaction_hash,"blockHash":fill.block_hash,
        "blockNumber":format!("0x{:x}",fill.block_number),"transactionIndex":format!("0x{:x}",fill.transaction_index),
        "logIndex":format!("0x{:x}",fill.log_index),"removed":false});
    json!({"status":"0x1","transactionHash":fill.transaction_hash,"blockHash":fill.block_hash,
        "blockNumber":format!("0x{:x}",fill.block_number),"transactionIndex":format!("0x{:x}",fill.transaction_index),"logs":[log]})
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    /// Only an OrderFilled log of an owned order from a contract outside
    /// EXCHANGES is named.
    #[test]
    fn untrusted_order_fills_name_only_owned_orders_outside_the_exchanges() {
        let wallet = format!("0x{}", "11".repeat(20));
        let fill = crate::accounting::receipt_tests::receipt("BUY", 1);
        let owned = BTreeMap::from([(
            fill.order_id.clone(),
            Intent {
                order_id: fill.order_id.clone(),
                token_id: fill.token_id.clone(),
                action: "BUY".into(),
                price: 0.4,
                size: 10.,
                filled: 4.,
                state: "terminal".into(),
                post_only: true,
                reserve_unit_cost: None,
            },
        )]);
        let mut receipt = test_receipt(&wallet, &fill);
        assert!(untrusted_order_fills(&receipt, &owned).is_empty(), "a trusted exchange's log");
        let untrusted = format!("0x{}", "9A".repeat(20));
        receipt["logs"][0]["address"] = json!(untrusted);
        assert_eq!(untrusted_order_fills(&receipt, &owned), vec![untrusted.to_lowercase()]);
        assert!(decode_order_fills(&receipt, &wallet, &owned).unwrap().is_empty());
        let mut other_order = receipt.clone();
        other_order["logs"][0]["topics"][1] = json!(format!("0x{:064x}", 5));
        assert!(untrusted_order_fills(&other_order, &owned).is_empty(), "not an owned order");
        let mut other_event = receipt.clone();
        other_event["logs"][0]["topics"][0] = json!(format!("0x{:064x}", 6));
        assert!(untrusted_order_fills(&other_event, &owned).is_empty(), "not an OrderFilled log");
        receipt["logs"] = json!([receipt["logs"][0].clone(), receipt["logs"][0].clone()]);
        assert_eq!(untrusted_order_fills(&receipt, &owned).len(), 1, "each contract once");
    }
    /// Decode one fill of `shares` for `cash` units against a signed order at
    /// `limit` (a partial fill: a BUY at 0.73 settled
    /// 125186 shares for 91386 units).
    fn decode_at(action: &str, limit: f64, shares: u64, cash: u64) -> Result<Vec<ReceiptFill>> {
        let wallet = format!("0x{}", "11".repeat(20));
        let mut fill = crate::accounting::receipt_tests::receipt(action, 1);
        (fill.shares_units, fill.cash_units, fill.fee_units) = (shares, cash, 0);
        let order = Intent {
            order_id: fill.order_id.clone(),
            token_id: fill.token_id.clone(),
            action: action.into(),
            price: limit,
            size: 10.0,
            filled: 0.0,
            state: "live".into(),
            post_only: true,
            reserve_unit_cost: None,
        };
        let owned = BTreeMap::from([(fill.order_id.clone(), order)]);
        decode_order_fills(&test_receipt(&wallet, &fill), &wallet, &owned)
    }
    fn refused(result: Result<Vec<ReceiptFill>>) -> bool {
        result.is_err_and(|e| e.to_string() == "Settlement violates signed limit")
    }
    #[test]
    fn a_buy_settles_at_most_one_cash_unit_above_its_signed_limit() {
        // Exact cost 91385.78 units: the venue's rounding to 91386 is the limit.
        let fills = decode_at("BUY", 0.73, 125_186, 91_386).unwrap();
        assert_eq!((fills[0].shares_units, fills[0].cash_units), (125_186, 91_386));
        assert!(refused(decode_at("BUY", 0.73, 125_186, 91_387)));
        assert!(refused(decode_at("BUY", 0.73, 125_186, 91_388)));
        // At an exact cost of 730000 units, one unit over settles, two do not.
        assert!(decode_at("BUY", 0.73, 1_000_000, 730_001).is_ok());
        assert!(refused(decode_at("BUY", 0.73, 1_000_000, 730_002)));
        // The other fill of that market, and a price below the limit.
        assert!(decode_at("BUY", 0.73, 5_370_371, 3_920_371).is_ok());
        assert!(decode_at("BUY", 0.73, 1_000_000, 700_000).is_ok());
        // A limit off the 1e-6 grid counts in the signer's rounded units.
        assert!(decode_at("BUY", 2.0 / 3.0, 1_000_000, 666_668).is_ok());
        assert!(refused(decode_at("BUY", 1.0 / 3.0, 1_000_000, 333_335)));
    }
    #[test]
    fn a_sell_settles_at_most_one_cash_unit_below_its_signed_limit() {
        let fills = decode_at("SELL", 0.73, 125_186, 91_385).unwrap();
        assert_eq!((fills[0].shares_units, fills[0].cash_units), (125_186, 91_385));
        assert!(refused(decode_at("SELL", 0.73, 125_186, 91_384)));
        assert!(refused(decode_at("SELL", 0.73, 125_186, 91_383)));
        assert!(decode_at("SELL", 0.73, 1_000_000, 729_999).is_ok());
        assert!(refused(decode_at("SELL", 0.73, 1_000_000, 729_998)));
        assert!(decode_at("SELL", 0.73, 1_000_000, 760_000).is_ok());
    }
    #[test]
    fn a_limit_that_is_not_a_price_refuses_every_settlement() {
        for limit in [f64::NAN, f64::INFINITY, 0.0, -0.73, 1e-9, 1.0, 1e308] {
            assert!(refused(decode_at("SELL", limit, 1_000_000, 730_000)), "{limit}");
            assert!(refused(decode_at("BUY", limit, 1_000_000, 1)), "{limit}");
        }
    }
    #[test]
    fn fees_come_from_exact_v2_cash_logs_and_wrong_identity_is_rejected() {
        for buy in [true, false] {
            let wallet = format!("0x{}", "11".repeat(20));
            let id = format!("0x{}", "22".repeat(32));
            let tx = format!("0x{}", "33".repeat(32));
            let block = format!("0x{}", "44".repeat(32));
            let order = Intent {
                order_id: id.clone(),
                token_id: "123".into(),
                action: if buy { "BUY" } else { "SELL" }.into(),
                price: 0.4,
                size: 10.0,
                filled: 0.0,
                state: "live".into(),
                post_only: false,
                reserve_unit_cost: None,
            };
            let owned = BTreeMap::from([(id.clone(), order)]);
            let words = [
                if buy { 0 } else { 1 },
                123,
                if buy { 4_000_000 } else { 10_000_000 },
                if buy { 10_000_000 } else { 4_000_000 },
                168_000,
                0,
                0,
            ];
            let data = format!(
                "0x{}",
                words
                    .iter()
                    .map(|n| format!("{n:064x}"))
                    .collect::<String>()
            );
            let log = json!({"address":EXCHANGES[0],"topics":[format!("0x{}",hex::encode(crate::abi::keccak256(EVENT))),
                id,format!("0x{:0>64}",&wallet[2..]),format!("0x{}","00".repeat(32))],"data":data,
                "transactionHash":tx,"blockHash":block,"blockNumber":"0xa","transactionIndex":"0x1","logIndex":"0x7","removed":false});
            let mut receipt = json!({"status":"0x1","transactionHash":tx,"blockHash":block,"blockNumber":"0xa","transactionIndex":"0x1","logs":[log]});
            let fills = decode_order_fills(&receipt, &wallet, &owned).unwrap();
            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].shares_units, 10_000_000);
            assert_eq!(fills[0].cash_units, 4_000_000);
            assert_eq!(fills[0].fee_units, 168_000);
            receipt["logs"][0]["removed"] = json!(true);
            assert!(decode_order_fills(&receipt, &wallet, &owned).is_err());
            receipt["logs"][0]["removed"] = json!(false);
            receipt["logs"][0]["topics"][2] = json!(format!("0x{}", "ff".repeat(32)));
            assert!(decode_order_fills(&receipt, &wallet, &owned).is_err());
        }
    }
}
