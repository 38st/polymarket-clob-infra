//! CLOB API credentials derived from the wallet key: an L1 EIP-712 ClobAuth
//! signature, as the official clients do.

use anyhow::{Context, Result};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use super::signing;

/// The L1 ClobAuth signature of `wallet` at timestamp `ts` (seconds).
pub fn clob_auth_signature(wallet: &signing::Wallet, ts: &str) -> Result<String> {
    use crate::abi::{encode, keccak256, Token, U256};
    let domain = keccak256(encode(&[
        Token::FixedBytes(keccak256("EIP712Domain(string name,string version,uint256 chainId)").to_vec()),
        Token::FixedBytes(keccak256("ClobAuthDomain").to_vec()),
        Token::FixedBytes(keccak256("1").to_vec()),
        Token::Uint(U256::from(137u64)),
    ]));
    let message = keccak256(encode(&[
        Token::FixedBytes(keccak256("ClobAuth(address address,string timestamp,uint256 nonce,string message)").to_vec()),
        Token::Address(wallet.address()),
        Token::FixedBytes(keccak256(ts).to_vec()),
        Token::Uint(U256::from(0u64)),
        Token::FixedBytes(keccak256("This message attests that I control the given wallet").to_vec()),
    ]));
    let mut preimage = vec![0x19, 0x01];
    preimage.extend(domain);
    preimage.extend(message);
    Ok(format!("0x{}", hex::encode(wallet.sign_hash(&keccak256(preimage))?)))
}

/// (API key, secret, passphrase) for the wallet: the existing ones, or new ones
/// if the wallet has none yet. Nothing secret is logged.
pub async fn derive_api_creds(
    http: &reqwest::Client,
    wallet: &signing::Wallet,
    clob_url: &str,
) -> Result<(String, String, String)> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs().to_string();
    let sig = clob_auth_signature(wallet, &ts)?;
    let address = wallet.address().to_checksum(None);
    let headers = |b: reqwest::RequestBuilder| {
        b.header("POLY_ADDRESS", &address)
            .header("POLY_SIGNATURE", &sig)
            .header("POLY_TIMESTAMP", &ts)
            .header("POLY_NONCE", "0")
    };
    let clob = clob_url.trim_end_matches('/');
    // A refusal (a rate limit, an outage) is an error, not an answer without
    // a key: only a wallet the venue says has none gets new ones.
    let derived = answer(headers(http.get(format!("{clob}/auth/derive-api-key"))).send().await?).await?;
    let resp = match derived_key(derived)? {
        Derived::Key(resp) => resp,
        Derived::NoKey => create(headers(http.post(format!("{clob}/auth/api-key")))).await?,
    };
    let field = |k: &str| {
        resp[k]
            .as_str()
            .map(str::to_string)
            .with_context(|| format!("API key derivation returned no {}", k))
    };
    Ok((field("apiKey")?, field("secret")?, field("passphrase")?))
}

/// What the venue's derivation answer says about the wallet's API key.
#[derive(Debug)]
enum Derived {
    /// The wallet's key (the answer, with its secret and passphrase).
    Key(Value),
    /// The venue's answer for a wallet without keys (HTTP 400 "Could not
    /// derive api key!", as its clients handle it): new ones are created.
    NoKey,
}

/// derive-api-key's answer as a key, a wallet without one, or an error. A 2xx
/// answer without an `apiKey` (`{}`, a renamed field) created new keys: a
/// new key needs the owner's OK, so only the venue's exact "no key" answer
/// creates one, and any other answer refuses the start.
fn derived_key(derived: std::result::Result<Value, AuthRefused>) -> Result<Derived> {
    match derived {
        Ok(resp) if resp["apiKey"].as_str().is_some() => Ok(Derived::Key(resp)),
        Ok(_) => anyhow::bail!(
            "API key derivation answered without an apiKey; no new key is created (only a wallet the venue \
             says has none gets one)"
        ),
        Err(refused) if refused.no_key => Ok(Derived::NoKey),
        Err(refused) => Err(refused.into()),
    }
}

/// A refused API key request: its status, the venue's Retry-After header (a
/// rate limit), and the start of its body.
#[derive(Debug)]
pub struct AuthRefused {
    pub status: u16,
    // Read by the venue client's start-up (not in this repository).
    #[allow(dead_code)]
    pub retry_after: Option<String>,
    pub body: String,
    /// The venue's answer for a wallet without keys: HTTP 400 whose `error`
    /// is exactly "Could not derive api key!".
    pub no_key: bool,
}
impl std::fmt::Display for AuthRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "API key request refused: HTTP {} {}", self.status, self.body)
    }
}
impl std::error::Error for AuthRefused {}

/// The JSON of a 2xx answer, or the refusal.
async fn answer(response: reqwest::Response) -> Result<std::result::Result<Value, AuthRefused>> {
    let status = response.status();
    if status.is_success() {
        return Ok(Ok(response.json().await?));
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let text = response.text().await.unwrap_or_default();
    let no_key = status == reqwest::StatusCode::BAD_REQUEST
        && serde_json::from_str::<Value>(&text)
            .is_ok_and(|answer| answer["error"].as_str() == Some("Could not derive api key!"));
    let body = text.chars().take(200).collect();
    Ok(Err(AuthRefused { status: status.as_u16(), retry_after, body, no_key }))
}

async fn create(request: reqwest::RequestBuilder) -> Result<Value> {
    Ok(answer(request.send().await?).await??)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn refused(status: u16, no_key: bool) -> AuthRefused {
        AuthRefused { status, retry_after: None, body: String::new(), no_key }
    }
    /// Only the venue's exact "no key" answer creates keys: a 2xx answer
    /// without an `apiKey` refuses instead (it created new keys before).
    #[test]
    fn only_the_no_key_answer_creates_keys() {
        let key = json!({"apiKey": "k", "secret": "s", "passphrase": "p"});
        assert!(matches!(derived_key(Ok(key)).unwrap(), Derived::Key(answer) if answer["apiKey"] == "k"));
        assert!(matches!(derived_key(Err(refused(400, true))).unwrap(), Derived::NoKey));
        for answer in [json!({}), json!({"api_key": "k", "secret": "s", "passphrase": "p"}), json!({"apiKey": 5})] {
            let error = derived_key(Ok(answer)).unwrap_err();
            assert!(error.to_string().contains("no new key is created"), "{error}");
        }
        let error = derived_key(Err(refused(429, false))).unwrap_err();
        assert_eq!(error.downcast_ref::<AuthRefused>().map(|r| r.status), Some(429));
    }
}
