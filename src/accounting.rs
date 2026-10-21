//! Durable execution facts and an average-cost portfolio projection.
//! Missing fees or settlement evidence remain unknown, never silently zero.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Unsettled fills: their (transaction, order) groups, and orders with no transaction yet.
type Unsettled = (BTreeSet<(String, String)>, BTreeSet<String>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFill {
    #[serde(default)]
    pub transaction_hash: Option<String>,
    pub trade_id: String,
    pub order_id: String,
    pub token_id: String,
    pub action: String,
    pub size: f64,
    pub price: f64,
    pub matched_at_ms: u64,
    /// The venue's trade status, or VOID: reported FAILED and decided at a
    /// restart, its one settlement transaction reverted and nothing of it in
    /// the wallet (PolymarketClient::recover_at_startup). A void execution
    /// moved nothing: no shares, no cost, no P&L.
    pub status: String,
    /// Cash fees increase BUY basis or reduce SELL proceeds.
    pub fee_cash: Option<f64>,
    /// Share-denominated BUY fees reduce the net inventory received.
    pub fee_shares: Option<f64>,
    /// "maker" or a verified transaction receipt identifier.
    pub fee_evidence: Option<String>,
    /// Every other settlement transaction the venue reported for it (a retry,
    /// a late notice of an earlier attempt): a FAILED execution with any is
    /// never voided.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub other_transactions: Vec<String>,
}
impl ExecutionFill {
    pub fn key(&self) -> String {
        format!("{}:{}", self.trade_id, self.order_id)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.trade_id.is_empty() && !self.order_id.is_empty() && !self.token_id.is_empty(),
            "Missing execution identity"
        );
        ensure!(
            matches!(self.action.as_str(), "BUY" | "SELL"),
            "Invalid execution action"
        );
        ensure!(
            self.size.is_finite()
                && self.size > 0.0
                && self.price.is_finite()
                && self.price > 0.0
                && self.price < 1.0,
            "Invalid execution amounts"
        );
        ensure!(self.matched_at_ms > 0, "Missing execution time");
        for tx in self.transaction_hash.iter().chain(&self.other_transactions) {
            ensure!(is_word(tx), "Invalid execution transaction hash");
        }
        ensure!(
            matches!(
                self.status.as_str(),
                "MATCHED" | "MINED" | "CONFIRMED" | "RETRYING" | "FAILED" | "VOID"
            ),
            "Invalid execution status"
        );
        for fee in [self.fee_cash, self.fee_shares].into_iter().flatten() {
            ensure!(fee.is_finite() && fee >= 0.0, "Invalid execution fee");
        }
        ensure!(
            self.fee_shares.unwrap_or(0.0) < self.size,
            "Fee consumes entire execution"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Settlement {
    #[serde(default)]
    pub block_hash: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub chain_order: Option<ChainOrder>,
    pub event_id: String,
    pub transaction_hash: String,
    pub token_id: String,
    pub quantity: f64,
    pub cash_received: f64,
    pub fee_cash: f64,
    pub confirmed_at_ms: u64,
}

impl Settlement {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.event_id.is_empty()
                && !self.token_id.is_empty()
                && self.transaction_hash.starts_with("0x")
                && self.transaction_hash.len() == 66
                && self.confirmed_at_ms > 0,
            "Missing confirmed settlement identity"
        );
        ensure!(
            self.quantity.is_finite()
                && self.quantity > 0.0
                && self.cash_received.is_finite()
                && self.cash_received >= 0.0
                && self.fee_cash.is_finite()
                && self.fee_cash >= 0.0,
            "Invalid settlement amounts"
        );
        for hash in [&self.block_hash, &self.condition_id].into_iter().flatten() {
            ensure!(
                hash.len() == 66 && hash.starts_with("0x") && hex::decode(&hash[2..]).is_ok(),
                "Invalid settlement chain identity"
            );
        }
        ensure!(
            self.cash_received <= self.quantity + 1e-6,
            "Settlement payout exceeds collateral backing"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChainOrder {
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    /// Multiple outcomes may be removed by one ERC1155 batch log.
    #[serde(default)]
    pub event_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainCursor {
    pub block_number: u64,
    pub block_hash: String,
}

fn is_word(value: &str) -> bool {
    value.len() == 66 && value.starts_with("0x") && hex::decode(&value[2..]).is_ok()
}

/// A binary condition's payout vector, read from the CTF at finalized blocks,
/// and the first block at which it was reported. Inventory still held then is
/// valued at this payout, so a hold-to-resolution loss is realized when the
/// market resolves, not when (or whether) the wallet later redeems.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resolution {
    pub condition_id: String,
    /// Outcome token IDs in outcome-slot order, recomputed on chain.
    pub tokens: [String; 2],
    pub numerators: [u64; 2],
    pub denominator: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub resolved_at_ms: u64,
}

impl Resolution {
    fn validate(&self) -> Result<()> {
        ensure!(
            is_word(&self.condition_id) && is_word(&self.block_hash),
            "Invalid resolution chain identity"
        );
        ensure!(
            self.tokens.iter().all(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()))
                && self.tokens[0] != self.tokens[1],
            "Invalid resolution outcome tokens"
        );
        ensure!(
            self.denominator > 0
                && self.numerators[0].checked_add(self.numerators[1]) == Some(self.denominator),
            "Invalid resolution payout vector"
        );
        ensure!(
            self.block_number > 0 && self.resolved_at_ms > 0,
            "Resolution lacks a finalized block"
        );
        Ok(())
    }
}

/// A value the ledgers that copy it share until one of them changes it: a
/// clone shares it (an Arc), and a mutable access (DerefMut) first copies it
/// when it is shared (Arc::make_mut), so no ledger ever sees another's
/// change. Copying a ledger therefore copies none of its maps, and a change
/// copies only the maps it touches: the maps it did not touch are still the
/// very ones its original holds (Shared::same), which is how
/// ExecutionJournal::commit_ledger diffs only the touched maps. It encodes,
/// compares and prints as the value itself.
#[derive(Default)]
pub struct Shared<T>(std::sync::Arc<T>);

impl<T> Shared<T> {
    /// Whether both share one value: then they are equal (nothing can change a
    /// shared value; the first mutable access copies it).
    pub fn same(a: &Self, b: &Self) -> bool {
        std::sync::Arc::ptr_eq(&a.0, &b.0)
    }
}

impl<T: Clone> Shared<T> {
    /// The value, copied only when it is shared.
    pub fn into_inner(self) -> T {
        std::sync::Arc::try_unwrap(self.0).unwrap_or_else(|shared| (*shared).clone())
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> std::ops::Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Clone> std::ops::DerefMut for Shared<T> {
    fn deref_mut(&mut self) -> &mut T {
        std::sync::Arc::make_mut(&mut self.0)
    }
}

impl<T> From<T> for Shared<T> {
    fn from(value: T) -> Self {
        Self(std::sync::Arc::new(value))
    }
}

impl<T: PartialEq> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        *self.0 == *other.0
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: Serialize> Serialize for Shared<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Shared<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self::from)
    }
}

impl<'a, T> IntoIterator for &'a Shared<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        (&*self.0).into_iter()
    }
}

fn is_empty_map<T>(map: &Shared<BTreeMap<String, T>>) -> bool {
    map.is_empty()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountingLedger {
    #[serde(default)]
    pub chain_cursor: Option<ChainCursor>,
    /// Canonical finalized block timestamps; legacy journals backfill by receipt.
    #[serde(default)]
    pub block_times_ms: Shared<BTreeMap<String, u64>>,
    /// Production journals require chain evidence; paper/legacy projections do not.
    #[serde(default)]
    pub require_receipts: bool,
    #[serde(default)]
    pub receipt_fills: Shared<BTreeMap<String, crate::receipts::ReceiptFill>>,
    pub fills: Shared<BTreeMap<String, ExecutionFill>>,
    pub settlements: Shared<BTreeMap<String, Settlement>>,
    /// Condition IDs the exchange reported for owned tokens (the trade `market`
    /// field). Untrusted until a chain checkpoint recomputes the token IDs.
    #[serde(default)]
    pub condition_claims: Shared<BTreeMap<String, BTreeSet<String>>>,
    /// Verified resolutions, keyed by condition ID.
    #[serde(default)]
    pub resolutions: Shared<BTreeMap<String, Resolution>>,
    /// Totals of history moved to sealed archive segments (execution.rs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<ArchiveBase>,
    /// FAILED reports that named no recorded execution, by trade ID, with
    /// every settlement transaction they named: the execution, once recorded,
    /// is FAILED with them (ExecutionJournal::record_event); until then a
    /// restart refuses to start.
    #[serde(default, skip_serializing_if = "is_empty_map")]
    pub failed_notices: Shared<BTreeMap<String, BTreeSet<String>>>,
    /// Reports of a trade reported FAILED that could not be recorded, by trade
    /// ID, with every valid settlement transaction they named: nothing clears
    /// one but reconciliation by hand, and a restart refuses while one is here.
    #[serde(default, skip_serializing_if = "is_empty_map")]
    pub unrecorded_reports: Shared<BTreeMap<String, BTreeSet<String>>>,
}

/// Cash flow and realized P&L of archived history. Only flat, fully
/// receipt-verified tokens are archived, so archived positions contribute
/// nothing else to the live projection.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ArchiveSummary {
    pub cash_delta: f64,
    pub realized_pnl: f64,
    pub fees_cash: f64,
    /// Realized P&L per UTC day index (confirmation time / 86_400_000).
    pub daily_realized: BTreeMap<u64, f64>,
}

impl ArchiveSummary {
    pub fn add(&mut self, other: &ArchiveSummary) {
        self.cash_delta += other.cash_delta;
        self.realized_pnl += other.realized_pnl;
        self.fees_cash += other.fees_cash;
        for (day, value) in &other.daily_realized {
            *self.daily_realized.entry(*day).or_default() += value;
        }
    }
    pub fn matches(&self, other: &ArchiveSummary) -> bool {
        let close = |a: f64, b: f64| (a - b).abs() <= 1e-6;
        close(self.cash_delta, other.cash_delta)
            && close(self.realized_pnl, other.realized_pnl)
            && close(self.fees_cash, other.fees_cash)
            && self.daily_realized.len() == other.daily_realized.len()
            && self
                .daily_realized
                .iter()
                .all(|(day, v)| other.daily_realized.get(day).is_some_and(|o| close(*v, *o)))
    }
}

/// One sealed segment file, `<wallet>.archive/<seq:06>.json` next to the journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArchiveSegmentRef {
    pub seq: u64,
    pub sha256: String,
    pub bytes: u64,
    pub orders: u64,
    pub summary: ArchiveSummary,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ArchiveBase {
    pub segments: Vec<ArchiveSegmentRef>,
    pub totals: ArchiveSummary,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Position {
    pub quantity: f64,
    pub cost_basis: f64,
    pub realized_pnl: f64,
    pub fees_cash: f64,
    pub fees_shares: f64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Portfolio {
    pub positions: BTreeMap<String, Position>,
    pub cash_delta: f64,
}

/// One economic disposal, derived alongside portfolio basis, never from a mark.
#[derive(Clone, Debug, Serialize)]
pub struct Realization {
    pub event_id: String,
    pub token_id: String,
    pub transaction_hash: String,
    pub order_id: Option<String>,
    pub condition_id: Option<String>,
    pub kind: String,
    pub confirmed_at_ms: Option<u64>,
    pub quantity: f64,
    pub cash_received: f64,
    pub fee_cash: f64,
    pub cost_basis: f64,
    pub realized_pnl: f64,
}

#[cfg(test)]
thread_local! {
    /// Tests: how many times this thread projected a ledger (portfolio,
    /// realizations), so a test can show an unchanged ledger is not replayed.
    pub(crate) static PROJECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
fn count_projection() {
    PROJECTIONS.with(|count| count.set(count.get() + 1));
}
#[cfg(test)]
thread_local! {
    /// Tests: executions compared with a receipt in receipt_projection.
    pub(crate) static IDENTITY_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
fn count_identity_check() {
    IDENTITY_CHECKS.with(|count| count.set(count.get() + 1));
}

impl AccountingLedger {
    pub fn install_block_time(&mut self, hash: &str, timestamp: u64) -> Result<bool> {
        ensure!(timestamp > 0, "Missing finalized block timestamp");
        if let Some(prior) = self.block_times_ms.get(hash) {
            ensure!(
                *prior == timestamp,
                "Conflicting finalized block timestamps"
            );
            return Ok(false);
        }
        self.block_times_ms.insert(hash.to_owned(), timestamp);
        Ok(true)
    }

    /// What is not fully settled: (transaction, order) groups with a fill
    /// MATCHED, MINED or RETRYING, or (with receipts required) CONFIRMED but
    /// its receipt not read yet; and orders with a settling fill whose
    /// transaction is not known yet (it may share any of that order's
    /// receipts). A RETRYING fill's transaction failed and the venue settles it
    /// again: until then it waits with the transaction it names, a group that
    /// holds no receipt of its order, so nothing verified leaves the view; a
    /// retry that lands where a sibling's receipt is already read refuses the
    /// projection (its receipt holds more than the confirmed executions) until
    /// the fill is confirmed there. One that names no transaction, or one
    /// whose order has a receipt in it, would take verified fills, and the
    /// losses they realized, out of the settled view: that, and a failed settlement, is an error; it needs
    /// reconciliation first.
    fn unsettled(&self) -> Result<Unsettled> {
        let receipted: BTreeSet<(&str, &str)> = self
            .receipt_fills
            .values()
            .map(|r| (r.transaction_hash.as_str(), r.order_id.as_str()))
            .collect();
        let (mut groups, mut orders) = (BTreeSet::new(), BTreeSet::new());
        for fill in self.fills.values() {
            let group = fill.transaction_hash.as_ref().map(|tx| (tx.clone(), fill.order_id.clone()));
            match fill.status.as_str() {
                "CONFIRMED" => {
                    if let Some(group) = group {
                        if self.require_receipts && !receipted.contains(&(group.0.as_str(), group.1.as_str())) {
                            groups.insert(group);
                        }
                    }
                }
                "RETRYING" => match group {
                    Some(group) if !receipted.contains(&(group.0.as_str(), group.1.as_str())) => {
                        groups.insert(group);
                    }
                    _ => anyhow::bail!("Execution settlement is RETRYING; reconciliation required"),
                },
                "MATCHED" | "MINED" => match group {
                    Some(group) => {
                        groups.insert(group);
                    }
                    None => {
                        orders.insert(fill.order_id.clone());
                    }
                },
                "VOID" => {}
                other => anyhow::bail!("Execution settlement is {}; reconciliation required", other),
            }
        }
        Ok((groups, orders))
    }

    /// The ledger without whatever is not fully settled (see unsettled: the
    /// fills and receipts of those groups and orders) and without fills that
    /// have no transaction yet: what a market maker that publishes while fills
    /// settle must keep consistent.
    pub fn settled_view(&self) -> Result<AccountingLedger> {
        let (groups, orders) = self.unsettled()?;
        let mut view = self.clone();
        view.fills.retain(|_, fill| {
            fill.status == "CONFIRMED"
                && !orders.contains(&fill.order_id)
                && fill
                    .transaction_hash
                    .as_ref()
                    .is_some_and(|tx| !groups.contains(&(tx.clone(), fill.order_id.clone())))
        });
        view.receipt_fills.retain(|_, r| {
            !orders.contains(&r.order_id) && !groups.contains(&(r.transaction_hash.clone(), r.order_id.clone()))
        });
        Ok(view)
    }

    /// Evidence still to read while fills settle: the receipt transactions of
    /// CONFIRMED fills not read yet and of receipts lacking block times. Fills
    /// still MATCHED, MINED or RETRYING are skipped, not an error (see
    /// pending_evidence).
    pub fn settled_pending_evidence(&self) -> Result<BTreeSet<String>> {
        self.unsettled()?;
        let mut pending = BTreeSet::new();
        for fill in self.fills.values().filter(|fill| fill.status == "CONFIRMED") {
            let tx = fill
                .transaction_hash
                .as_ref()
                .context("Execution transaction is not yet known; replay required")?;
            if !self
                .receipt_fills
                .values()
                .any(|r| &r.transaction_hash == tx && r.order_id == fill.order_id)
            {
                pending.insert(tx.clone());
            }
        }
        pending.extend(
            self.receipt_fills
                .values()
                .filter(|r| !self.block_times_ms.contains_key(&r.block_hash))
                .map(|r| r.transaction_hash.clone()),
        );
        Ok(pending)
    }

    /// Whether any fill is not yet confirmed on chain (a void one never is).
    pub fn has_settling(&self) -> bool {
        self.fills.values().any(|fill| !matches!(fill.status.as_str(), "CONFIRMED" | "VOID"))
    }

    pub fn pending_evidence(&self) -> Result<std::collections::BTreeSet<String>> {
        let mut pending = self.pending_receipts()?;
        pending.extend(
            self.receipt_fills
                .values()
                .filter(|r| !self.block_times_ms.contains_key(&r.block_hash))
                .map(|r| r.transaction_hash.clone()),
        );
        Ok(pending)
    }

    pub fn realizations(&self) -> Result<Vec<Realization>> {
        ensure!(
            self.require_receipts,
            "Realized reporting requires receipts"
        );
        Ok(self.receipt_projection()?.1)
    }

    pub fn install_receipt_fills(
        &mut self,
        fills: &[crate::receipts::ReceiptFill],
    ) -> Result<bool> {
        // Validate the whole batch before changing memory or writing a checkpoint.
        let mut next = self.receipt_fills.clone();
        let mut changed = false;
        for fill in fills {
            if let Some(prior) = next.get(&fill.key()) {
                ensure!(prior == fill, "Conflicting finalized receipt evidence");
            } else {
                next.insert(fill.key(), fill.clone());
                changed = true;
            }
        }
        self.receipt_fills = next;
        Ok(changed)
    }

    pub fn pending_receipts(&self) -> Result<std::collections::BTreeSet<String>> {
        let mut result = std::collections::BTreeSet::new();
        for fill in self.fills.values().filter(|fill| fill.status != "VOID") {
            ensure!(
                fill.status == "CONFIRMED",
                "Execution is awaiting final confirmation"
            );
            let tx = fill
                .transaction_hash
                .as_ref()
                .context("Execution transaction is not yet known; replay required")?;
            if !self
                .receipt_fills
                .values()
                .any(|r| &r.transaction_hash == tx && r.order_id == fill.order_id)
            {
                result.insert(tx.clone());
            }
        }
        Ok(result)
    }

    /// The ledger projects (portfolio); with receipt accounting, its
    /// realizations from the same projection.
    pub fn projection_facts(&self) -> Result<Option<Vec<Realization>>> {
        if self.require_receipts {
            return Ok(Some(self.receipt_projection()?.1));
        }
        self.portfolio()?;
        Ok(None)
    }

    fn receipt_projection(&self) -> Result<(Portfolio, Vec<Realization>)> {
        #[cfg(test)]
        count_projection();
        // The exchange can report several trade IDs for one OrderFilled log.
        // Compare aggregates, then project the chain events once. Allocating a
        // receipt's fee independently to each trade would multiply that fee.
        type Group = (String, String);
        // Each (transaction, order)'s executions, compared with its receipts below.
        let mut executions: BTreeMap<Group, Vec<&ExecutionFill>> = BTreeMap::new();
        for fill in self.fills.values().filter(|fill| fill.status != "VOID") {
            fill.validate()?;
            ensure!(
                fill.status == "CONFIRMED",
                "Unconfirmed execution needs reconciliation"
            );
            let tx = fill
                .transaction_hash
                .as_ref()
                .context("Execution has no settlement transaction")?;
            let key = (tx.clone(), fill.order_id.clone());
            executions.entry(key).or_default().push(fill);
        }
        let mut settled: BTreeMap<Group, (u64, u64)> = BTreeMap::new();
        let mut chain_slots = std::collections::BTreeSet::new();
        let mut block_hashes = BTreeMap::new();
        let mut transaction_blocks = BTreeMap::new();
        for receipt in self.receipt_fills.values() {
            let identity = (
                receipt.block_number,
                receipt.transaction_index,
                receipt.block_hash.clone(),
            );
            if let Some(prior) =
                transaction_blocks.insert(receipt.transaction_hash.clone(), identity.clone())
            {
                ensure!(
                    prior == identity,
                    "One transaction has conflicting chain positions"
                );
            }
            let group = settled
                .entry((receipt.transaction_hash.clone(), receipt.order_id.clone()))
                .or_default();
            group.0 = group
                .0
                .checked_add(receipt.shares_units)
                .context("Share amount overflow")?;
            group.1 = group
                .1
                .checked_add(receipt.cash_units)
                .context("Cash amount overflow")?;
            ensure!(
                chain_slots.insert((receipt.block_number, receipt.log_index, 0u32)),
                "Duplicated chain log position"
            );
            if let Some(hash) = block_hashes.insert(receipt.block_number, &receipt.block_hash) {
                ensure!(
                    hash == &receipt.block_hash,
                    "Conflicting canonical block evidence"
                );
            }
            // Every execution has its transaction here (checked above).
            let key = (receipt.transaction_hash.clone(), receipt.order_id.clone());
            for f in executions.get(&key).into_iter().flatten() {
                #[cfg(test)]
                count_identity_check();
                ensure!(
                    f.token_id == receipt.token_id && f.action == receipt.action,
                    "Execution/receipt identity mismatch"
                );
            }
        }
        ensure!(
            executions.len() == settled.len(),
            "Trade replay and receipt coverage differ"
        );
        for (key, fills) in &executions {
            let (shares, collateral) = settled
                .get(key)
                .context("Missing finalized execution receipt")?;
            ensure!(
                reports_settle_as_receipts(fills, *shares, *collateral),
                "Exchange and receipt execution amounts differ"
            );
        }
        enum Event<'a> {
            Fill(&'a crate::receipts::ReceiptFill),
            Settlement(&'a Settlement),
            Resolution(&'a Resolution),
        }
        let mut events = Vec::new();
        for r in self.receipt_fills.values() {
            events.push((
                ChainOrder {
                    event_index: 0,
                    block_number: r.block_number,
                    transaction_index: r.transaction_index,
                    log_index: r.log_index,
                },
                Event::Fill(r),
            ));
        }
        for s in self.settlements.values() {
            s.validate()?;
            let position = s
                .chain_order
                .context("Settlement lacks verified chain ordering")?;
            ensure!(
                chain_slots.insert((
                    position.block_number,
                    position.log_index,
                    position.event_index
                )),
                "Duplicated settlement log position"
            );
            let block_hash = s
                .block_hash
                .as_ref()
                .context("Settlement lacks canonical block evidence")?;
            let identity = (
                position.block_number,
                position.transaction_index,
                block_hash.clone(),
            );
            if let Some(prior) =
                transaction_blocks.insert(s.transaction_hash.clone(), identity.clone())
            {
                ensure!(
                    prior == identity,
                    "Settlement transaction has conflicting chain positions"
                );
            }
            if let Some(hash) = block_hashes.insert(position.block_number, block_hash) {
                ensure!(hash == block_hash, "Conflicting settlement block evidence");
            }
            events.push((position, Event::Settlement(s)));
        }
        for resolution in self.resolutions.values() {
            resolution.validate()?;
            if let Some(hash) = block_hashes.insert(resolution.block_number, &resolution.block_hash)
            {
                ensure!(
                    hash == &resolution.block_hash,
                    "Conflicting resolution block evidence"
                );
            }
            // Payouts are reported by a transaction inside this block. Fills
            // and redemptions in the same block are ordered before it; only a
            // redemption can follow a resolution, and either order books the
            // same total realized P&L.
            let position = ChainOrder {
                block_number: resolution.block_number,
                transaction_index: u64::MAX,
                log_index: u64::MAX,
                event_index: 0,
            };
            events.push((position, Event::Resolution(resolution)));
        }
        events.sort_by_key(|(order, _)| *order);
        // Archived history is flat; only its cash flow carries forward.
        let mut portfolio = Portfolio {
            positions: BTreeMap::new(),
            cash_delta: self.archive.as_ref().map_or(0., |a| a.totals.cash_delta),
        };
        let mut realizations = Vec::new();
        for (_, event) in events {
            match event {
                Event::Fill(r) => {
                    let quantity = r.shares_units as f64 / 1e6;
                    let cash = r.cash_units as f64 / 1e6;
                    let fee = r.fee_units as f64 / 1e6;
                    let p = portfolio.positions.entry(r.token_id.clone()).or_default();
                    p.fees_cash += fee;
                    if r.action == "BUY" {
                        p.quantity += quantity;
                        p.cost_basis += cash + fee;
                        portfolio.cash_delta -= cash + fee;
                    } else {
                        ensure!(r.action == "SELL", "Unknown receipt action");
                        let basis = remove_inventory(p, quantity)?;
                        p.realized_pnl += cash - fee - basis;
                        portfolio.cash_delta += cash - fee;
                        realizations.push(Realization {
                            event_id: format!("fill:{}", r.key()),
                            token_id: r.token_id.clone(),
                            transaction_hash: r.transaction_hash.clone(),
                            order_id: Some(r.order_id.clone()),
                            condition_id: None,
                            kind: "sell".into(),
                            confirmed_at_ms: self.block_times_ms.get(&r.block_hash).copied(),
                            quantity,
                            cash_received: cash,
                            fee_cash: fee,
                            cost_basis: basis,
                            realized_pnl: cash - fee - basis,
                        });
                    }
                }
                Event::Settlement(s) => {
                    let p = portfolio.positions.entry(s.token_id.clone()).or_default();
                    let basis = remove_inventory(p, s.quantity)?;
                    p.fees_cash += s.fee_cash;
                    p.realized_pnl += s.cash_received - s.fee_cash - basis;
                    portfolio.cash_delta += s.cash_received - s.fee_cash;
                    realizations.push(Realization {
                        event_id: format!("settlement:{}", s.event_id),
                        token_id: s.token_id.clone(),
                        transaction_hash: s.transaction_hash.clone(),
                        order_id: None,
                        condition_id: s.condition_id.clone(),
                        kind: "settlement".into(),
                        confirmed_at_ms: Some(s.confirmed_at_ms),
                        quantity: s.quantity,
                        cash_received: s.cash_received,
                        fee_cash: s.fee_cash,
                        cost_basis: basis,
                        realized_pnl: s.cash_received - s.fee_cash - basis,
                    });
                }
                Event::Resolution(r) => {
                    for (outcome, token) in r.tokens.iter().enumerate() {
                        let Some(p) = portfolio.positions.get_mut(token) else {
                            continue;
                        };
                        if p.quantity <= 1e-9 {
                            continue;
                        }
                        // Rebase to the payout value; a later redemption of this
                        // inventory then realizes (almost exactly) nothing.
                        let value =
                            p.quantity * r.numerators[outcome] as f64 / r.denominator as f64;
                        let basis = p.cost_basis;
                        p.realized_pnl += value - basis;
                        p.cost_basis = value;
                        realizations.push(Realization {
                            event_id: format!("resolution:{}:{}", r.condition_id, token),
                            token_id: token.clone(),
                            transaction_hash: String::new(),
                            order_id: None,
                            condition_id: Some(r.condition_id.clone()),
                            kind: "resolution".into(),
                            confirmed_at_ms: Some(r.resolved_at_ms),
                            quantity: p.quantity,
                            cash_received: 0.,
                            fee_cash: 0.,
                            cost_basis: basis,
                            realized_pnl: value - basis,
                        });
                    }
                }
            }
        }
        Ok((portfolio, realizations))
    }

    pub fn record_fill(&mut self, mut fill: ExecutionFill) -> Result<bool> {
        // What a conflict names: the facts each side holds.
        fn facts(fill: &ExecutionFill) -> String {
            format!(
                "{} {} at {} of token {} matched at {} ms",
                fill.action, fill.size, fill.price, fill.token_id, fill.matched_at_ms
            )
        }
        fill.validate()?;
        let key = fill.key();
        // Every settlement transaction reported for it is kept, durably.
        let mut others = std::mem::take(&mut fill.other_transactions);
        let mut reopened = false;
        if let Some(prior) = self.fills.get(&key) {
            others.extend(prior.other_transactions.iter().cloned());
            if let (Some(a), Some(b)) = (&prior.transaction_hash, &fill.transaction_hash) {
                if a != b {
                    // A final execution keeps its transaction and the other
                    // one beside it: a late notice of an earlier attempt, or
                    // any for a FAILED or void execution (a void one is FAILED
                    // again: it may have settled). Only a CONFIRMED one
                    // confirmed again under another is refused.
                    if matches!(prior.status.as_str(), "FAILED" | "VOID")
                        || (prior.status == "CONFIRMED" && fill.status != "CONFIRMED")
                    {
                        reopened = prior.status == "VOID";
                        others.push(b.clone());
                        fill.transaction_hash = prior.transaction_hash.clone();
                    } else {
                        ensure!(
                            prior.status != "CONFIRMED"
                                && !self.receipt_fills.values().any(
                                    |r| &r.transaction_hash == a && r.order_id == fill.order_id
                                ),
                            "Final execution changed its settlement transaction"
                        );
                        others.push(a.clone());
                    }
                }
            }
            if fill.transaction_hash.is_none() {
                fill.transaction_hash = prior.transaction_hash.clone();
            }
        }
        others.sort();
        others.dedup();
        if fill.transaction_hash.is_none() && !others.is_empty() {
            fill.transaction_hash = Some(others.remove(0));
        }
        others.retain(|tx| Some(tx) != fill.transaction_hash.as_ref());
        fill.other_transactions = others;
        if let Some(prior) = self.fills.get(&key) {
            ensure!(
                prior.token_id == fill.token_id
                    && prior.action == fill.action
                    && prior.size == fill.size
                    && prior.price == fill.price
                    && prior.matched_at_ms == fill.matched_at_ms,
                "Conflicting economic facts for an existing execution {key}: recorded {}, reported {}",
                facts(prior),
                facts(&fill)
            );
            // A delayed MATCHED/MINED notification must not undo finality. A
            // FAILED execution stays FAILED, whatever is reported next (a
            // restart decides it), and a void one VOID unless reopened above.
            if reopened {
                fill.status = "FAILED".into();
            } else if matches!(prior.status.as_str(), "FAILED" | "VOID")
                || (prior.status == "CONFIRMED" && !matches!(fill.status.as_str(), "CONFIRMED" | "FAILED"))
            {
                fill.status = prior.status.clone();
            }
            if fill.fee_evidence.is_none() {
                fill.fee_cash = prior.fee_cash;
                fill.fee_shares = prior.fee_shares;
                fill.fee_evidence = prior.fee_evidence.clone();
            } else if prior.fee_evidence.is_some() {
                ensure!(
                    prior.fee_cash == fill.fee_cash && prior.fee_shares == fill.fee_shares,
                    "Conflicting verified execution fees"
                );
            }
            if prior == &fill {
                return Ok(false);
            }
        }
        self.fills.insert(key, fill);
        Ok(true)
    }

    /// The executions of one trade (keys are trade:order).
    pub fn trade_fills<'a>(&'a self, trade_id: &'a str) -> impl Iterator<Item = &'a ExecutionFill> + 'a {
        self.fills
            .range(format!("{trade_id}:")..format!("{trade_id};"))
            .map(|(_, fill)| fill)
            .filter(move |fill| fill.trade_id == trade_id)
    }

    /// Keep a report of `trade_id` (reported FAILED) that could not be
    /// recorded (unrecorded_reports), with the valid settlement transactions
    /// it named, whatever its other fields: they join every recorded
    /// execution of the trade, FAILED from now on. Returns whether the ledger
    /// changed.
    pub fn keep_unrecorded(&mut self, trade_id: &str, transactions: &[String]) -> Result<bool> {
        let named: Vec<String> = transactions.iter().filter(|tx| is_word(tx)).cloned().collect();
        let new = !self.unrecorded_reports.contains_key(trade_id);
        let kept = self.unrecorded_reports.entry(trade_id.to_owned()).or_default();
        let mut changed = new;
        for tx in &named {
            changed |= kept.insert(tx.clone());
        }
        let recorded: Vec<ExecutionFill> = self.trade_fills(trade_id).cloned().collect();
        for prior in recorded {
            changed |= self.record_fill(ExecutionFill {
                status: "FAILED".into(),
                other_transactions: [prior.other_transactions.clone(), named.clone()].concat(),
                ..prior
            })?;
        }
        Ok(changed)
    }

    /// Keep a FAILED report of `trade_id` that names no recorded execution,
    /// with the settlement transaction it named (failed_notices). Returns
    /// whether the ledger changed.
    pub fn note_failed(&mut self, trade_id: &str, transaction: Option<&String>) -> Result<bool> {
        if let Some(tx) = transaction {
            ensure!(is_word(tx), "Invalid execution transaction hash");
        }
        let new = !self.failed_notices.contains_key(trade_id);
        let named = self.failed_notices.entry(trade_id.to_owned()).or_default();
        Ok(transaction.is_some_and(|tx| named.insert(tx.clone())) || new)
    }

    pub fn record_settlement(&mut self, settlement: Settlement) -> Result<bool> {
        settlement.validate()?;
        if let Some(prior) = self.settlements.get(&settlement.event_id) {
            ensure!(prior == &settlement, "Conflicting settlement evidence");
            return Ok(false);
        }
        self.settlements
            .insert(settlement.event_id.clone(), settlement);
        Ok(true)
    }

    /// Remember the exchange's condition ID for an owned token. A few distinct
    /// claims are kept so one bad report cannot hide the verifiable one.
    pub fn record_condition_claim(&mut self, token_id: &str, condition_id: &str) -> bool {
        let condition = condition_id.to_ascii_lowercase();
        if token_id.is_empty() || !is_word(&condition) {
            return false;
        }
        let claims = self.condition_claims.entry(token_id.to_owned()).or_default();
        claims.len() < 4 && claims.insert(condition)
    }

    pub fn record_resolution(&mut self, resolution: Resolution) -> Result<bool> {
        resolution.validate()?;
        if let Some(prior) = self.resolutions.get(&resolution.condition_id) {
            ensure!(prior == &resolution, "Conflicting resolution evidence");
            return Ok(false);
        }
        for token in &resolution.tokens {
            ensure!(
                !self.resolutions.values().any(|r| r.tokens.contains(token)),
                "Token resolved under two conditions"
            );
        }
        self.resolutions
            .insert(resolution.condition_id.clone(), resolution);
        Ok(true)
    }

    /// Re-derive archive totals from this ledger's own events. Every position
    /// must end flat and every realization must carry its confirmation time.
    pub fn summarize(&self) -> Result<ArchiveSummary> {
        ensure!(
            self.require_receipts && self.archive.is_none(),
            "Only receipt-verified segment history can be summarized"
        );
        let (portfolio, realizations) = self.receipt_projection()?;
        ensure!(
            portfolio.positions.values().all(|p| p.quantity.abs() <= 1e-9),
            "Archived history must be flat"
        );
        let mut summary = ArchiveSummary {
            cash_delta: portfolio.cash_delta,
            fees_cash: portfolio.positions.values().map(|p| p.fees_cash).sum(),
            ..Default::default()
        };
        for event in realizations {
            let at = event
                .confirmed_at_ms
                .context("Archived realization lacks finalized block time")?;
            summary.realized_pnl += event.realized_pnl;
            *summary.daily_realized.entry(at / 86_400_000).or_default() += event.realized_pnl;
        }
        Ok(summary)
    }

    /// Move the complete history of `tokens` into a separate ledger and
    /// return it with its totals; `self` keeps everything else. Resolution
    /// records are copied into the archive and stay live while a live token
    /// still needs them.
    pub fn split_archive(
        &mut self,
        tokens: &BTreeSet<String>,
    ) -> Result<(AccountingLedger, ArchiveSummary)> {
        let mut archived = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let (moved, kept): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.fills)
            .into_inner()
            .into_iter()
            .partition(|(_, f)| tokens.contains(&f.token_id));
        (archived.fills, self.fills) = (moved.into(), kept.into());
        let (moved, kept): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.receipt_fills)
            .into_inner()
            .into_iter()
            .partition(|(_, r)| tokens.contains(&r.token_id));
        (archived.receipt_fills, self.receipt_fills) = (moved.into(), kept.into());
        let (moved, kept): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.settlements)
            .into_inner()
            .into_iter()
            .partition(|(_, s)| tokens.contains(&s.token_id));
        (archived.settlements, self.settlements) = (moved.into(), kept.into());
        let (moved, kept): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.condition_claims)
            .into_inner()
            .into_iter()
            .partition(|(token, _)| tokens.contains(token));
        (archived.condition_claims, self.condition_claims) = (moved.into(), kept.into());
        let live_tokens: BTreeSet<&String> = self
            .receipt_fills
            .values()
            .map(|r| &r.token_id)
            .chain(self.settlements.values().map(|s| &s.token_id))
            .chain(self.condition_claims.keys())
            .collect();
        let mut kept = BTreeMap::new();
        for (condition, resolution) in std::mem::take(&mut self.resolutions).into_inner() {
            if resolution.tokens.iter().any(|t| tokens.contains(t)) {
                archived.resolutions.insert(condition.clone(), resolution.clone());
            }
            if resolution.tokens.iter().any(|t| live_tokens.contains(t)) {
                kept.insert(condition, resolution);
            }
        }
        self.resolutions = kept.into();
        for receipt in archived.receipt_fills.values() {
            if let Some(time) = self.block_times_ms.get(&receipt.block_hash) {
                archived
                    .block_times_ms
                    .insert(receipt.block_hash.clone(), *time);
            }
        }
        let referenced: BTreeSet<&String> =
            self.receipt_fills.values().map(|r| &r.block_hash).collect();
        self.block_times_ms
            .retain(|hash, _| referenced.contains(hash) || !archived.block_times_ms.contains_key(hash));
        let summary = archived.summarize()?;
        Ok((archived, summary))
    }

    /// Tokens held now whose condition has not been verified as resolved.
    pub fn unresolved_holdings(&self) -> Result<Vec<(String, BTreeSet<String>)>> {
        let portfolio = self.portfolio()?;
        Ok(portfolio
            .positions
            .iter()
            .filter(|(token, p)| {
                p.quantity > 1e-9 && !self.resolutions.values().any(|r| r.tokens.contains(*token))
            })
            .filter_map(|(token, _)| {
                self.condition_claims
                    .get(token)
                    .map(|claims| (token.clone(), claims.clone()))
            })
            .collect())
    }

    /// Projection is rebuilt in execution-time order, so delivery reordering
    /// cannot change realized P&L. Unconfirmed activity blocks recovery.
    pub fn portfolio(&self) -> Result<Portfolio> {
        if self.require_receipts {
            return Ok(self.receipt_projection()?.0);
        }
        #[cfg(test)]
        count_projection();
        ensure!(
            self.archive.is_none(),
            "Archived history requires receipt accounting"
        );
        enum Event<'a> {
            Fill(&'a ExecutionFill),
            Settlement(&'a Settlement),
        }
        let mut events = Vec::new();
        for fill in self.fills.values().filter(|fill| fill.status != "VOID") {
            fill.validate()?;
            ensure!(
                fill.status == "CONFIRMED",
                "Execution is not confirmed; portfolio requires reconciliation"
            );
            events.push((fill.matched_at_ms, fill.key(), Event::Fill(fill)));
        }
        for settlement in self.settlements.values() {
            settlement.validate()?;
            events.push((
                settlement.confirmed_at_ms,
                settlement.event_id.clone(),
                Event::Settlement(settlement),
            ));
        }
        events.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        // Opposite actions at indistinguishable times need finer receipt ordering.
        let mut actions_at_time = BTreeMap::new();
        for (time, _, event) in &events {
            if let Event::Fill(fill) = event {
                if let Some(prior) =
                    actions_at_time.insert((*time, fill.token_id.as_str()), fill.action.as_str())
                {
                    ensure!(
                        prior == fill.action,
                        "Execution order is ambiguous; transaction ordering required"
                    );
                }
            }
        }
        let mut portfolio = Portfolio::default();
        for (_, _, event) in events {
            match event {
                Event::Fill(fill) => {
                    let fee_cash = fill.fee_cash.context("Cash fee is unknown")?;
                    let fee_shares = fill.fee_shares.context("Share fee is unknown")?;
                    ensure!(fill.fee_evidence.is_some(), "Fee evidence is missing");
                    let p = portfolio
                        .positions
                        .entry(fill.token_id.clone())
                        .or_default();
                    p.fees_cash += fee_cash;
                    p.fees_shares += fee_shares;
                    if fill.action == "BUY" {
                        p.quantity += fill.size - fee_shares;
                        let cost = fill.size * fill.price + fee_cash;
                        p.cost_basis += cost;
                        portfolio.cash_delta -= cost;
                    } else {
                        let removed = fill.size + fee_shares;
                        let basis = remove_inventory(p, removed)?;
                        let proceeds = fill.size * fill.price - fee_cash;
                        p.realized_pnl += proceeds - basis;
                        portfolio.cash_delta += proceeds;
                    }
                }
                Event::Settlement(s) => {
                    let p = portfolio.positions.entry(s.token_id.clone()).or_default();
                    let basis = remove_inventory(p, s.quantity)?;
                    let proceeds = s.cash_received - s.fee_cash;
                    p.fees_cash += s.fee_cash;
                    p.realized_pnl += proceeds - basis;
                    portfolio.cash_delta += proceeds;
                }
            }
        }
        Ok(portfolio)
    }
}
fn remove_inventory(p: &mut Position, quantity: f64) -> Result<f64> {
    ensure!(
        quantity <= p.quantity + 1e-6 && p.quantity > 0.0,
        "Execution exceeds accounted inventory"
    );
    let quantity = quantity.min(p.quantity);
    let basis = p.cost_basis * quantity / p.quantity;
    p.quantity -= quantity;
    p.cost_basis -= basis;
    if p.quantity < 1e-9 {
        p.quantity = 0.0;
        p.cost_basis = 0.0;
    }
    Ok(basis)
}
/// Whether one (transaction, order)'s venue reports settle as its receipts
/// say, compared in whole units: the reported shares within one unit of the
/// receipts' (as before), the reported cash within one unit per report. The
/// f64 dollar comparison this replaces refused exact one-unit gaps: 0.5
/// shares at 0.73 against 365001 units differed by 1.0000000000287557e-6,
/// and 600000 shares at 0.73 against one unit more by 1.00000761449337e-6.
fn reports_settle_as_receipts(fills: &[&ExecutionFill], shares: u64, cash: u64) -> bool {
    let (mut reported_shares, mut reported_cash) = (0., 0.);
    for fill in fills {
        // The rounding bound is relative, so a report whose price or cash
        // underflows (below f64::MIN_POSITIVE, 2.2e-308) is refused (a 5e-324
        // price, or 50 million reports of 1e-9
        // shares at 2.4e-315, err absolutely, beyond any relative bound).
        if !(fill.price >= f64::MIN_POSITIVE && fill.size * fill.price >= f64::MIN_POSITIVE) {
            return false;
        }
        reported_shares += fill.size * 1e6;
        reported_cash += fill.size * fill.price * 1e6;
    }
    let reports = fills.len() as f64;
    within_units(reported_shares, shares, 1., reports)
        && within_units(reported_cash, cash, reports, reports)
}
/// Whether `reported`, the f64 sum of `reports` venue amounts in units, is
/// within `allowed` units of the receipts' exact `settled` units. Its floats
/// err by at most `error`, which counts each rounding as a whole
/// f64::EPSILON of `reported + settled`: per report two parsed decimals (a
/// parser may be one ulp off), a product and a scale, all relative to that
/// report's part of the sum; then `reports - 1` additions, the receipts'
/// units as an f64 and the difference. A gap within the allowance always
/// passes, and one beyond it by more than twice `error` never does. Where
/// `error` reaches a hundredth of a unit (about 3.75 million shares in one
/// report) the floats cannot be compared that finely and are refused. No
/// amount underflows (reports_settle_as_receipts refuses those), so every
/// rounding is relative.
fn within_units(reported: f64, settled: u64, allowed: f64, reports: f64) -> bool {
    let settled = settled as f64;
    let error = (reports + 5.) * f64::EPSILON * (reported + settled);
    error < 0.01 && (reported - settled).abs() <= allowed + error
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fill(id: &str, action: &str, quantity: f64, price: f64, time: u64) -> ExecutionFill {
        ExecutionFill {
            transaction_hash: None,
            trade_id: id.into(),
            order_id: format!("order-{id}"),
            token_id: "123".into(),
            action: action.into(),
            size: quantity,
            price,
            matched_at_ms: time,
            status: "CONFIRMED".into(),
            fee_cash: Some(0.0),
            fee_shares: Some(0.0),
            fee_evidence: Some("maker".into()),
            other_transactions: Vec::new(),
        }
    }
    /// A ledger with shared maps (Shared) reads and writes exactly the JSON
    /// of the plain maps it replaced, empty kept-report maps left out as
    /// before: the journal, its log records and the Python executor's reader
    /// see no difference. The old definition is spelled out here.
    #[test]
    fn a_ledger_of_shared_maps_encodes_as_the_plain_maps_did() {
        #[derive(Serialize, Deserialize)]
        struct PlainLedger {
            #[serde(default)]
            chain_cursor: Option<ChainCursor>,
            #[serde(default)]
            block_times_ms: BTreeMap<String, u64>,
            #[serde(default)]
            require_receipts: bool,
            #[serde(default)]
            receipt_fills: BTreeMap<String, crate::receipts::ReceiptFill>,
            fills: BTreeMap<String, ExecutionFill>,
            settlements: BTreeMap<String, Settlement>,
            #[serde(default)]
            condition_claims: BTreeMap<String, BTreeSet<String>>,
            #[serde(default)]
            resolutions: BTreeMap<String, Resolution>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            archive: Option<ArchiveBase>,
            #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
            failed_notices: BTreeMap<String, BTreeSet<String>>,
            #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
            unrecorded_reports: BTreeMap<String, BTreeSet<String>>,
        }
        let mut ledger = AccountingLedger { require_receipts: true, ..Default::default() };
        let buy = fill("buy", "BUY", 10.0, 0.4, 1);
        ledger.record_fill(buy).unwrap();
        ledger.install_receipt_fills(&[receipt_tests::receipt("BUY", 1)]).unwrap();
        ledger.install_block_time(&format!("0x{}", "ab".repeat(32)), 7).unwrap();
        ledger.record_condition_claim("123", &format!("0x{}", "cd".repeat(32)));
        for kept in [false, true] {
            if kept {
                ledger.note_failed("trade", None).unwrap();
                ledger.keep_unrecorded("other", &[]).unwrap();
            }
            let shared = serde_json::to_string(&ledger).unwrap();
            let plain: PlainLedger = serde_json::from_str(&shared).unwrap();
            assert_eq!(serde_json::to_string(&plain).unwrap(), shared, "kept reports {kept}");
            assert_eq!(shared.contains("failed_notices"), kept);
            let read: AccountingLedger = serde_json::from_str(&serde_json::to_string(&plain).unwrap()).unwrap();
            assert_eq!(serde_json::to_string(&read).unwrap(), shared);
        }
        // Without the optional maps (an old journal) it reads them as empty.
        let read: AccountingLedger = serde_json::from_str(r#"{"fills":{},"settlements":{}}"#).unwrap();
        assert!(read.block_times_ms.is_empty() && read.failed_notices.is_empty() && !read.require_receipts);
    }
    #[test]
    fn reordered_partial_sells_keep_basis_and_realized_pnl_exactly_once() {
        let mut ledger = AccountingLedger::default();
        let buy = fill("buy", "BUY", 10.0, 0.4, 1);
        ledger
            .record_fill(fill("sell2", "SELL", 2.0, 0.3, 3))
            .unwrap();
        ledger.record_fill(buy.clone()).unwrap();
        assert!(!ledger.record_fill(buy).unwrap());
        ledger
            .record_fill(fill("sell1", "SELL", 2.0, 0.6, 2))
            .unwrap();
        let portfolio = ledger.portfolio().unwrap();
        let p = &portfolio.positions["123"];
        assert_eq!(p.quantity, 6.0);
        assert!((p.cost_basis - 2.4).abs() < 1e-9);
        assert!((p.realized_pnl - 0.2).abs() < 1e-9);
        assert!((portfolio.cash_delta + 2.2).abs() < 1e-9);
    }
    #[test]
    fn fees_and_confirmed_settlement_are_distinct_from_resolution_estimates() {
        let mut ledger = AccountingLedger::default();
        let mut buy = fill("buy", "BUY", 10.0, 0.4, 1);
        buy.fee_cash = Some(0.02);
        buy.fee_shares = Some(0.1);
        buy.fee_evidence = Some("receipt-fixture".into());
        ledger.record_fill(buy).unwrap();
        let before = ledger.portfolio().unwrap();
        assert_eq!(before.positions["123"].realized_pnl, 0.0);
        let s = Settlement {
            block_hash: None,
            condition_id: None,
            chain_order: None,
            event_id: "receipt:7:123".into(),
            transaction_hash: format!("0x{}", "ab".repeat(32)),
            token_id: "123".into(),
            quantity: 9.9,
            cash_received: 9.9,
            fee_cash: 0.0,
            confirmed_at_ms: 2,
        };
        ledger.record_settlement(s.clone()).unwrap();
        assert!(!ledger.record_settlement(s).unwrap());
        let portfolio = ledger.portfolio().unwrap();
        let p = &portfolio.positions["123"];
        assert_eq!(p.quantity, 0.0);
        assert_eq!(p.cost_basis, 0.0);
        assert!((p.realized_pnl - 5.88).abs() < 1e-9);
        assert!((portfolio.cash_delta - 5.88).abs() < 1e-9);
    }
    #[test]
    fn unknown_fees_failed_trades_oversells_and_conflicts_block_recovery() {
        let mut ledger = AccountingLedger::default();
        let mut f = fill("a", "BUY", 10.0, 0.4, 1);
        f.fee_cash = None;
        f.fee_evidence = None;
        ledger.record_fill(f.clone()).unwrap();
        assert!(ledger.portfolio().is_err());
        f.fee_cash = Some(0.0);
        f.fee_evidence = Some("maker".into());
        ledger.record_fill(f.clone()).unwrap();
        assert!(ledger.portfolio().is_ok());
        f.size = 11.0;
        assert!(ledger.record_fill(f).is_err());
        ledger.record_fill(fill("b", "SELL", 11.0, 0.5, 2)).unwrap();
        assert!(ledger.portfolio().is_err());
    }

}

#[cfg(test)]
pub(crate) mod receipt_tests {
    use super::*;
    use crate::receipts::ReceiptFill;
    pub(crate) fn receipt(action: &str, index: u64) -> ReceiptFill {
        ReceiptFill {
            transaction_hash: format!("0x{index:064x}"),
            block_hash: format!("0x{}", "ab".repeat(32)),
            block_number: 10,
            transaction_index: index,
            log_index: index,
            order_id: format!("0x{:064x}", index + 100),
            token_id: "123".into(),
            action: action.into(),
            shares_units: 4_000_000,
            cash_units: 1_600_000,
            fee_units: 100_000,
        }
    }
    fn trade(id: &str, r: &ReceiptFill, quantity: f64) -> ExecutionFill {
        ExecutionFill {
            transaction_hash: Some(r.transaction_hash.clone()),
            trade_id: id.into(),
            order_id: r.order_id.clone(),
            token_id: r.token_id.clone(),
            action: r.action.clone(),
            size: quantity,
            price: r.cash_units as f64 / r.shares_units as f64,
            matched_at_ms: 1700000000000,
            status: "CONFIRMED".into(),
            fee_cash: None,
            fee_shares: None,
            fee_evidence: None,
            other_transactions: Vec::new(),
        }
    }
    /// receipt_projection compared every receipt with every execution:
    /// receipts x fills comparisons, about 51,000 at 226
    /// fills, most of each 0.9 ms projection, and growing with the square of
    /// the history. Each execution is now compared
    /// with the receipts of its own transaction and order only.
    #[test]
    fn receipt_projection_compares_each_execution_with_its_own_receipts_only() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let receipts: Vec<_> = (1..=40).map(|index| receipt("BUY", index)).collect();
        for (n, r) in receipts.iter().enumerate() {
            ledger.record_fill(trade(&format!("trade-{n}"), r, 4.)).unwrap();
        }
        ledger.install_receipt_fills(&receipts).unwrap();
        IDENTITY_CHECKS.with(|count| count.set(0));
        let portfolio = ledger.receipt_projection().unwrap().0;
        assert!((portfolio.positions["123"].quantity - 160.).abs() < 1e-9);
        let checks = IDENTITY_CHECKS.with(|count| count.get());
        assert!(checks <= 40, "{checks} comparisons for 40 executions");
    }
    /// The comparison still refuses an execution its receipt contradicts.
    #[test]
    fn an_execution_its_receipt_contradicts_is_refused() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let buy = receipt("BUY", 1);
        ledger.record_fill(trade("first", &buy, 2.)).unwrap();
        let mut other = trade("second", &buy, 2.);
        other.token_id = "456".into(); // same transaction and order, another token
        ledger.record_fill(other).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&buy)).unwrap();
        let error = ledger.receipt_projection().unwrap_err();
        assert!(error.to_string().contains("Execution/receipt identity mismatch"), "{error:#}");
    }
    /// A (transaction, order)'s venue reports and its receipts compare in
    /// whole units: the reported shares within one unit, the reported cash
    /// within one unit per report. The f64 dollar comparison refused exact
    /// one-unit gaps (0.5 shares at 0.73 against 365001 units; 600000 shares
    /// at 0.73 against one unit either way).
    #[test]
    fn venue_reports_and_receipts_compare_in_whole_units() {
        let projects = |reports: &[(f64, f64)], shares: u64, cash: u64| {
            let mut buy = receipt("BUY", 1);
            buy.shares_units = shares;
            buy.cash_units = cash;
            let mut ledger = AccountingLedger {
                require_receipts: true,
                ..Default::default()
            };
            for (n, &(size, price)) in reports.iter().enumerate() {
                let mut report = trade(&format!("trade-{n}"), &buy, size);
                report.price = price;
                ledger.record_fill(report).unwrap();
            }
            ledger.install_receipt_fills(&[buy]).unwrap();
            match ledger.receipt_projection() {
                Ok(_) => true,
                Err(error) => {
                    assert_eq!(error.to_string(), "Exchange and receipt execution amounts differ");
                    false
                }
            }
        };
        // One cash unit either way; two are refused.
        assert!(projects(&[(0.5, 0.73)], 500_000, 365_001));
        assert!(projects(&[(0.5, 0.73)], 500_000, 364_999));
        assert!(!projects(&[(0.5, 0.73)], 500_000, 365_002));
        assert!(!projects(&[(0.5, 0.73)], 500_000, 364_998));
        // A partial fill: the venue's settled cash, 91386 units. Its
        // floats are 125185.99999999999 shares and 91385.99999999999 cash
        // units, so a unit more on each side is just over one unit off.
        let live = [(0.125186, 0.730001757385011)];
        assert!(projects(&live, 125_186, 91_386));
        assert!(projects(&live, 125_187, 91_387));
        assert!(projects(&live, 125_185, 91_385));
        assert!(!projects(&live, 125_186, 91_388));
        assert!(!projects(&live, 125_188, 91_386));
        // A limit-price report's cash is not a whole unit: 73001.46 is
        // within one unit of 73001 and 73002 only.
        assert!(projects(&[(0.100002, 0.73)], 100_002, 73_001));
        assert!(projects(&[(0.100002, 0.73)], 100_002, 73_002));
        assert!(!projects(&[(0.100002, 0.73)], 100_002, 73_000));
        assert!(!projects(&[(0.100002, 0.73)], 100_002, 73_003));
        // 1.000026 shares scale to 1000026.0000000001 units, 730018.98 cash.
        assert!(projects(&[(1.000026, 0.73)], 1_000_025, 730_018));
        assert!(!projects(&[(1.000026, 0.73)], 1_000_024, 730_019));
        assert!(!projects(&[(1.000026, 0.73)], 1_000_026, 730_017));
        // Sizes need not be whole units: four of 0.10000049 report 400001.96
        // shares and 200000.98 cash units, never rounded report by report
        // (rounding each to 100000 would accept 400000 shares).
        let fractions = [(0.10000049, 0.5); 4];
        assert!(projects(&fractions, 400_002, 200_001));
        assert!(projects(&fractions, 400_001, 200_000));
        assert!(!projects(&fractions, 400_000, 200_001));
        assert!(!projects(&fractions, 399_999, 200_000));
        // One cash unit per report; shares one unit whatever the count.
        let halves = [(0.25, 0.73); 2];
        assert!(projects(&halves, 500_000, 365_002));
        assert!(projects(&halves, 500_000, 364_998));
        assert!(!projects(&halves, 500_000, 365_003));
        assert!(projects(&halves, 500_001, 365_000));
        assert!(!projects(&halves, 500_002, 365_000));
        assert!(!projects(&halves, 499_998, 365_000));
        // Large fills compare to the unit.
        let large = [(600_000., 0.73)];
        assert!(projects(&large, 600_000_000_000, 438_000_000_001));
        assert!(projects(&large, 600_000_000_000, 437_999_999_999));
        assert!(projects(&large, 600_000_000_001, 438_000_000_000));
        assert!(!projects(&large, 600_000_000_000, 438_000_000_002));
        assert!(!projects(&large, 599_999_999_998, 438_000_000_000));
        assert!(projects(&[(1e6, 0.73)], 1_000_000_000_000, 730_000_000_001));
        assert!(!projects(&[(1e6, 0.73)], 1_000_000_000_000, 730_000_000_002));
        // The rounding bound there is 2.7e-3 units: a share gap of 1.002
        // units passes, one of 1.004 is refused.
        assert!(projects(&[(1_000_000.000_001_002, 0.5)], 1_000_000_000_000, 500_000_000_000));
        assert!(!projects(&[(1_000_000.000_001_004, 0.5)], 1_000_000_000_000, 500_000_000_000));
        // The bound reaches a hundredth of a unit between 3.5 million shares
        // in one report (9.3e-3) and 4 million (1.07e-2), and at three
        // reports of a million (1.07e-2); there even an exact match is refused.
        assert!(projects(&[(3.5e6, 0.5)], 3_500_000_000_000, 1_750_000_000_000));
        assert!(!projects(&[(4e6, 0.5)], 4_000_000_000_000, 2_000_000_000_000));
        assert!(!projects(&[(1e6, 0.5); 3], 3_000_000_000_000, 1_500_000_000_000));
        // An amount that underflows is refused: the bound is relative.
        assert!(!projects(&[(1.0, 5e-324)], 1_000_000, 0));
        assert!(!projects(&[(3e6, 1e-314)], 3_000_000_000_000, 0));
        assert!(!projects(&[(1e-300, 1e-9)], 0, 0));
        assert!(projects(&[(1.0, f64::MIN_POSITIVE)], 1_000_000, 0));
    }
    #[test]
    fn held_inventory_is_realized_at_resolution_and_redemption_adds_nothing() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let buy = receipt("BUY", 1); // 4 shares, $1.60 + $0.10 fee
        ledger.record_fill(trade("buy", &buy, 4.)).unwrap();
        ledger
            .install_receipt_fills(std::slice::from_ref(&buy))
            .unwrap();
        let day = 20 * 86_400_000;
        ledger.install_block_time(&buy.block_hash, day + 1).unwrap();
        assert!(ledger.realizations().unwrap().is_empty());
        let condition = format!("0x{}", "2a".repeat(32));
        assert!(ledger.record_condition_claim("123", &format!("0x{}", "2A".repeat(32))));
        assert!(!ledger.record_condition_claim("123", &condition));
        assert!(!ledger.record_condition_claim("123", "0x22"));
        for n in 0..5 {
            ledger.record_condition_claim("123", &format!("0x{:064x}", n));
        }
        assert_eq!(ledger.condition_claims["123"].len(), 4); // bounded
        assert_eq!(ledger.unresolved_holdings().unwrap()[0].0, "123");
        let resolution = Resolution {
            condition_id: condition.clone(),
            tokens: ["123".into(), "456".into()],
            numerators: [0, 1], // the held outcome lost
            denominator: 1,
            block_number: 12,
            block_hash: format!("0x{}", "ef".repeat(32)),
            resolved_at_ms: day + 5,
        };
        let mut conflicting = resolution.clone();
        conflicting.numerators = [1, 0];
        let mut unbalanced = resolution.clone();
        unbalanced.condition_id = format!("0x{}", "33".repeat(32));
        unbalanced.numerators = [1, 1];
        assert!(ledger.record_resolution(unbalanced).is_err());
        assert!(ledger.record_resolution(resolution.clone()).unwrap());
        assert!(!ledger.record_resolution(resolution.clone()).unwrap());
        assert!(ledger.record_resolution(conflicting).is_err());
        assert!(ledger.unresolved_holdings().unwrap().is_empty());
        let events = ledger.realizations().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "resolution");
        assert_eq!(events[0].confirmed_at_ms, Some(day + 5));
        assert!((events[0].realized_pnl + 1.7).abs() < 1e-9);
        let position = &ledger.portfolio().unwrap().positions["123"];
        assert_eq!((position.quantity, position.cost_basis), (4., 0.));
        // Redeeming the worthless inventory later realizes nothing more.
        ledger
            .record_settlement(Settlement {
                event_id: "redeem".into(),
                transaction_hash: format!("0x{:064x}", 3),
                block_hash: Some(format!("0x{}", "cd".repeat(32))),
                condition_id: Some(condition.clone()),
                chain_order: Some(ChainOrder {
                    block_number: 13,
                    transaction_index: 0,
                    log_index: 0,
                    event_index: 1,
                }),
                token_id: "123".into(),
                quantity: 4.,
                cash_received: 0.,
                fee_cash: 0.,
                confirmed_at_ms: day + 86_400_000,
            })
            .unwrap();
        let reopened: AccountingLedger =
            serde_json::from_slice(&serde_json::to_vec(&ledger).unwrap()).unwrap();
        let events = reopened.realizations().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].kind, "settlement");
        assert!(events[1].realized_pnl.abs() < 1e-9);
        let total: f64 = events.iter().map(|e| e.realized_pnl).sum();
        let position = &reopened.portfolio().unwrap().positions["123"];
        assert!((total - position.realized_pnl).abs() < 1e-9);
        assert_eq!(position.quantity, 0.);
        // A winning resolution books the gain; its redemption again adds nothing.
        let mut winner = reopened.clone();
        winner.resolutions.get_mut(&condition).unwrap().numerators = [1, 0];
        winner.settlements.get_mut("redeem").unwrap().cash_received = 4.;
        let events = winner.realizations().unwrap();
        assert!((events[0].realized_pnl - 2.3).abs() < 1e-9);
        assert!(events[1].realized_pnl.abs() < 1e-9);
    }
    #[test]
    fn receipt_realizations_use_chain_basis_dates_and_survive_restart_without_double_counting() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let buy = receipt("BUY", 1);
        let mut sell = receipt("SELL", 2);
        sell.shares_units = 2_000_000;
        sell.cash_units = 600_000;
        sell.fee_units = 20_000;
        ledger.record_fill(trade("buy-1", &buy, 2.)).unwrap();
        ledger.record_fill(trade("buy-2", &buy, 2.)).unwrap();
        ledger.record_fill(trade("sell", &sell, 2.)).unwrap();
        ledger
            .install_receipt_fills(&[sell.clone(), buy.clone()])
            .unwrap();
        assert_eq!(ledger.pending_evidence().unwrap().len(), 2); // old journals backfill dates
        let day = 20 * 86_400_000;
        assert!(ledger.install_block_time(&buy.block_hash, day + 1).unwrap());
        assert!(!ledger.install_block_time(&buy.block_hash, day + 1).unwrap());
        assert!(ledger.install_block_time(&buy.block_hash, day + 2).is_err());
        assert!(ledger.pending_evidence().unwrap().is_empty());
        let events = ledger.realizations().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, format!("fill:{}", sell.key()));
        assert_eq!(events[0].confirmed_at_ms, Some(day + 1));
        assert!((events[0].cost_basis - 0.85).abs() < 1e-9); // includes BUY fee
        assert!((events[0].realized_pnl + 0.27).abs() < 1e-9); // includes SELL fee
        ledger
            .record_settlement(Settlement {
                event_id: "losing-burn".into(),
                transaction_hash: format!("0x{:064x}", 3),
                block_hash: Some(format!("0x{}", "cd".repeat(32))),
                condition_id: None,
                chain_order: Some(ChainOrder {
                    block_number: 11,
                    transaction_index: 0,
                    log_index: 0,
                    event_index: 0,
                }),
                token_id: "123".into(),
                quantity: 2.,
                cash_received: 0.,
                fee_cash: 0.,
                confirmed_at_ms: day + 86_400_000,
            })
            .unwrap();
        let reopened: AccountingLedger =
            serde_json::from_slice(&serde_json::to_vec(&ledger).unwrap()).unwrap();
        let events = reopened.realizations().unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            (events.iter().map(|e| e.realized_pnl).sum::<f64>()
                - reopened.portfolio().unwrap().positions["123"].realized_pnl)
                .abs()
                < 1e-9
        );
        assert_eq!(reopened.portfolio().unwrap().positions["123"].quantity, 0.);
    }
    #[test]
    fn retrying_transaction_is_replaced_before_finality_without_duplicate_inventory() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let final_receipt = receipt("BUY", 2);
        let mut fill = trade("retry", &final_receipt, 4.);
        let failed_tx = format!("0x{:064x}", 1);
        fill.transaction_hash = Some(failed_tx.clone());
        fill.status = "MINED".into();
        ledger.record_fill(fill.clone()).unwrap();
        assert!(ledger.pending_receipts().is_err());
        assert!(ledger.portfolio().is_err());
        fill.status = "RETRYING".into();
        ledger.record_fill(fill.clone()).unwrap();
        assert!(ledger.pending_receipts().is_err());
        fill.status = "CONFIRMED".into();
        fill.transaction_hash = Some(final_receipt.transaction_hash.clone());
        ledger.record_fill(fill.clone()).unwrap();
        assert_eq!(
            ledger.pending_receipts().unwrap(),
            [final_receipt.transaction_hash.clone()].into()
        );
        ledger
            .install_receipt_fills(std::slice::from_ref(&final_receipt))
            .unwrap();
        let p = ledger.portfolio().unwrap();
        assert_eq!(p.positions["123"].quantity, 4.);
        assert!((p.positions["123"].cost_basis - 1.7).abs() < 1e-9);
        // A delayed notification from the failed transaction cannot roll back
        // finality, replace the receipt, or add a second position.
        fill.status = "MINED".into();
        fill.transaction_hash = Some(failed_tx);
        assert!(!ledger.record_fill(fill.clone()).unwrap());
        assert_eq!(ledger.portfolio().unwrap().positions["123"].quantity, 4.);
        fill.status = "CONFIRMED".into();
        assert!(ledger.record_fill(fill).is_err());
    }

    #[test]
    fn a_settled_view_leaves_out_settling_fills_and_blocks_failed_ones() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let settled = receipt("BUY", 1);
        ledger.install_block_time(&settled.block_hash, 1_700_000_000_000).unwrap(); // every fixture block
        ledger.record_fill(trade("settled", &settled, 4.)).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&settled)).unwrap();
        assert_eq!(ledger.portfolio().unwrap().positions["123"].quantity, 4.);
        // A second fill matched and mined, its receipt already read: the full
        // ledger cannot project yet, the settled part can.
        let settling = receipt("BUY", 2);
        let mut fill = trade("settling", &settling, 4.);
        fill.status = "MINED".into();
        ledger.record_fill(fill.clone()).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&settling)).unwrap();
        assert!(ledger.portfolio().is_err());
        let view = ledger.settled_view().unwrap();
        assert_eq!(view.portfolio().unwrap().positions["123"].quantity, 4.);
        assert_eq!(view.fills.len(), 1);
        // Confirmed but its receipt not read yet: still left out, and its
        // transaction is evidence to read.
        let unread = receipt("BUY", 3);
        let mut late = trade("unread", &unread, 4.);
        late.status = "CONFIRMED".into();
        ledger.record_fill(late).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 4.);
        assert_eq!(ledger.settled_pending_evidence().unwrap(), [unread.transaction_hash.clone()].into());
        ledger.install_receipt_fills(std::slice::from_ref(&unread)).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 8.);
        // Two trades of one (transaction, order), one confirmed and one mined:
        // the whole group waits, so the view stays consistent.
        let shared = receipt("BUY", 4);
        let mut first = trade("pair-1", &shared, 2.);
        let mut second = trade("pair-2", &shared, 2.);
        second.status = "MINED".into();
        ledger.record_fill(first.clone()).unwrap();
        ledger.record_fill(second.clone()).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&shared)).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 8.);
        second.status = "CONFIRMED".into();
        ledger.record_fill(second).unwrap();
        first.status = "CONFIRMED".into();
        ledger.record_fill(first).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 12.);
        // A settling trade without a transaction yet may share a confirmed
        // sibling's receipt: that order waits as a whole.
        let aggregate = receipt("BUY", 5); // 4 shares for two 2-share trades
        let confirmed_half = trade("half-1", &aggregate, 2.);
        let mut unknown_half = trade("half-2", &aggregate, 2.);
        unknown_half.transaction_hash = None;
        unknown_half.status = "MATCHED".into();
        ledger.record_fill(confirmed_half).unwrap();
        ledger.record_fill(unknown_half.clone()).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&aggregate)).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 12.);
        unknown_half.transaction_hash = Some(aggregate.transaction_hash.clone());
        unknown_half.status = "CONFIRMED".into();
        ledger.record_fill(unknown_half).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 16.);
        // Reported RETRYING in a transaction whose receipt of its order is
        // read: the venue and the chain disagree, so it needs reconciliation
        // (a_retrying_fill_waits_with_its_transaction_and_its_orders_losses_stay).
        fill.status = "RETRYING".into();
        ledger.record_fill(fill.clone()).unwrap();
        assert!(ledger.settled_view().is_err());
        fill.status = "CONFIRMED".into();
        ledger.record_fill(fill).unwrap();
        assert_eq!(ledger.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 20.);
        // A failed settlement still needs reconciliation.
        let mut failed = trade("failed", &receipt("BUY", 6), 4.);
        failed.status = "FAILED".into();
        ledger.record_fill(failed).unwrap();
        assert!(ledger.settled_view().is_err());
        assert!(ledger.settled_pending_evidence().is_err());
    }

    /// A RETRYING fill stopped every publication: the settled view refused
    /// it as it refuses a FAILED one, although it is only unsettled (its
    /// settlement transaction failed and the venue settles it again). It waits
    /// with the transaction it names: the order's other, verified fills stay in
    /// the view, and so do the losses they realized (waiting
    /// with the whole order would let a late RETRYING report of another trade hide a
    /// resolved loss from the settled view). Where waiting would take a
    /// verified fill out (no transaction named, or one with a receipt of the
    /// order), it is refused.
    #[test]
    fn a_retrying_fill_waits_with_its_transaction_and_its_orders_losses_stay() {
        let day = 20 * 86_400_000;
        let failed_tx = format!("0x{:064x}", 0xdead);
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        // An order's verified BUY, $1.70 for 4 shares, resolved worthless today.
        let bought = receipt("BUY", 1);
        ledger.install_block_time(&bought.block_hash, day + 1).unwrap(); // every fixture block
        ledger.record_fill(trade("bought", &bought, 4.)).unwrap();
        ledger.install_receipt_fills(std::slice::from_ref(&bought)).unwrap();
        ledger
            .record_resolution(Resolution {
                condition_id: format!("0x{}", "2a".repeat(32)),
                tokens: ["123".into(), "456".into()],
                numerators: [0, 1],
                denominator: 1,
                block_number: 12,
                block_hash: format!("0x{}", "ef".repeat(32)),
                resolved_at_ms: day + 5,
            })
            .unwrap();
        // A late RETRYING report of another trade of that order.
        let mut retrying = trade("retrying", &bought, 2.);
        retrying.transaction_hash = Some(failed_tx.clone());
        retrying.status = "RETRYING".into();
        ledger.record_fill(retrying.clone()).unwrap();
        let view = ledger.settled_view().unwrap();
        assert_eq!((view.fills.len(), view.receipt_fills.len()), (1, 1));
        assert!(ledger.settled_pending_evidence().unwrap().is_empty());
        // The whole ledger still waits for it.
        assert!(ledger.portfolio().is_err() && ledger.pending_receipts().is_err());
        // Naming no transaction, it could share any of the order's receipts:
        // refused, so the loss cannot leave the view.
        let mut unnamed = ledger.clone();
        let mut no_tx = retrying.clone();
        no_tx.transaction_hash = None;
        unnamed.fills.insert(no_tx.key(), no_tx);
        assert!(unnamed.settled_view().is_err() && unnamed.settled_pending_evidence().is_err());

        // A retry that lands in a transaction with a sibling of its order whose
        // receipt is read first: that receipt holds both, so the projection is
        // refused until the RETRYING fill is confirmed there.
        let mut batched = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let retry = receipt("BUY", 2); // 4 shares: two 2-share trades
        let mut late = trade("late", &retry, 2.);
        late.transaction_hash = Some(failed_tx.clone());
        late.status = "RETRYING".into();
        batched.record_fill(late.clone()).unwrap();
        batched.record_fill(trade("sibling", &retry, 2.)).unwrap();
        assert_eq!(batched.settled_pending_evidence().unwrap(), [retry.transaction_hash.clone()].into());
        batched.install_receipt_fills(std::slice::from_ref(&retry)).unwrap();
        assert!(batched.settled_view().unwrap().portfolio().is_err());
        late.transaction_hash = Some(retry.transaction_hash.clone());
        late.status = "CONFIRMED".into();
        batched.record_fill(late).unwrap();
        assert_eq!(batched.settled_view().unwrap().portfolio().unwrap().positions["123"].quantity, 4.);
        assert_eq!(batched.portfolio().unwrap().positions["123"].quantity, 4.);

        // Named for a transaction that settled its order (a receipt of the
        // order is in it): the venue's reports contradict the chain, and
        // waiting would take that receipt out of the view: refused.
        let mut grouped = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let shared = receipt("BUY", 3);
        let mut first = trade("first", &shared, 2.);
        first.status = "RETRYING".into();
        grouped.record_fill(first).unwrap();
        grouped.record_fill(trade("second", &shared, 2.)).unwrap();
        assert!(grouped.settled_view().is_ok(), "no receipt in it yet: it waits");
        grouped.install_receipt_fills(std::slice::from_ref(&shared)).unwrap();
        assert!(grouped.settled_view().is_err());
    }

    #[test]
    fn aggregate_receipt_fee_is_once_and_chain_order_resolves_equal_trade_times() {
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        let buy = receipt("BUY", 1);
        let mut sell = receipt("SELL", 2);
        sell.shares_units = 2_000_000;
        sell.cash_units = 1_000_000;
        sell.fee_units = 20_000;
        // Delivered sell first; trade IDs and wall times cannot order execution.
        ledger.record_fill(trade("a-sell", &sell, 2.)).unwrap();
        ledger.record_fill(trade("z-buy-1", &buy, 2.)).unwrap();
        ledger.record_fill(trade("z-buy-2", &buy, 2.)).unwrap();
        assert!(ledger.portfolio().is_err());
        assert_eq!(ledger.pending_receipts().unwrap().len(), 2);
        assert!(ledger
            .install_receipt_fills(&[sell.clone(), buy.clone()])
            .unwrap());
        assert!(!ledger.install_receipt_fills(std::slice::from_ref(&buy)).unwrap());
        assert!(ledger.pending_receipts().unwrap().is_empty());
        let p = ledger.portfolio().unwrap();
        let position = &p.positions["123"];
        assert_eq!(position.quantity, 2.);
        assert!((position.cost_basis - 0.85).abs() < 1e-9);
        assert!((position.realized_pnl - 0.13).abs() < 1e-9);
        assert!((position.fees_cash - 0.12).abs() < 1e-9);
        assert!((p.cash_delta + 0.72).abs() < 1e-9);
        let mut late = trade("z-buy-1", &buy, 2.);
        late.status = "MATCHED".into();
        late.transaction_hash = None;
        assert!(!ledger.record_fill(late).unwrap());
        assert!((ledger.portfolio().unwrap().positions["123"].fees_cash - 0.12).abs() < 1e-9);
        let mut changed = buy.clone();
        changed.fee_units += 1;
        assert!(ledger.install_receipt_fills(&[changed]).is_err());
        assert_eq!(ledger.receipt_fills[&buy.key()], buy);
    }
    #[test]
    fn incomplete_or_conflicting_trade_and_receipt_coverage_never_projects() {
        let buy = receipt("BUY", 1);
        let mut ledger = AccountingLedger {
            require_receipts: true,
            ..Default::default()
        };
        ledger.install_receipt_fills(std::slice::from_ref(&buy)).unwrap();
        assert!(ledger.portfolio().is_err());
        ledger.record_fill(trade("partial", &buy, 2.)).unwrap();
        assert!(ledger.portfolio().is_err());
        ledger.record_fill(trade("remainder", &buy, 2.)).unwrap();
        assert!(ledger.portfolio().is_ok());
        let mut changed = trade("partial", &buy, 2.);
        changed.transaction_hash = Some(format!("0x{}", "ff".repeat(32)));
        assert!(ledger.record_fill(changed).is_err());
        let mut wrong_token = ledger.clone();
        wrong_token
            .fills
            .get_mut(&format!("partial:{}", buy.order_id))
            .unwrap()
            .token_id = "456".into();
        assert!(wrong_token.portfolio().is_err());
    }
}
