//! One atomic decision snapshot for all subscribed outcome tokens. A book's
//! exchange timestamp is its last change, which on a quiet book (before the
//! open, overnight) can be minutes old. So books stay current while the stream
//! proves alive: any frame, data or heartbeat, within LIVENESS. Exchange
//! timestamps still reject reordered and future-dated messages.
use anyhow::{ensure, Result};
use parking_lot::RwLock;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Books stay current while the stream delivered a frame this recently; the
/// trackers ping every 2 s, so a live connection answers well within it.
pub const LIVENESS: Duration = Duration::from_secs(5);
/// A decision must reach its order check within this time.
pub const MAX_DECISION_AGE: Duration = Duration::from_secs(2);
const MAX_FUTURE_SKEW_MS: u64 = 1_000;

/// Prices as integer millionths of a dollar (0.455 -> 455_000), exact for
/// every tick size the venue uses.
pub const PRICE_UNIT: f64 = 1_000_000.0;

/// One token's resting size per price level, in millionths of a dollar. The
/// stream sends a full `book` on subscribing and after trades; `price_change`
/// carries each level's new total size and the resulting best bid and ask.
/// Trades consume levels without a `price_change` for them, so levels better
/// than the reported best bid or ask are dropped when a change arrives.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Depth {
    pub bids: BTreeMap<u32, f64>,
    pub asks: BTreeMap<u32, f64>,
}

impl Depth {
    /// (price, size) from the best price outward.
    pub fn bid_levels(&self) -> impl Iterator<Item = (u32, f64)> + '_ {
        self.bids.iter().rev().map(|(p, s)| (*p, *s))
    }
    pub fn ask_levels(&self) -> impl Iterator<Item = (u32, f64)> + '_ {
        self.asks.iter().map(|(p, s)| (*p, *s))
    }
    pub fn best_bid(&self) -> Option<(u32, f64)> {
        self.bid_levels().next()
    }
    pub fn best_ask(&self) -> Option<(u32, f64)> {
        self.ask_levels().next()
    }
    /// Bid size priced at or above `floor`.
    pub fn bid_depth_from(&self, floor: u32) -> f64 {
        self.bids.range(floor..).map(|(_, s)| s).sum()
    }
    /// Ask size priced at or below `ceiling`.
    pub fn ask_depth_to(&self, ceiling: u32) -> f64 {
        self.asks.range(..=ceiling).map(|(_, s)| s).sum()
    }
    fn set(&mut self, bid: bool, price: u32, size: f64) {
        let side = if bid { &mut self.bids } else { &mut self.asks };
        if size > 0.0 {
            side.insert(price, size);
        } else {
            side.remove(&price);
        }
    }
    /// Drop levels a trade consumed: nothing rests better than the reported best.
    fn trim(&mut self, best_bid: Option<u32>, best_ask: Option<u32>) {
        match best_bid {
            Some(bid) => self.bids.retain(|p, _| *p <= bid),
            None => self.bids.clear(),
        }
        match best_ask {
            Some(ask) => self.asks.retain(|p, _| *p >= ask),
            None => self.asks.clear(),
        }
    }
}

#[derive(Clone, Debug)]
struct Quote {
    bid: Option<f64>,
    ask: Option<f64>,
    timestamp: u64,
    depth: Arc<Depth>,
    /// The depth's best levels are the reported best prices. A quote-only
    /// update (no level) can move the best to a price the depth lacks.
    depth_exact: bool,
}

/// How an order's price is checked against the market before it is sent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QuoteCheck {
    /// The decision's quotes must still be current, and a passive order may
    /// not improve the decision's best price.
    #[default]
    Decision,
    /// For post-only orders: a fresh decision from the same connection whose
    /// order does not cross the latest quotes, at any price inside the spread.
    /// Market makers quote on books that change every few milliseconds and
    /// create new best prices. Orders that are not post-only keep `Decision`.
    NonCrossing,
}

/// A post-only order the check before its POST refused because it would cross
/// the latest quote (QuoteCheck::NonCrossing): nothing was sent.
#[derive(Debug)]
pub struct WouldCross;
impl std::fmt::Display for WouldCross {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Passive order would cross the latest quote")
    }
}
impl std::error::Error for WouldCross {}

/// A token's applied book: best bid, best ask, book time, depth, depth matches the
/// best prices (MarketData::state_of).
pub type TokenState = (Option<f64>, Option<f64>, u64, Depth, bool);

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    generation: u64,
    revision: u64,
    tick_epoch: u64,
    quotes: HashMap<String, Quote>,
    taken: Instant,
    /// The stream's latest frame when the snapshot was taken.
    heartbeat: Instant,
}

impl MarketSnapshot {
    pub fn best_bid(&self, token: &str) -> Option<f64> {
        self.quotes.get(token)?.bid
    }
    pub fn best_ask(&self, token: &str) -> Option<f64> {
        self.quotes.get(token)?.ask
    }
    /// The token's full book as of this snapshot; None while the depth does not
    /// match the reported best prices (until the next full book or level change).
    pub fn depth(&self, token: &str) -> Option<&Depth> {
        self.quotes
            .get(token)
            .filter(|q| q.depth_exact)
            .map(|q| q.depth.as_ref())
    }
    /// The exchange time of the token's latest book change.
    pub fn book_timestamp(&self, token: &str) -> Option<u64> {
        self.quotes.get(token).map(|q| q.timestamp)
    }
    /// How long ago the stream last proved these quotes current.
    pub fn max_age_ms(&self) -> u64 {
        self.heartbeat.elapsed().as_millis() as u64
    }
    /// When the snapshot was taken: an order is validated against it while it
    /// is at most MAX_DECISION_AGE old (validate_excluding).
    pub fn taken(&self) -> Instant {
        self.taken
    }
}

#[derive(Default)]
struct State {
    connected: bool,
    /// When the stream last delivered a frame (data or heartbeat).
    heartbeat: Option<Instant>,
    /// When the stream last delivered a frame (heartbeat), not counting the
    /// connect itself.
    frame: Option<Instant>,
    generation: u64,
    revision: u64,
    /// Advances whenever a token's tick size may have changed: a tick-size
    /// event, a reconnect (events may have been missed) or a new market.
    tick_epoch: u64,
    quotes: HashMap<String, Option<Quote>>,
}

#[derive(Clone, Default)]
pub struct MarketData(Arc<RwLock<State>>);

impl MarketData {
    pub fn reset(&self, tokens: &[String]) {
        let mut s = self.0.write();
        s.generation += 1;
        s.tick_epoch += 1;
        s.connected = false;
        s.heartbeat = None;
        s.frame = None;
        s.quotes = tokens.iter().map(|t| (t.clone(), None)).collect();
    }
    pub fn connected(&self) {
        self.disconnect();
        let mut s = self.0.write();
        s.connected = true;
        s.heartbeat = Some(Instant::now());
    }
    pub fn disconnect(&self) {
        let mut s = self.0.write();
        s.connected = false;
        s.heartbeat = None;
        s.frame = None;
        s.generation += 1;
        s.tick_epoch += 1;
        for q in s.quotes.values_mut() {
            *q = None;
        }
    }
    /// The stream delivered a frame: its books are still current.
    pub fn heartbeat(&self) {
        self.heartbeat_at(Instant::now());
    }
    fn heartbeat_at(&self, now: Instant) {
        let mut s = self.0.write();
        if s.connected {
            s.heartbeat = Some(now);
            s.frame = Some(now);
        }
    }
    /// Book rules (tick, minimum size) read while this epoch holds are current.
    pub fn tick_epoch(&self) -> u64 {
        self.0.read().tick_epoch
    }
    pub fn is_connected(&self) -> bool {
        self.0.read().connected
    }
    /// The stream is connected and delivered a frame within LIVENESS, the
    /// connect itself not counting (a server that takes the handshake and then
    /// sends nothing never feeds it).
    pub fn is_fed_at(&self, now: Instant) -> bool {
        let s = self.0.read();
        s.connected && s.frame.is_some_and(|at| now.saturating_duration_since(at) <= LIVENESS)
    }
    /// A token's applied book, whatever its validity: best bid, best ask, book time,
    /// depth and whether the depth matches the best prices. None without a book.
    pub fn state_of(&self, token: &str) -> Option<TokenState> {
        let s = self.0.read();
        let q = s.quotes.get(token)?.as_ref()?;
        Some((q.bid, q.ask, q.timestamp, q.depth.as_ref().clone(), q.depth_exact))
    }
    /// `ingest` without the check against the local clock: a replay of messages
    /// already accepted (the market feed's view of a connection).
    pub fn ingest_any(&self, value: &Value) -> bool {
        self.ingest_at(value, Instant::now(), u64::MAX / 2)
    }
    /// The token's book as the venue's `book` message would show it (tests: the
    /// snapshot a connection subscribing now would get).
    #[cfg(test)]
    pub fn book_message(&self, token: &str) -> Option<Value> {
        let s = self.0.read();
        let q = s.quotes.get(token)?.as_ref()?;
        let level = |(price, size): (u32, f64)| serde_json::json!({"price": format!("{:.6}", price as f64 / PRICE_UNIT), "size": size.to_string()});
        Some(serde_json::json!({
            "event_type": "book", "asset_id": token, "market": "test", "timestamp": q.timestamp.to_string(),
            "hash": "test",
            "bids": q.depth.bid_levels().map(level).collect::<Vec<_>>(),
            "asks": q.depth.ask_levels().map(level).collect::<Vec<_>>(),
        }))
    }
    pub fn snapshot(&self) -> Result<MarketSnapshot> {
        self.snapshot_at(Instant::now(), now_ms())
    }
    fn snapshot_at(&self, now: Instant, wall_ms: u64) -> Result<MarketSnapshot> {
        let s = self.0.read();
        ensure!(s.connected, "Market feed disconnected");
        let heartbeat = s
            .heartbeat
            .filter(|at| now.saturating_duration_since(*at) <= LIVENESS)
            .ok_or_else(|| anyhow::anyhow!("Market feed is silent; quotes may be stale"))?;
        let quotes = s
            .quotes
            .iter()
            .map(|(t, q)| q.clone().map(|q| (t.clone(), q)))
            .collect::<Option<HashMap<_, _>>>()
            .ok_or_else(|| anyhow::anyhow!("Awaiting all outcome snapshots"))?;
        ensure!(!quotes.is_empty(), "No market snapshot");
        for q in quotes.values() {
            ensure!(
                q.bid.is_some() && q.ask.is_some() && q.bid < q.ask,
                "Incomplete or crossed book"
            );
            ensure!(
                q.timestamp <= wall_ms.saturating_add(MAX_FUTURE_SKEW_MS),
                "Exchange timestamp ahead of the local clock"
            );
        }
        Ok(MarketSnapshot {
            generation: s.generation,
            revision: s.revision,
            tick_epoch: s.tick_epoch,
            quotes,
            taken: now,
            heartbeat,
        })
    }
    pub fn validate(
        &self,
        decision: &MarketSnapshot,
        token: &str,
        action: &str,
        price: f64,
        post_only: bool,
    ) -> Result<()> {
        self.validate_with(decision, token, action, price, post_only, QuoteCheck::Decision)
    }
    pub fn validate_with(
        &self,
        decision: &MarketSnapshot,
        token: &str,
        action: &str,
        price: f64,
        post_only: bool,
        check: QuoteCheck,
    ) -> Result<()> {
        self.validate_excluding(decision, token, action, price, post_only, check, &[])
    }
    /// validate_with, where the non-crossing check ignores `departing`: the
    /// strategy's own orders on the opposite side of this token's book whose
    /// DELETE is on its way, as (price in millionths, shares). A level holding
    /// only those does not count as the best price; with the book's depth not
    /// matching its best prices, nothing is ignored.
    #[allow(clippy::too_many_arguments)] // validate_with's arguments plus the exclusions.
    pub fn validate_excluding(
        &self,
        decision: &MarketSnapshot,
        token: &str,
        action: &str,
        price: f64,
        post_only: bool,
        check: QuoteCheck,
        departing: &[(u32, f64)],
    ) -> Result<()> {
        let latest = self.snapshot()?;
        ensure!(
            decision.taken.elapsed() <= MAX_DECISION_AGE,
            "Strategy decision expired; recompute order"
        );
        if post_only && check == QuoteCheck::NonCrossing {
            ensure!(
                latest.generation == decision.generation,
                "Market feed reconnected since strategy decision; recompute order"
            );
            ensure!(
                latest.tick_epoch == decision.tick_epoch,
                "Tick size may have changed since strategy decision; recompute order"
            );
            ensure!(
                decision.quotes.contains_key(token),
                "Order token is outside the decision market"
            );
            let q = latest
                .quotes
                .get(token)
                .ok_or_else(|| anyhow::anyhow!("Order token is outside the decision market"))?;
            ensure!(price.is_finite() && price > 0.0 && price < 1.0, "Invalid limit price");
            // Compared as the exact amounts the order is signed with.
            let buy = action == "BUY";
            let opposite = uncrossed_best(q, buy, departing);
            if !opposite.is_none_or(|best| if buy { units(price) < best } else { units(price) > best }) {
                return Err(WouldCross.into());
            }
            return Ok(());
        }
        ensure!(
            latest.generation == decision.generation && latest.revision == decision.revision,
            "Market changed since strategy decision; recompute order"
        );
        let q = decision
            .quotes
            .get(token)
            .ok_or_else(|| anyhow::anyhow!("Order token is outside the decision market"))?;
        ensure!(price.is_finite() && price > 0.0 && price < 1.0, "Invalid limit price");
        if post_only {
            ensure!(
                if action == "BUY" {
                    price <= q.bid.unwrap()
                } else {
                    price >= q.ask.unwrap()
                },
                "Passive order would cross or improve beyond the decision quote"
            );
        }
        Ok(())
    }
    pub fn ingest(&self, value: &Value) -> bool {
        self.ingest_at(value, Instant::now(), now_ms())
    }
    fn ingest_at(&self, value: &Value, now: Instant, wall_ms: u64) -> bool {
        if let Some(values) = value.as_array() {
            // Every message is ingested; `any` would stop at the first change.
            let mut changed = false;
            for v in values {
                changed |= self.ingest_at(v, now, wall_ms);
            }
            return changed;
        }
        let Some(ts) = value
            .get("timestamp")
            .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        else {
            return false;
        };
        // An old timestamp is a book that has not changed since (the first
        // snapshot of a quiet market, for example), not stale data.
        if ts > wall_ms.saturating_add(MAX_FUTURE_SKEW_MS) {
            return false;
        }
        let mut s = self.0.write();
        if !s.connected {
            return false;
        }
        s.heartbeat = Some(now);
        let event = value["event_type"].as_str().unwrap_or("");
        let changes: Vec<&Value> = if event == "price_change" {
            value["price_changes"]
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default()
        } else {
            vec![value]
        };
        let mut updated = false;
        for change in changes {
            let Some(token) = change["asset_id"].as_str() else {
                continue;
            };
            let Some(slot) = s.quotes.get_mut(token) else {
                continue;
            };
            if slot.as_ref().is_some_and(|q| ts < q.timestamp) {
                continue;
            }
            let previous = slot.as_ref().map(|q| (q.bid, q.ask));
            if event == "tick_size_change" {
                *slot = None; // Require a full book after the tick changes.
                s.revision += 1;
                s.tick_epoch += 1;
                updated = true;
                continue;
            }
            let pair = match event {
                "book" => match (
                    levels(&change["bids"], true),
                    levels(&change["asks"], false),
                    book_side(&change["bids"]),
                    book_side(&change["asks"]),
                ) {
                    (Some(bid), Some(ask), Some(bids), Some(asks)) => {
                        Some((bid, ask, Arc::new(Depth { bids, asks }), true))
                    }
                    _ => None,
                },
                "price_change" | "best_bid_ask" if slot.is_some() => {
                    match (number(&change["best_bid"]), number(&change["best_ask"])) {
                        (Some(bid), Some(ask)) if bid >= 0.0 && ask <= 1.0 => {
                            let (bid, ask) = ((bid > 0.0).then_some(bid), (ask < 1.0).then_some(ask));
                            let level = (event == "price_change").then(|| level_change(change)).flatten();
                            match level {
                                Some(Err(())) => None, // a malformed level change
                                level => {
                                    // The quote is replaced below: its book is changed in
                                    // place, and copied only while a decision snapshot
                                    // still holds it.
                                    let mut depth = slot.take().expect("a quote").depth;
                                    let book = Arc::make_mut(&mut depth);
                                    if let Some(Ok((is_bid, price, size))) = level {
                                        book.set(is_bid, price, size);
                                    }
                                    book.trim(bid.map(units), ask.map(units));
                                    let exact = book.best_bid().map(|l| l.0) == bid.map(units)
                                        && book.best_ask().map(|l| l.0) == ask.map(units);
                                    Some((bid, ask, depth, exact))
                                }
                            }
                        }
                        _ => None,
                    }
                }
                _ => continue,
            };
            let Some((bid, ask, depth, depth_exact)) = pair else {
                // Malformed data cannot keep a previously actionable quote alive.
                *slot = None;
                s.revision += 1;
                updated = true;
                continue;
            };
            let changed = previous != Some((bid, ask));
            *slot = Some(Quote {
                bid,
                ask,
                timestamp: ts,
                depth,
                depth_exact,
            });
            if changed {
                s.revision += 1;
            }
            updated = true;
        }
        updated
    }
}

/// The best price a passive order on the other side could cross (the ask for a
/// BUY, the bid for a SELL), in millionths, not counting `departing` sizes (see
/// validate_excluding). None: nothing else rests on that side.
fn uncrossed_best(q: &Quote, buy: bool, departing: &[(u32, f64)]) -> Option<u32> {
    let reported = if buy { q.ask } else { q.bid }.map(units);
    if departing.is_empty() || !q.depth_exact {
        return reported;
    }
    let others = |(price, size): (u32, f64)| {
        let ours: f64 = departing
            .iter()
            .filter(|(at, _)| *at == price)
            .map(|(_, shares)| shares)
            .sum();
        (size > ours + 1e-9).then_some(price)
    };
    if buy {
        q.depth.ask_levels().find_map(others)
    } else {
        q.depth.bid_levels().find_map(others)
    }
}

fn number(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str()?.parse().ok())
        .filter(|n| n.is_finite())
}
fn levels(v: &Value, bid: bool) -> Option<Option<f64>> {
    let mut best: Option<f64> = None;
    for level in v.as_array()? {
        let price = number(&level["price"])?;
        let size = number(&level["size"])?;
        if !(0.0..=1.0).contains(&price) || size < 0.0 {
            return None;
        }
        if size == 0.0 || price == 0.0 || price == 1.0 {
            continue;
        }
        best = Some(
            best.map(|p| if bid { p.max(price) } else { p.min(price) })
                .unwrap_or(price),
        );
    }
    Some(best)
}
/// Dollars to integer millionths.
pub fn units(price: f64) -> u32 {
    (price * PRICE_UNIT).round() as u32
}
/// A level's price in millionths, exact for decimal strings.
fn price_units(v: &Value) -> Option<u32> {
    if let Some(text) = v.as_str() {
        let (whole, fraction) = text.trim().split_once('.').unwrap_or((text.trim(), ""));
        if fraction.len() > 6 || !fraction.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let whole: u32 = if whole.is_empty() { 0 } else { whole.parse().ok()? };
        let fraction: u32 = if fraction.is_empty() {
            0
        } else {
            format!("{fraction:0<6}").parse().ok()?
        };
        return whole.checked_mul(1_000_000)?.checked_add(fraction);
    }
    number(v).filter(|p| *p >= 0.0 && *p <= 1.0).map(units)
}
/// A full side of a `book` message, without empty or terminal levels.
fn book_side(v: &Value) -> Option<BTreeMap<u32, f64>> {
    let mut side = BTreeMap::new();
    for level in v.as_array()? {
        let price = price_units(&level["price"])?;
        let size = number(&level["size"])?;
        if price > 1_000_000 || size < 0.0 {
            return None;
        }
        if size > 0.0 && price > 0 && price < 1_000_000 {
            side.insert(price, size);
        }
    }
    Some(side)
}
/// A `price_change` entry's level update: (bid side, price, new total size).
/// None when the entry carries no level (only best prices); Err when malformed.
fn level_change(change: &Value) -> Option<Result<(bool, u32, f64), ()>> {
    if change.get("price").is_none() && change.get("size").is_none() && change.get("side").is_none() {
        return None;
    }
    let parsed = (|| {
        let is_bid = match change["side"].as_str()? {
            "BUY" => true,
            "SELL" => false,
            _ => return None,
        };
        let price = price_units(&change["price"]).filter(|p| *p > 0 && *p < 1_000_000)?;
        let size = number(&change["size"]).filter(|s| *s >= 0.0)?;
        Some((is_bid, price, size))
    })();
    Some(parsed.ok_or(()))
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A digest of everything a MarketData holds, for comparing two runs in tests.
#[cfg(test)]
impl MarketData {
    /// Everything it holds but the stream's receive instants (local clock):
    /// connection state, counters and every token's applied book, exactly.
    pub fn replay_digest(&self, h: &mut impl std::hash::Hasher) {
        let s = self.0.read();
        h.write_u8(s.connected as u8);
        h.write_u64(s.generation);
        h.write_u64(s.revision);
        h.write_u64(s.tick_epoch);
        let mut tokens: Vec<&String> = s.quotes.keys().collect();
        tokens.sort();
        let price = |h: &mut dyn std::hash::Hasher, p: Option<f64>| match p {
            Some(p) => h.write_u64(p.to_bits()),
            None => h.write_u8(0xee),
        };
        for token in tokens {
            h.write(token.as_bytes());
            let Some(q) = s.quotes[token].as_ref() else {
                h.write_u8(0);
                continue;
            };
            h.write_u8(1);
            price(h, q.bid);
            price(h, q.ask);
            h.write_u64(q.timestamp);
            h.write_u8(q.depth_exact as u8);
            for side in [&q.depth.bids, &q.depth.asks] {
                h.write_usize(side.len());
                for (p, size) in side {
                    h.write_u32(*p);
                    h.write_u64(size.to_bits());
                }
            }
        }
    }

    /// A decision snapshot as `snapshot` takes it, but with the stream's last frame
    /// as the time it is taken: a replay runs at its own pace, so the local time
    /// since that frame says nothing.
    pub fn replay_snapshot(&self) -> Result<MarketSnapshot> {
        let heartbeat = self.0.read().heartbeat;
        self.snapshot_at(heartbeat.unwrap_or_else(Instant::now), u64::MAX / 2)
    }
}

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use tests::fixture as test_market_data;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn book(token: &str, ts: u64) -> Value {
        json!({"event_type":"book", "asset_id":token, "timestamp":ts.to_string(),
            "bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.42","size":"10"}]})
    }
    pub fn fixture() -> MarketData {
        let data = MarketData::default();
        data.reset(&["123".into(), "456".into()]);
        data.connected();
        data.ingest(&json!([book("123", now_ms()), book("456", now_ms())]));
        data
    }
    #[test]
    fn fresh_synchronized_snapshots_expire_and_cannot_cross_reconnect() {
        let d = fixture();
        let snapshot = d.snapshot().unwrap();
        assert!(d.validate(&snapshot, "123", "BUY", 0.4, true).is_ok());
        assert!(d.validate(&snapshot, "999", "BUY", 0.4, true).is_err());
        assert!(d.validate(&snapshot, "123", "BUY", 0.42, true).is_err());
        assert!(d
            .snapshot_at(Instant::now() + LIVENESS + Duration::from_secs(1), now_ms())
            .is_err());
        d.disconnect();
        assert!(d.snapshot().is_err());
        d.connected();
        d.ingest(&book("123", now_ms()));
        assert!(d.snapshot().is_err());
        d.ingest(&book("456", now_ms()));
        assert!(d.validate(&snapshot, "123", "BUY", 0.4, true).is_err());
    }
    #[test]
    fn reordered_future_and_malformed_quotes_never_refresh_decisions() {
        let d = fixture();
        let now = now_ms();
        assert!(!d.ingest(&book("123", now - 10_000))); // older than the current quote
        assert!(!d.ingest(&book("123", now + 5_000))); // ahead of the local clock
        let mut newer = book("123", now + 600);
        newer["bids"][0]["price"] = json!("0.41");
        assert!(d.ingest(&newer));
        assert!(!d.ingest(&book("123", now))); // reordered
        assert!(d.snapshot().is_ok());
        let bad = json!({"event_type":"price_change","timestamp":now+600,
            "price_changes":[{"asset_id":"123","best_bid":"NaN","best_ask":"0.42"}]});
        d.ingest(&bad);
        assert!(d.snapshot().is_err());
    }
    #[test]
    fn quiet_books_stay_current_while_the_stream_is_alive() {
        // Before the open a book can go minutes
        // without a change, and the snapshot sent on subscribing carries that
        // time. Dropping it left the feed without a book, so no decision.
        let d = MarketData::default();
        d.reset(&["123".into(), "456".into()]);
        d.connected();
        let minute_ago = now_ms() - 60_000;
        assert!(d.ingest(&json!([book("123", minute_ago), book("456", minute_ago)])));
        let start = Instant::now();
        let decision = d.snapshot_at(start, now_ms()).unwrap();
        assert!(d.validate(&decision, "123", "BUY", 0.4, true).is_ok());
        assert!(decision.max_age_ms() < 1_000); // the stream's age, not the book's
                                                // A decision still has to reach its order check promptly.
        let taken = start
            .checked_sub(MAX_DECISION_AGE + Duration::from_millis(100))
            .unwrap();
        let expired = d.snapshot_at(taken, now_ms()).unwrap();
        let error = d.validate(&expired, "123", "BUY", 0.4, true).unwrap_err();
        assert!(error.to_string().contains("expired"), "{error}");
        // A silent stream makes quotes unavailable until it proves alive again.
        let later = start + LIVENESS + Duration::from_secs(1);
        assert!(d.snapshot_at(later, now_ms()).is_err());
        d.heartbeat_at(start + LIVENESS);
        assert!(d.snapshot_at(later, now_ms()).is_ok());
    }
    #[test]
    fn depth_follows_books_level_changes_and_trades() {
        let d = fixture();
        let now = now_ms();
        d.ingest(
            &json!({"event_type":"book","asset_id":"123","timestamp":now.to_string(),
            "bids":[{"price":"0.40","size":"10"},{"price":"0.39","size":"5"},{"price":"0.38","size":"0"}],
            "asks":[{"price":"0.42","size":"7"},{"price":"0.455","size":"3"}]}),
        );
        let depth = d.snapshot().unwrap().depth("123").unwrap().clone();
        assert_eq!(depth.best_bid(), Some((400_000, 10.0)));
        assert_eq!(
            depth.ask_levels().collect::<Vec<_>>(),
            vec![(420_000, 7.0), (455_000, 3.0)]
        );
        assert_eq!(depth.bid_depth_from(390_000), 15.0);
        assert_eq!(depth.ask_depth_to(450_000), 7.0);
        // A new best bid inside the spread, then a level removed.
        d.ingest(
            &json!({"event_type":"price_change","timestamp":now + 1,"price_changes":[
            {"asset_id":"123","price":"0.41","size":"4","side":"BUY","best_bid":"0.41","best_ask":"0.42"},
            {"asset_id":"123","price":"0.455","size":"0","side":"SELL","best_bid":"0.41","best_ask":"0.42"}]}),
        );
        let snapshot = d.snapshot().unwrap();
        assert_eq!(snapshot.best_bid("123"), Some(0.41));
        let depth = snapshot.depth("123").unwrap();
        assert_eq!(
            depth.bid_levels().collect::<Vec<_>>(),
            vec![(410_000, 4.0), (400_000, 10.0), (390_000, 5.0)]
        );
        assert_eq!(depth.ask_levels().collect::<Vec<_>>(), vec![(420_000, 7.0)]);
        // A trade took the 0.41 and 0.40 bids; the next change reports the new best
        // before any book arrives, so levels above it are gone.
        d.ingest(
            &json!({"event_type":"price_change","timestamp":now + 2,"price_changes":[
            {"asset_id":"123","price":"0.43","size":"2","side":"SELL","best_bid":"0.39","best_ask":"0.42"}]}),
        );
        let depth = d.snapshot().unwrap().depth("123").unwrap().clone();
        assert_eq!(depth.bid_levels().collect::<Vec<_>>(), vec![(390_000, 5.0)]);
        assert_eq!(
            depth.ask_levels().collect::<Vec<_>>(),
            vec![(420_000, 7.0), (430_000, 2.0)]
        );
        // The older snapshot keeps its own copy.
        assert_eq!(snapshot.depth("123").unwrap().best_bid(), Some((410_000, 4.0)));
        // A quote-only update moves the best to a level the depth lacks: no depth
        // until a change or book restores it.
        d.ingest(
            &json!({"event_type":"best_bid_ask","asset_id":"123","timestamp":now + 2,
            "best_bid":"0.40","best_ask":"0.42"}),
        );
        let snapshot = d.snapshot().unwrap();
        assert_eq!(snapshot.best_bid("123"), Some(0.40));
        assert!(snapshot.depth("123").is_none());
        d.ingest(
            &json!({"event_type":"price_change","timestamp":now + 2,"price_changes":[
            {"asset_id":"123","price":"0.40","size":"6","side":"BUY","best_bid":"0.40","best_ask":"0.42"}]}),
        );
        assert_eq!(
            d.snapshot().unwrap().depth("123").unwrap().best_bid(),
            Some((400_000, 6.0))
        );
        // A malformed level drops the quote until a full book arrives.
        d.ingest(
            &json!({"event_type":"price_change","timestamp":now + 3,"price_changes":[
            {"asset_id":"123","price":"0.4x","size":"2","side":"BUY","best_bid":"0.39","best_ask":"0.42"}]}),
        );
        assert!(d.snapshot().is_err());
        assert_eq!(price_units(&json!("0.0005")), Some(500));
        assert_eq!(price_units(&json!("1")), Some(1_000_000));
        assert_eq!(price_units(&json!("0.1234567")), None);
        assert_eq!(price_units(&json!(0.07)), Some(70_000));
    }
    #[test]
    fn non_crossing_check_accepts_new_best_prices_on_a_moving_book() {
        let d = fixture(); // 0.40 / 0.42 on both tokens
        let decision = d.snapshot().unwrap();
        let check =
            |price: f64, action: &str| d.validate_with(&decision, "123", action, price, true, QuoteCheck::NonCrossing);
        assert!(check(0.41, "BUY").is_ok()); // creates a level; Decision would refuse it
        assert!(d.validate(&decision, "123", "BUY", 0.41, true).is_err());
        assert!(check(0.42, "BUY").is_err() && check(0.40, "SELL").is_err());
        assert!(check(0.41, "SELL").is_ok());
        assert!(check(0.42 - 1e-15, "BUY").is_err()); // signs as 0.42
                                                      // The top moved since the decision: only crossing the latest quote refuses.
        d.ingest(
            &json!({"event_type":"price_change","timestamp":now_ms(),"price_changes":[
            {"asset_id":"123","price":"0.415","size":"5","side":"SELL","best_bid":"0.40","best_ask":"0.415"}]}),
        );
        assert!(d.validate(&decision, "123", "BUY", 0.40, true).is_err());
        assert!(check(0.41, "BUY").is_ok() && check(0.415, "BUY").is_err());
        // A tick-size change voids the decision even after a new book arrives.
        let before_tick = d.snapshot().unwrap();
        d.ingest(
            &json!({"event_type":"tick_size_change","asset_id":"123","timestamp":now_ms(),"new_tick_size":"0.001"}),
        );
        d.ingest(&book("123", now_ms()));
        assert!(d
            .validate_with(&before_tick, "123", "BUY", 0.41, true, QuoteCheck::NonCrossing)
            .unwrap_err()
            .to_string()
            .contains("Tick size"));
        // Taker orders keep the strict check; a reconnect voids the decision.
        assert!(d
            .validate_with(&decision, "123", "BUY", 0.40, false, QuoteCheck::NonCrossing)
            .is_err());
        d.disconnect();
        d.connected();
        d.ingest(&json!([book("123", now_ms()), book("456", now_ms())]));
        assert!(check(0.41, "BUY").unwrap_err().to_string().contains("reconnected"));
    }
    #[test]
    fn non_crossing_check_ignores_our_own_departing_orders_only() {
        let d = fixture(); // 0.40 / 0.42, 10 shares each, on both tokens
        let decision = d.snapshot().unwrap();
        let check = |price: f64, action: &str, departing: &[(u32, f64)]| {
            d.validate_excluding(
                &decision,
                "123",
                action,
                price,
                true,
                QuoteCheck::NonCrossing,
                departing,
            )
        };
        let error = check(0.42, "BUY", &[]).unwrap_err();
        assert!(error.is::<WouldCross>(), "{error:#}");
        // The whole 0.42 offer is ours and its DELETE is on its way.
        assert!(check(0.42, "BUY", &[(420_000, 10.0)]).is_ok());
        // Others offer there too, or ours rests elsewhere: still a cross.
        assert!(check(0.42, "BUY", &[(420_000, 4.0)]).unwrap_err().is::<WouldCross>());
        assert!(check(0.42, "BUY", &[(430_000, 10.0)]).unwrap_err().is::<WouldCross>());
        assert!(check(0.40, "SELL", &[(400_000, 10.0)]).is_ok());
        // The next level is the bound; with none, nothing can be crossed.
        d.ingest(&json!({"event_type":"book","asset_id":"123","timestamp":now_ms(),
            "bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.42","size":"10"},{"price":"0.45","size":"5"}]}));
        assert!(check(0.44, "BUY", &[(420_000, 10.0)]).is_ok());
        assert!(check(0.45, "BUY", &[(420_000, 10.0)]).unwrap_err().is::<WouldCross>());
        assert!(check(0.45, "BUY", &[(420_000, 10.0), (450_000, 5.0)]).is_ok());
        // A depth that does not match the reported best prices ignores nothing.
        d.ingest(
            &json!({"event_type":"best_bid_ask","asset_id":"123","timestamp":now_ms(),
            "best_bid":"0.40","best_ask":"0.41"}),
        );
        assert!(d.snapshot().unwrap().depth("123").is_none());
        assert!(check(0.41, "BUY", &[(410_000, 10.0)]).unwrap_err().is::<WouldCross>());
    }
    #[test]
    fn a_level_change_that_keeps_the_best_prices_keeps_a_decision_current() {
        let d = fixture(); // 0.40 / 0.42 on both tokens
        let decision = d.snapshot().unwrap();
        let now = now_ms();
        let change = |price: &str, best_bid: &str| {
            json!({"event_type":"price_change","timestamp":now.to_string(),"price_changes":[
                {"asset_id":"123","price":price,"size":"7","side":"BUY","best_bid":best_bid,"best_ask":"0.42"}]})
        };
        // A level behind the best: the strict check (revisions) still passes.
        assert!(d.ingest(&change("0.39", "0.40")));
        assert!(d.validate(&decision, "123", "BUY", 0.40, true).is_ok());
        // The best bid moves: the decision is stale.
        assert!(d.ingest(&change("0.41", "0.41")));
        assert!(d.validate(&decision, "123", "BUY", 0.40, true).is_err());
        // A level behind the new best, on a fresh decision: current again.
        let decision = d.snapshot().unwrap();
        assert!(d.ingest(&change("0.38", "0.41")));
        assert!(d.validate(&decision, "123", "BUY", 0.41, true).is_ok());
    }

    /// The token's book as MarketData holds it (the shared allocation).
    fn book_ptr(d: &MarketData, token: &str) -> *const Depth {
        Arc::as_ptr(&d.0.read().quotes[token].as_ref().unwrap().depth)
    }

    #[test]
    fn a_level_change_edits_the_book_in_place_unless_a_snapshot_holds_it() {
        let d = fixture(); // 0.40 / 0.42 on both tokens
        let now = now_ms();
        let change = |price: &str, size: &str| {
            json!({"event_type":"price_change","timestamp":now.to_string(),"price_changes":[
                {"asset_id":"123","price":price,"size":size,"side":"BUY","best_bid":"0.40","best_ask":"0.42"}]})
        };
        let before = book_ptr(&d, "123");
        assert!(d.ingest(&change("0.39", "7")));
        assert_eq!(book_ptr(&d, "123"), before, "no copy while nothing else holds the book");
        assert_eq!(
            d.state_of("123").unwrap().3.bid_levels().collect::<Vec<_>>(),
            vec![(400_000, 10.0), (390_000, 7.0)]
        );
        // A decision snapshot holds it: the change goes to a copy, the snapshot keeps its own.
        let snapshot = d.snapshot().unwrap();
        assert!(d.ingest(&change("0.38", "3")));
        assert_ne!(book_ptr(&d, "123"), before);
        assert_eq!(snapshot.depth("123").unwrap().bid_levels().count(), 2);
        assert_eq!(d.state_of("123").unwrap().3.bid_levels().count(), 3);
        drop(snapshot);
        // Released: in place again.
        let copied = book_ptr(&d, "123");
        assert!(d.ingest(&change("0.38", "0")));
        assert_eq!(book_ptr(&d, "123"), copied);
        assert_eq!(d.state_of("123").unwrap().3.bid_levels().count(), 2);
    }

    /// Random messages to two MarketData, one with its books held over every
    /// message (each change copies the book, as every change did before it was
    /// edited in place) and a decision snapshot when there is one: the same
    /// books, counters and answers after every message, and the held books
    /// unchanged.
    #[test]
    fn books_edited_in_place_equal_books_copied_on_every_change() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        use std::hash::Hasher;
        let digest = |d: &MarketData| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            d.replay_digest(&mut h);
            h.finish()
        };
        let mut checked = 0;
        for seed in 0..200u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let (in_place, copied) = (MarketData::default(), MarketData::default());
            for d in [&in_place, &copied] {
                d.reset(&["U".into(), "D".into()]);
                d.connected();
            }
            let base = now_ms() as i64 - 60_000;
            let mut ts = base;
            let price = |rng: &mut StdRng| format!("{:.2}", rng.random_range(1..=99) as f64 / 100.0);
            for _ in 0..300 {
                // Mostly forward in time, sometimes the same or an older millisecond.
                ts = (ts + rng.random_range(-2..=5)).max(base);
                let token = if rng.random_bool(0.8) { "U" } else { "D" };
                let msg = match rng.random_range(0..100) {
                    0..=9 => {
                        let side = |rng: &mut StdRng| {
                            (0..rng.random_range(0..6))
                                .map(|_| json!({"price": price(rng), "size": rng.random_range(0..50).to_string()}))
                                .collect::<Vec<_>>()
                        };
                        json!({"event_type":"book","asset_id":token,"timestamp":ts.to_string(),"bids":side(&mut rng),"asks":side(&mut rng)})
                    }
                    10..=11 => json!({"event_type":"tick_size_change","asset_id":token,"timestamp":ts.to_string()}),
                    12..=14 => json!({"event_type":"best_bid_ask","asset_id":token,"timestamp":ts.to_string(),
                        "best_bid":price(&mut rng),"best_ask":price(&mut rng)}),
                    15 => json!({"event_type":"price_change","timestamp":ts.to_string(),"price_changes":[
                        {"asset_id":token,"price":"0.4x","size":"2","side":"BUY","best_bid":"0.39","best_ask":"0.42"}]}),
                    _ => {
                        let entries: Vec<Value> = (0..rng.random_range(1..=3))
                            .map(|_| {
                                let token = if rng.random_bool(0.7) { "U" } else { "D" };
                                let (bid, ask) = (rng.random_range(0..=60), rng.random_range(40..=100));
                                json!({"asset_id":token,"price":price(&mut rng),"size":rng.random_range(0..40).to_string(),
                                    "side":if rng.random_bool(0.5) { "BUY" } else { "SELL" },
                                    "best_bid":format!("{:.2}", bid as f64 / 100.0),"best_ask":format!("{:.2}", ask as f64 / 100.0)})
                            })
                            .collect();
                        json!({"event_type":"price_change","timestamp":ts.to_string(),"price_changes":entries})
                    }
                };
                let held = copied.snapshot().ok();
                let held_quotes = held.as_ref().map(|s| (s.best_bid("U"), s.depth("U").cloned()));
                let held_books: Vec<(Arc<Depth>, Depth)> = copied
                    .0
                    .read()
                    .quotes
                    .values()
                    .flatten()
                    .map(|q| (q.depth.clone(), q.depth.as_ref().clone()))
                    .collect();
                assert_eq!(in_place.ingest(&msg), copied.ingest(&msg), "seed {seed}: {msg}");
                for (book, was) in &held_books {
                    assert_eq!(book.as_ref(), was, "seed {seed}: a held book changed");
                }
                assert_eq!(digest(&in_place), digest(&copied), "seed {seed}: {msg}");
                for token in ["U", "D"] {
                    assert_eq!(in_place.state_of(token), copied.state_of(token));
                }
                assert_eq!(in_place.snapshot().is_ok(), copied.snapshot().is_ok());
                // The held snapshot never saw the change.
                assert_eq!(
                    held.as_ref().map(|s| (s.best_bid("U"), s.depth("U").cloned())),
                    held_quotes
                );
                checked += 1;
            }
        }
        assert_eq!(checked, 60_000);
    }

    #[test]
    fn full_snapshot_required_after_disconnect_or_tick_change() {
        let d = fixture();
        let epoch = d.tick_epoch();
        d.ingest(&json!({"event_type":"book","asset_id":"123","timestamp":now_ms(),
            "bids":[{"price":"0.41","size":"10"}],"asks":[{"price":"0.43","size":"10"}]}));
        assert_eq!(d.tick_epoch(), epoch); // quotes alone never invalidate book rules
        d.ingest(
            &json!({"event_type":"tick_size_change","asset_id":"123","timestamp":now_ms(),"new_tick_size":"0.001"}),
        );
        assert_eq!(d.tick_epoch(), epoch + 1);
        d.ingest(&json!({"event_type":"price_change","timestamp":now_ms(),
            "price_changes":[{"asset_id":"123","best_bid":"0.4","best_ask":"0.42"}]}));
        assert!(d.snapshot().is_err());
        d.ingest(&book("123", now_ms()));
        assert!(d.snapshot().is_ok());
        // Events may be missed while disconnected, and a new market has new rules.
        d.disconnect();
        assert_eq!(d.tick_epoch(), epoch + 2);
        d.reset(&["123".into(), "456".into()]);
        assert_eq!(d.tick_epoch(), epoch + 3);
    }
}
