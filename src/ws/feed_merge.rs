//! Several market-data connections, one book per market: each
//! venue event is applied once, from the connection that delivers it first, in
//! the venue's order, and a disagreement between connections rebuilds the market
//! from a fresh snapshot.
//!
//! What the venue sends two connections (two recorders, a shared
//! socket against a socket per market, 26 markets over 22 minutes):
//! - book events (price_change, the `book` sent after a trade, tick_size_change)
//!   come in one order on every connection: 1,393,161 common events, no swap;
//! - last_trade_price floats against them (2,757 inversions, each with a print);
//! - some messages come twice, verbatim, within ten messages, on every connection
//!   (price_change, tick_size_change): part of the stream, applied twice;
//! - a subscription snapshot dates its token's last change.
//!
//! Book events are therefore merged by position. Each market keeps a log of the
//! book events applied, by content key, and each connection in line has a cursor
//! into it: its next book event is a copy of the event at its cursor (a duplicate)
//! or, at the end of the log, a new event, applied at once. Any other event at the
//! cursor means the connections disagree about the market's history: an integrity
//! failure, after which the market waits for a fresh snapshot. Prints and other
//! messages are counted instead: the k-th copy of a message a connection delivers
//! is new when no connection delivered k copies of it before.
//!
//! A subscription snapshot is not an event. It bootstraps a market no connection
//! is in line for, as the single socket always did. Otherwise the connection holds
//! its book events until they line up with the log: ALIGN_RUN of its held events
//! in a row match the log at exactly one place (it is behind or level, or the
//! connections it is ahead of caught up; its events past the log's end are then
//! applied at once), or, on a quiet market, its held events match the log's last
//! ones, or it holds none and its snapshot equals the applied book. A connection
//! that cannot line up within ALIGN_TIMEOUT rebuilds the market from its own
//! snapshot.
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    hash::Hasher,
    time::{Duration, Instant},
};

pub type ConnId = u64;

/// Book events applied stay in the log this long: a connection further behind
/// cannot be lined up and leaves the market's lines.
pub const LOG_KEEP: Duration = Duration::from_secs(10);
/// A connection that has not lined up after this long rebuilds the market from its
/// snapshot (or, without one, leaves the market's lines).
pub const ALIGN_TIMEOUT: Duration = Duration::from_secs(10);
/// A connection holding more book events than this leaves the market's lines (an
/// 80 ms lead is tens of events; this is seconds of the busiest market).
pub const MAX_HELD: usize = 5_000;
/// A quiet market lines a connection up when nothing changed the book for this
/// long: by its snapshot (it holds no event) or by its held events matching the
/// log's last ones.
pub const QUIET: Duration = Duration::from_secs(1);
/// Held events that match the log this many in a row line a connection up at once.
/// The venue repeats single messages verbatim, and pairs at most (one recorded hour:
/// 1,105 repeats in 6.5 million book events, none in a run of three), so a shorter
/// match can be an earlier copy of a repeated message.
pub const ALIGN_RUN: usize = 4;
/// Copies of a print are counted this long.
const OTHER_KEEP: Duration = Duration::from_secs(30);

/// A message's identity: its content with object keys in order, hashed twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EventKey(u64, u64);

/// The second hash starts with this.
const SECOND: u64 = 0x9E37_79B9_7F4A_7C15;
/// A key's bytes stay allocated for the next message up to this size.
const KEEP_BYTES: usize = 1 << 20;

thread_local! {
    /// The bytes of the message being keyed (one buffer per socket task's thread).
    static KEY_BYTES: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(16 << 10));
}

/// Appends what the key hashes of `value`: its content with object keys in order.
/// Each part is a type tag, then a bool's byte; a number's or string's text and
/// 0xFF (which no UTF-8 text holds, so no text is a prefix of another); an array's
/// or object's length (a usize, native order) and its items, an object's key text
/// and 0xFF before each item. These are the bytes Hash wrote part by part for each
/// (u8, bool, usize, str), so the keys are those of the part-by-part hash the feed
/// used before (`feed_hash`, kept in the tests).
fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(0),
        Value::Bool(b) => out.extend_from_slice(&[1, *b as u8]),
        Value::Number(n) => {
            out.push(2);
            // Writing to a Vec cannot fail.
            let _ = std::io::Write::write_fmt(out, format_args!("{n}"));
            out.push(0xFF);
        }
        Value::String(text) => {
            out.push(3);
            out.extend_from_slice(text.as_bytes());
            out.push(0xFF);
        }
        Value::Array(items) => {
            out.push(4);
            out.extend_from_slice(&items.len().to_ne_bytes());
            items.iter().for_each(|item| canonical(item, out));
        }
        Value::Object(map) => {
            out.push(5);
            out.extend_from_slice(&map.len().to_ne_bytes());
            let mut entry = |key: &String, item: &Value| {
                out.extend_from_slice(key.as_bytes());
                out.push(0xFF);
                canonical(item, out);
            };
            // serde_json's maps keep their keys in order (no preserve_order).
            if map.keys().is_sorted() {
                map.iter().for_each(|(key, item)| entry(key, item));
            } else {
                let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
                entries.into_iter().for_each(|(key, item)| entry(key, item));
            }
        }
    }
}

impl EventKey {
    /// The message's bytes (canonical) hashed by std's DefaultHasher (SipHash-1-3,
    /// fixed keys), then again after SECOND's 8 bytes.
    pub fn of(msg: &Value) -> Self {
        KEY_BYTES.with(|bytes| {
            let mut bytes = bytes.borrow_mut();
            bytes.clear();
            bytes.extend_from_slice(&SECOND.to_ne_bytes());
            canonical(msg, &mut bytes);
            let mut a = std::collections::hash_map::DefaultHasher::new();
            a.write(&bytes[8..]);
            let mut b = std::collections::hash_map::DefaultHasher::new();
            b.write(&bytes);
            if bytes.capacity() > KEEP_BYTES {
                *bytes = Vec::with_capacity(16 << 10);
            }
            Self(a.finish(), b.finish())
        })
    }
}

/// A message to apply to the market's book.
#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    /// The connection that delivered it first.
    pub conn: ConnId,
    pub msg: Value,
    /// A subscription snapshot (no delivery lag to record).
    pub snapshot: bool,
}

/// What to do with a message a connection delivered.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    /// Apply these, in order (the message itself, or held ones it released).
    Apply(Vec<Applied>),
    /// A copy of a message already applied.
    Duplicate,
    /// Held until the connection lines up (or a print from one not in line).
    Held,
    /// The connection is not subscribed to the market or has left its lines.
    Ignored,
    /// The connection's book events disagree with the applied history.
    Diverged,
    /// The connection just left the market's lines (too far behind the log, or too
    /// much held): it needs replacing.
    Failed,
}

/// After a connection closed.
#[derive(Clone, Debug, PartialEq)]
pub enum Lost {
    /// Another connection is still in line (or the closed one was not).
    Kept,
    /// No connection is in line any more: the market has no book until a snapshot.
    Gone,
    /// No connection was in line; one that holds a snapshot takes over: the market
    /// is rebuilt from these.
    Rebuild(Vec<Applied>),
}

#[derive(Clone, Debug)]
enum Item {
    /// A subscription snapshot of token 0 (Up) or 1 (Down).
    Snapshot(usize, Value),
    Event(EventKey, Value),
}

#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
enum Line {
    /// Subscribed; the snapshot of both tokens is not complete. What came so far, in order.
    Waiting { items: Vec<Item>, since: Instant },
    /// Holding its book events until they line up with the log. `snapshot` is when
    /// its snapshot completed (None: none counts, after an integrity failure).
    Pending {
        items: Vec<Item>,
        snapshot: Option<Instant>,
        since: Instant,
    },
    /// In line: its next book event is event `next`. `due`: tokens whose snapshot it
    /// still bootstraps.
    InLine { next: u64, due: [bool; 2] },
    /// Out of the market's lines until it reconnects.
    Failed,
}

#[derive(Debug)]
struct Copies {
    applied: u32,
    delivered: HashMap<ConnId, u32>,
    touched: Instant,
}

/// What upkeep did to a line (MarketMerge::tick).
#[derive(Clone, Debug, PartialEq)]
pub enum Expired {
    /// It lined up; these held events, past the log's end, are to be applied.
    Aligned(ConnId, Vec<Applied>),
    /// It could not line up: the market was rebuilt from its snapshot.
    Rebuild(ConnId, Vec<Applied>),
    /// It left the market's lines (no snapshot to rebuild from, or behind the log).
    Failed(ConnId),
}

/// Each connection's line. Tests iterate them in the same order in every run (a
/// replay compares runs step by step); production keeps std's
/// random order.
#[cfg(not(test))]
type Lines = HashMap<ConnId, Line>;
#[cfg(test)]
type Lines = HashMap<ConnId, Line, std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>>;

/// One market's merge of its connections (see the module comment).
#[derive(Debug, Default)]
pub struct MarketMerge {
    log: VecDeque<(EventKey, Instant)>,
    /// Event number of log[0].
    base: u64,
    /// Where each key sits in the log, ascending.
    positions: HashMap<EventKey, VecDeque<u64>>,
    others: HashMap<EventKey, Copies>,
    /// Prints a line got while not in line, by key: applied when it enters line
    /// if no line in line delivered them by then.
    held_prints: HashMap<ConnId, Vec<(EventKey, Value)>>,
    lines: Lines,
    last_applied: Option<Instant>,
}

impl MarketMerge {
    fn end(&self) -> u64 {
        self.base + self.log.len() as u64
    }

    fn key_at(&self, event: u64) -> Option<EventKey> {
        let i = event.checked_sub(self.base)?;
        self.log.get(i as usize).map(|(key, _)| *key)
    }

    pub fn in_line(&self, conn: ConnId) -> bool {
        matches!(self.lines.get(&conn), Some(Line::InLine { .. }))
    }

    pub fn any_in_line(&self) -> bool {
        self.lines.values().any(|line| matches!(line, Line::InLine { .. }))
    }

    /// Some connection is in line, or can start the market's book (its snapshot is
    /// coming or held). Otherwise only a fresh subscription can.
    pub fn servable(&self) -> bool {
        self.lines.values().any(|line| {
            matches!(
                line,
                Line::InLine { .. } | Line::Waiting { .. } | Line::Pending { snapshot: Some(_), .. }
            )
        })
    }

    /// The connection subscribed the market (its snapshot is due).
    pub fn subscribe(&mut self, conn: ConnId, now: Instant) {
        self.held_prints.remove(&conn);
        self.lines.insert(
            conn,
            Line::Waiting {
                items: Vec::new(),
                since: now,
            },
        );
    }

    fn push(&mut self, key: EventKey, now: Instant) {
        let event = self.end();
        self.log.push_back((key, now));
        self.positions.entry(key).or_default().push_back(event);
        self.last_applied = Some(now);
    }

    fn clear_log(&mut self) {
        self.base = self.end();
        self.log.clear();
        self.positions.clear();
    }

    /// Takes a line's items and makes it the market's source: the snapshots and
    /// events it holds are applied in the order it received them.
    fn take_over(&mut self, conn: ConnId, items: Vec<Item>, now: Instant) -> Vec<Applied> {
        let mut due = [true, true];
        let mut out = Vec::new();
        for item in items {
            match item {
                Item::Snapshot(index, msg) => {
                    due[index] = false;
                    out.push(Applied {
                        conn,
                        msg,
                        snapshot: true,
                    });
                }
                Item::Event(key, msg) => {
                    self.push(key, now);
                    out.push(Applied {
                        conn,
                        msg,
                        snapshot: false,
                    });
                }
            }
        }
        if !out.is_empty() {
            self.last_applied = Some(now);
        }
        self.lines.insert(conn, Line::InLine { next: self.end(), due });
        out.extend(self.release_prints(conn));
        out
    }

    /// The prints a line held, as it enters line: each copy it got that no line in
    /// line delivered is applied now (prints are not ordered with the book).
    fn release_prints(&mut self, conn: ConnId) -> Vec<Applied> {
        let mut out = Vec::new();
        for (key, msg) in self.held_prints.remove(&conn).unwrap_or_default() {
            let Some(copies) = self.others.get_mut(&key) else {
                continue;
            };
            let delivered = copies.delivered.get(&conn).copied().unwrap_or(0);
            if delivered > copies.applied {
                let new = (delivered - copies.applied) as usize;
                copies.applied = delivered;
                out.extend(std::iter::repeat_n(
                    Applied {
                        conn,
                        msg: msg.clone(),
                        snapshot: false,
                    },
                    new,
                ));
            }
        }
        out
    }

    /// A connection's subscription snapshot of token `index` (0 Up, 1 Down).
    pub fn snapshot(&mut self, conn: ConnId, index: usize, msg: &Value, now: Instant) -> Step {
        let someone = self.any_in_line();
        match self.lines.get_mut(&conn) {
            Some(Line::InLine { due, .. }) if due[index] => {
                due[index] = false;
                self.last_applied = Some(now);
                let mut out = vec![Applied {
                    conn,
                    msg: msg.clone(),
                    snapshot: true,
                }];
                // Lines held back while the source awaited its snapshot may fit now.
                out.extend(self.align_pending(now));
                Step::Apply(out)
            }
            Some(Line::Waiting { items, .. }) => {
                items.push(Item::Snapshot(index, msg.clone()));
                if !someone {
                    // The market's source from here on, as a single socket.
                    let items = std::mem::take(items);
                    return Step::Apply(self.take_over(conn, items, now));
                }
                let complete = (0..2).all(|i| items.iter().any(|item| matches!(item, Item::Snapshot(j, _) if *j == i)));
                if complete {
                    let items = std::mem::take(items);
                    self.lines.insert(
                        conn,
                        Line::Pending {
                            items,
                            snapshot: Some(now),
                            since: now,
                        },
                    );
                    let released = self.try_align(conn, now, false);
                    return self.released(released, now);
                }
                Step::Held
            }
            _ => Step::Ignored,
        }
    }

    /// A book event (price_change, a venue `book`, tick_size_change). The message
    /// is copied only when it is held or applied: a duplicate costs no copy.
    pub fn book_event(&mut self, conn: ConnId, key: EventKey, msg: &Value, now: Instant) -> Step {
        let end = self.end();
        match self.lines.get_mut(&conn) {
            None | Some(Line::Failed) => Step::Ignored,
            Some(Line::Waiting { items, .. }) | Some(Line::Pending { items, .. }) => {
                if items.len() >= MAX_HELD {
                    tracing::warn!("Market feed: connection {conn} held {MAX_HELD} events without lining up; out of line");
                    self.lines.insert(conn, Line::Failed);
                    return Step::Failed;
                }
                items.push(Item::Event(key, msg.clone()));
                let pending = matches!(self.lines.get(&conn), Some(Line::Pending { .. }));
                if !pending {
                    return Step::Held;
                }
                let released = self.try_align(conn, now, false);
                if released.is_empty() && self.in_line(conn) {
                    // It lined up behind the log: this event was a copy.
                    return Step::Duplicate;
                }
                self.released(released, now)
            }
            Some(Line::InLine { next, .. }) => {
                if *next < self.base {
                    self.lines.insert(conn, Line::Failed);
                    return Step::Failed;
                }
                if *next < end {
                    let at = *next;
                    return if self.key_at(at) == Some(key) {
                        if let Some(Line::InLine { next, .. }) = self.lines.get_mut(&conn) {
                            *next += 1;
                        }
                        Step::Duplicate
                    } else {
                        Step::Diverged
                    };
                }
                *next += 1;
                self.push(key, now);
                let mut out = vec![Applied {
                    conn,
                    msg: msg.clone(),
                    snapshot: false,
                }];
                out.extend(self.align_pending(now));
                Step::Apply(out)
            }
        }
    }

    fn released(&mut self, released: Vec<Applied>, now: Instant) -> Step {
        if released.is_empty() {
            return Step::Held;
        }
        let mut out = released;
        out.extend(self.align_pending(now));
        Step::Apply(out)
    }

    /// Any other message (last_trade_price): the k-th copy a connection delivers
    /// is applied when it is in line and no connection delivered k copies before.
    /// Counts start at a connection's subscription, so two identical prints (two
    /// fills of one transaction, same time) that a subscription falls between count
    /// once. Prints change no book; they date the clock and the token's lag, which
    /// the first of the pair already did.
    pub fn other(&mut self, conn: ConnId, key: EventKey, msg: &Value, now: Instant) -> Step {
        let in_line = match self.lines.get(&conn) {
            None | Some(Line::Failed) => return Step::Ignored,
            Some(line) => matches!(line, Line::InLine { .. }),
        };
        let copies = self.others.entry(key).or_insert_with(|| Copies {
            applied: 0,
            delivered: HashMap::new(),
            touched: now,
        });
        copies.touched = now;
        let delivered = copies.delivered.entry(conn).or_insert(0);
        *delivered += 1;
        if *delivered <= copies.applied {
            Step::Duplicate
        } else if in_line {
            // Copies it got while holding count too: each occurrence once.
            let new = (*delivered - copies.applied) as usize;
            copies.applied = *delivered;
            Step::Apply(vec![
                Applied {
                    conn,
                    msg: msg.clone(),
                    snapshot: false,
                };
                new
            ])
        } else {
            let held = self.held_prints.entry(conn).or_default();
            if !held.iter().any(|(k, _)| *k == key) {
                if held.len() >= MAX_HELD {
                    // As for book events: out of line rather than missing prints later.
                    tracing::warn!("Market feed: connection {conn} held {MAX_HELD} prints without lining up; out of line");
                    self.held_prints.remove(&conn);
                    self.lines.insert(conn, Line::Failed);
                    return Step::Failed;
                }
                held.push((key, msg.clone()));
            }
            Step::Held
        }
    }

    /// Where a pending line's held events line up: (j, p) when exactly one place
    /// fits, meaning its held events from the j-th on are the log's events from
    /// number p on (events past the log's end fit anywhere). Either its first held
    /// event is in the log (j = 0: it is behind or level), or the log starts inside
    /// its held events (p = the log's first event: the ones before j are older than
    /// the log, applied before it was pruned or covered by the snapshot it was
    /// rebuilt from). The place counts only when ALIGN_RUN held events match there
    /// in a row, or, `quiet`, when every held event matched up to the log's end.
    /// Nobody lines up while the market's source still waits for a snapshot (the
    /// newcomer's own may be needed if that source closes: drop_conn).
    fn alignment(&self, conn: ConnId, quiet: bool) -> Option<(usize, u64)> {
        let Some(Line::Pending { items, .. }) = self.lines.get(&conn) else {
            return None;
        };
        if self
            .lines
            .values()
            .any(|line| matches!(line, Line::InLine { due, .. } if due[0] || due[1]))
        {
            return None;
        }
        let held: Vec<EventKey> = items
            .iter()
            .filter_map(|item| match item {
                Item::Event(key, _) => Some(*key),
                Item::Snapshot(..) => None,
            })
            .collect();
        let first = *held.first()?;
        let end = self.end();
        let log_first = self.key_at(self.base)?;
        // Held events matched at a place, or None where one differs.
        let matched = |(j, start): (usize, u64)| -> Option<usize> {
            let mut count = 0;
            for (i, key) in held[j..].iter().enumerate() {
                let at = start + i as u64;
                if at >= end {
                    break;
                }
                if self.key_at(at) != Some(*key) {
                    return None;
                }
                count += 1;
            }
            Some(count)
        };
        let from_first = self
            .positions
            .get(&first)
            .into_iter()
            .flatten()
            .map(|&p| (0, p));
        let from_log_start = held
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(_, key)| **key == log_first)
            .map(|(j, _)| (j, self.base));
        // Places that fit and count: ALIGN_RUN in a row, or (quiet) every held event
        // matched exactly up to the log's end. A shorter fit elsewhere (a match that
        // runs past the log's end) does not compete with a run.
        let mut places = from_first
            .chain(from_log_start)
            .filter_map(|place| matched(place).map(|count| (place, count)))
            .filter(|&((j, start), count)| {
                let level = start + count as u64 == end && j + count == held.len();
                count >= ALIGN_RUN || (quiet && level)
            });
        let (place, _) = places.next()?;
        places.next().is_none().then_some(place)
    }

    /// Lines the connection up if its held events fit the log: returns the held
    /// events past the log's end, now applied.
    fn try_align(&mut self, conn: ConnId, now: Instant, quiet: bool) -> Vec<Applied> {
        let Some((skip, start)) = self.alignment(conn, quiet) else {
            return Vec::new();
        };
        let Some(Line::Pending { items, .. }) = self.lines.remove(&conn) else {
            return Vec::new();
        };
        let end = self.end();
        let mut next = start;
        let mut out = Vec::new();
        let events = items.into_iter().filter_map(|item| match item {
            Item::Event(key, msg) => Some((key, msg)),
            Item::Snapshot(..) => None,
        });
        for (key, msg) in events.skip(skip) {
            if next < end {
                next += 1;
                continue;
            }
            self.push(key, now);
            next += 1;
            out.push(Applied {
                conn,
                msg,
                snapshot: false,
            });
        }
        self.lines.insert(conn, Line::InLine { next, due: [false, false] });
        out.extend(self.release_prints(conn));
        out
    }

    /// Lines up every pending line the log now fits (one's released events can
    /// line up another).
    fn align_pending(&mut self, now: Instant) -> Vec<Applied> {
        let mut out = Vec::new();
        loop {
            let pending: Vec<ConnId> = self
                .lines
                .iter()
                .filter(|(_, line)| matches!(line, Line::Pending { .. }))
                .map(|(conn, _)| *conn)
                .collect();
            let mut progress = false;
            for conn in pending {
                if !self.in_line(conn) && self.alignment(conn, false).is_some() {
                    out.extend(self.try_align(conn, now, false));
                    progress = true;
                }
            }
            if !progress {
                return out;
            }
        }
    }

    /// The connection closed.
    pub fn drop_conn(&mut self, conn: ConnId, now: Instant) -> Lost {
        self.held_prints.remove(&conn);
        let was_in_line = matches!(self.lines.remove(&conn), Some(Line::InLine { .. }));
        if !was_in_line || self.any_in_line() {
            return Lost::Kept;
        }
        self.promote(now)
    }

    /// No line is in line any more (the last one closed or left): one holding a
    /// snapshot takes over, or the market has no book until a snapshot.
    pub fn promote(&mut self, now: Instant) -> Lost {
        if self.any_in_line() {
            return Lost::Kept;
        }
        self.clear_log();
        // A line holding a complete snapshot takes over (its own stream since),
        // else one holding part of one.
        let source = self
            .lines
            .iter()
            .filter_map(|(c, line)| match line {
                Line::Pending { snapshot: Some(at), .. } => Some((0, *at, *c)),
                Line::Waiting { items, since } if items.iter().any(|i| matches!(i, Item::Snapshot(..))) => {
                    Some((1, *since, *c))
                }
                _ => None,
            })
            .min_by_key(|(rank, at, c)| (*rank, std::cmp::Reverse(*at), *c));
        let Some((_, _, source)) = source else {
            return Lost::Gone;
        };
        let items = match self.lines.remove(&source) {
            Some(Line::Pending { items, .. }) | Some(Line::Waiting { items, .. }) => items,
            _ => Vec::new(),
        };
        let mut out = self.take_over(source, items, now);
        out.extend(self.align_pending(now));
        Lost::Rebuild(out)
    }

    /// An integrity failure: the applied history is in doubt. No connection is in
    /// line until a snapshot from a connection subscribed after now; every current
    /// one holds its next events to line up with that.
    pub fn reset(&mut self, now: Instant) {
        self.clear_log();
        for line in self.lines.values_mut() {
            if !matches!(line, Line::Failed) {
                *line = Line::Pending {
                    items: Vec::new(),
                    snapshot: None,
                    since: now,
                };
            }
        }
    }

    /// A pending line's own view: its snapshot and held events, in the order it
    /// got them.
    fn view(&self, conn: ConnId) -> Vec<&Value> {
        match self.lines.get(&conn) {
            Some(Line::Pending { items, .. }) => items
                .iter()
                .map(|item| match item {
                    Item::Snapshot(_, msg) | Item::Event(_, msg) => msg,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Periodic upkeep. `same_view(messages)`: a single connection that got these
    /// (a line's snapshot and held events, in order) would show the applied book
    /// of both tokens (best prices, every level, last change time).
    pub fn tick(&mut self, now: Instant, same_view: &dyn Fn(&[&Value]) -> bool) -> Vec<Expired> {
        while let Some((key, at)) = self.log.front().copied() {
            if now.saturating_duration_since(at) <= LOG_KEEP {
                break;
            }
            self.log.pop_front();
            if let Some(positions) = self.positions.get_mut(&key) {
                positions.pop_front();
                if positions.is_empty() {
                    self.positions.remove(&key);
                }
            }
            self.base += 1;
        }
        self.others
            .retain(|_, copies| now.saturating_duration_since(copies.touched) <= OTHER_KEEP);
        let mut expired = Vec::new();
        let conns: Vec<ConnId> = self.lines.keys().copied().collect();
        let awaiting_snapshot = self
            .lines
            .values()
            .any(|line| matches!(line, Line::InLine { due, .. } if due[0] || due[1]));
        for conn in conns {
            // Again for each line: one lined up just now (its events applied) leaves
            // the market no longer quiet for the next.
            let quiet = self
                .last_applied
                .is_none_or(|at| now.saturating_duration_since(at) >= QUIET);
            let someone = self.any_in_line();
            match self.lines.get(&conn) {
                Some(Line::InLine { next, .. }) if *next < self.base => {
                    self.lines.insert(conn, Line::Failed);
                    self.held_prints.remove(&conn);
                    expired.push(Expired::Failed(conn));
                    continue;
                }
                Some(Line::Pending { items, snapshot, .. }) if someone && !awaiting_snapshot => {
                    let holds_events = items.iter().any(|item| matches!(item, Item::Event(..)));
                    let snapshot_at = *snapshot;
                    if holds_events && self.alignment(conn, false).is_some() {
                        // ALIGN_RUN matched (it was held back while the source awaited
                        // a snapshot).
                        let released = self.try_align(conn, now, false);
                        let mut out = released;
                        out.extend(self.align_pending(now));
                        if !out.is_empty() {
                            expired.push(Expired::Aligned(conn, out));
                        }
                        continue;
                    }
                    // A quiet market: nothing applied for QUIET, and its own view (its
                    // snapshot and held events) is the applied book. Held events must
                    // also match the log's last ones exactly. Silence alone is no
                    // proof: a stalled source could be behind the newcomer's snapshot.
                    // Book equality is: the book is then right, though a stalled
                    // source that resumes after a same-millisecond change and its
                    // verbatim undo can disagree by position, which resets the market
                    // to a fresh snapshot (safe; without this line-up a quiet market
                    // would rebuild, and churn connections, on every new one).
                    let quiet_view = quiet
                        && snapshot_at.is_some_and(|at| now.saturating_duration_since(at) >= QUIET)
                        && (!holds_events || self.alignment(conn, true).is_some())
                        && same_view(&self.view(conn));
                    if quiet_view {
                        // A level line-up releases no book event, only held prints.
                        let released = if holds_events {
                            self.try_align(conn, now, true)
                        } else {
                            let next = self.end();
                            self.lines.insert(conn, Line::InLine { next, due: [false, false] });
                            self.release_prints(conn)
                        };
                        if !released.is_empty() {
                            expired.push(Expired::Aligned(conn, released));
                        }
                        continue;
                    }
                }
                _ => {}
            }
            if !self.timed_out(conn, now) {
                continue;
            }
            match self.lines.get(&conn) {
                Some(Line::Pending { snapshot: Some(_), .. }) => {
                    // The rebuilt book is applied by the caller after this returns:
                    // no other line may be compared with the book before it.
                    expired.push(self.rebuild_from(conn, now));
                    return expired;
                }
                Some(Line::Pending { .. }) | Some(Line::Waiting { .. }) => {
                    self.lines.insert(conn, Line::Failed);
                    self.held_prints.remove(&conn);
                    expired.push(Expired::Failed(conn));
                }
                _ => {}
            }
        }
        expired
    }

    fn timed_out(&self, conn: ConnId, now: Instant) -> bool {
        match self.lines.get(&conn) {
            Some(Line::Pending { since, .. }) | Some(Line::Waiting { since, .. }) => {
                now.saturating_duration_since(*since) >= ALIGN_TIMEOUT
            }
            _ => false,
        }
    }

    /// The market is rebuilt from `conn`'s snapshot and held events; the lines in
    /// line hold their next events to line up with it.
    fn rebuild_from(&mut self, conn: ConnId, now: Instant) -> Expired {
        let items = match self.lines.remove(&conn) {
            Some(Line::Pending { items, .. }) => items,
            _ => Vec::new(),
        };
        self.clear_log();
        for line in self.lines.values_mut() {
            if matches!(line, Line::InLine { .. }) {
                *line = Line::Pending {
                    items: Vec::new(),
                    snapshot: None,
                    since: now,
                };
            }
        }
        let mut out = self.take_over(conn, items, now);
        out.extend(self.align_pending(now));
        Expired::Rebuild(conn, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(n: u64) -> Value {
        json!({"event_type": "price_change", "market": "m", "timestamp": n.to_string(),
            "price_changes": [{"asset_id": "U", "price": "0.5", "size": n.to_string(), "side": "BUY"}]})
    }
    fn snap(token: &str) -> Value {
        json!({"event_type": "book", "asset_id": token, "timestamp": "1", "bids": [], "asks": []})
    }
    fn msgs(step: Step) -> Vec<(ConnId, Value)> {
        match step {
            Step::Apply(list) => list.into_iter().map(|a| (a.conn, a.msg)).collect(),
            other => panic!("expected Apply, got {other:?}"),
        }
    }
    fn deliver(m: &mut MarketMerge, conn: ConnId, n: u64, now: Instant) -> Step {
        m.book_event(conn, EventKey::of(&ev(n)), &ev(n), now)
    }
    /// A market with connection 1 in line from its snapshot.
    fn bootstrapped(now: Instant) -> MarketMerge {
        let mut m = MarketMerge::default();
        m.subscribe(1, now);
        assert_eq!(msgs(m.snapshot(1, 0, &snap("U"), now)).len(), 1);
        assert_eq!(msgs(m.snapshot(1, 1, &snap("D"), now)).len(), 1);
        assert!(m.in_line(1));
        m
    }

    /// The key as the feed first computed it: Hash part by part.
    fn reference_key(msg: &Value) -> EventKey {
        use std::hash::Hash;
        fn feed_hash(value: &Value, h: &mut impl Hasher) {
            match value {
                Value::Null => 0u8.hash(h),
                Value::Bool(b) => {
                    1u8.hash(h);
                    b.hash(h);
                }
                Value::Number(n) => {
                    2u8.hash(h);
                    n.to_string().hash(h);
                }
                Value::String(s) => {
                    3u8.hash(h);
                    s.hash(h);
                }
                Value::Array(items) => {
                    4u8.hash(h);
                    items.len().hash(h);
                    items.iter().for_each(|item| feed_hash(item, h));
                }
                Value::Object(map) => {
                    5u8.hash(h);
                    map.len().hash(h);
                    let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
                    for (key, item) in entries {
                        key.hash(h);
                        feed_hash(item, h);
                    }
                }
            }
        }
        let mut a = std::collections::hash_map::DefaultHasher::new();
        feed_hash(msg, &mut a);
        let mut b = std::collections::hash_map::DefaultHasher::new();
        0x9E37_79B9_7F4A_7C15u64.hash(&mut b);
        feed_hash(msg, &mut b);
        EventKey(a.finish(), b.finish())
    }

    fn random_text(rng: &mut rand::rngs::StdRng) -> String {
        use rand::Rng;
        const PIECES: [&str; 12] = ["a", "Z", "0", ".", "\"", "\\", "\n", "\u{0}", "é", "€", "\u{10FFFF}", "price"];
        (0..rng.random_range(0..12)).map(|_| PIECES[rng.random_range(0..PIECES.len())]).collect()
    }

    fn random_value(rng: &mut rand::rngs::StdRng, depth: u32) -> Value {
        use rand::Rng;
        match rng.random_range(0..if depth == 0 { 6 } else { 8 }) {
            0 => Value::Null,
            1 => Value::Bool(rng.random()),
            2 => json!(rng.random::<u64>() >> rng.random_range(0..64)),
            3 => json!(-((rng.random::<u64>() >> rng.random_range(1..64)) as i64) - 1),
            4 => {
                let f = rng.random::<f64>() * 10f64.powi(rng.random_range(-12..300)) * if rng.random() { 1.0 } else { -1.0 };
                json!(f)
            }
            5 => Value::String(random_text(rng)),
            6 => Value::Array((0..rng.random_range(0..5)).map(|_| random_value(rng, depth - 1)).collect()),
            _ => Value::Object((0..rng.random_range(0..6)).map(|_| (random_text(rng), random_value(rng, depth - 1))).collect()),
        }
    }

    #[test]
    fn keys_are_those_of_the_part_by_part_hash() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(17);
        for _ in 0..20_000 {
            let v = random_value(&mut rng, 4);
            assert_eq!(EventKey::of(&v), reference_key(&v), "{v}");
        }
        let edges = [
            json!(null), json!(true), json!(false), json!(0), json!(-0.0), json!(0.0), json!(u64::MAX), json!(i64::MIN),
            json!(1e300), json!(-1e-300), json!(0.1), json!(1.0), json!(""), json!([]), json!({}), json!([[]]), json!({"": ""}),
            json!({"a": [1, {"b": null, "a": "x"}], "A": -1}), json!("\u{0}\u{7f}\u{80}\u{7ff}\u{800}\u{ffff}\u{10000}"),
            serde_json::from_str::<Value>("[1, 1.0, -1, -1.0, 1e2, 100, 12345678901234567890, -0]").unwrap(),
        ];
        for v in edges {
            assert_eq!(EventKey::of(&v), reference_key(&v), "{v}");
        }
        // A message larger than the bytes kept between keys, and one after it.
        let big = json!({"event_type": "book", "bids": vec![json!({"price": "0.01", "size": "1"}); 60_000]});
        assert_eq!(EventKey::of(&big), reference_key(&big));
        assert_eq!(EventKey::of(&ev(3)), reference_key(&ev(3)));
        // Every message of a recorded market.
        let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/recorded_market.jsonl.gz");
        let file = std::fs::File::open(fixture).expect("fixture");
        let mut messages = 0;
        for line in std::io::BufRead::lines(std::io::BufReader::new(flate2::read::GzDecoder::new(file))) {
            let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
            let value: Value = serde_json::from_str(frame["m"].as_str().unwrap()).unwrap();
            for msg in value.as_array().cloned().unwrap_or_else(|| vec![value.clone()]) {
                assert_eq!(EventKey::of(&msg), reference_key(&msg), "{msg}");
                messages += 1;
            }
        }
        assert!(messages > 10_000, "{messages}");
    }

    #[test]
    fn serde_json_maps_keep_their_keys_in_order() {
        // EventKey hashes an object's entries as the map gives them when they are
        // in order (it sorts them otherwise, as serde_json's preserve_order would
        // need).
        let v: Value = serde_json::from_str(r#"{"b": 1, "a": 2, "c": {"y": 1, "x": 2}}"#).unwrap();
        let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["a", "b", "c"]);
    }

    #[test]
    fn keys_ignore_object_key_order_and_see_every_value() {
        let a: Value = serde_json::from_str(r#"{"b": "1", "a": [1, {"y": 2, "x": "3"}]}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a": [1, {"x": "3", "y": 2}], "b": "1"}"#).unwrap();
        let c: Value = serde_json::from_str(r#"{"a": [1, {"x": "3", "y": 2}], "b": "2"}"#).unwrap();
        assert_eq!(EventKey::of(&a), EventKey::of(&b));
        assert_ne!(EventKey::of(&a), EventKey::of(&c));
        assert_ne!(EventKey::of(&json!("1")), EventKey::of(&json!(1)));
    }

    /// Connection 2 subscribes (snapshot) on a bootstrapped market and lines up
    /// behind 1 over ALIGN_RUN shared events numbered from `first`.
    fn second_in_line(m: &mut MarketMerge, first: u64, now: Instant) {
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        for n in first..first + ALIGN_RUN as u64 {
            msgs(deliver(m, 1, n, now));
        }
        for n in first..first + ALIGN_RUN as u64 - 1 {
            assert_eq!(deliver(m, 2, n, now), Step::Held);
        }
        assert_eq!(deliver(m, 2, first + ALIGN_RUN as u64 - 1, now), Step::Duplicate);
        assert!(m.in_line(2));
    }

    #[test]
    fn the_first_copy_is_applied_and_later_copies_are_duplicates() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        // Connection 2's snapshot: pending, nothing held.
        assert_eq!(m.snapshot(2, 0, &snap("U"), now), Step::Held);
        assert_eq!(m.snapshot(2, 1, &snap("D"), now), Step::Held);
        for n in 1..=4 {
            assert_eq!(msgs(deliver(&mut m, 1, n, now)), vec![(1, ev(n))]);
        }
        // 2 delivers them later: held until ALIGN_RUN match in a row, then copies.
        for n in 1..=3 {
            assert_eq!(deliver(&mut m, 2, n, now), Step::Held);
        }
        assert_eq!(deliver(&mut m, 2, 4, now), Step::Duplicate);
        assert!(m.in_line(2));
        // 2 is first with event 5; 1's copy is a duplicate.
        assert_eq!(msgs(deliver(&mut m, 2, 5, now)), vec![(2, ev(5))]);
        assert_eq!(deliver(&mut m, 1, 5, now), Step::Duplicate);
        assert_eq!(msgs(deliver(&mut m, 1, 6, now)), vec![(1, ev(6))]);
        assert_eq!(deliver(&mut m, 2, 6, now), Step::Duplicate);
        // A repeat of the same content is a new event in the stream.
        assert_eq!(msgs(deliver(&mut m, 1, 6, now)), vec![(1, ev(6))]);
        assert_eq!(deliver(&mut m, 2, 6, now), Step::Duplicate);
        // An event at 2's cursor that differs from the log's: integrity failure.
        assert_eq!(msgs(deliver(&mut m, 1, 7, now)), vec![(1, ev(7))]);
        assert_eq!(deliver(&mut m, 2, 8, now), Step::Diverged);
    }

    #[test]
    fn a_connection_ahead_holds_its_events_until_the_log_catches_up() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        // 2 subscribed after events 1-2, which 1 has not delivered yet; 2 delivers 3-8.
        for n in 3..=8 {
            assert_eq!(deliver(&mut m, 2, n, now), Step::Held);
        }
        for n in 1..=5 {
            assert_eq!(msgs(deliver(&mut m, 1, n, now)), vec![(1, ev(n))]);
        }
        assert!(!m.in_line(2));
        // 1 delivers 6: 2's held 3-6 match in a run of four; its 7 and 8 are
        // released at once, after 6.
        assert_eq!(msgs(deliver(&mut m, 1, 6, now)), vec![(1, ev(6)), (2, ev(7)), (2, ev(8))]);
        assert!(m.in_line(2));
        assert_eq!(deliver(&mut m, 1, 7, now), Step::Duplicate);
        assert_eq!(deliver(&mut m, 1, 8, now), Step::Duplicate);
        assert_eq!(msgs(deliver(&mut m, 2, 9, now)), vec![(2, ev(9))]);
    }

    #[test]
    fn a_later_copy_of_a_repeated_message_does_not_line_up_at_the_earlier() {
        // The venue sent A, X, A (a verbatim repeat), Y. 1 has delivered A, X; 2
        // subscribed after X and delivers the second A, then Y.
        let now = Instant::now();
        let mut m = bootstrapped(now);
        let (a, x, y) = (1, 2, 3);
        msgs(deliver(&mut m, 1, a, now));
        msgs(deliver(&mut m, 1, x, now));
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        // One match at the earlier A is no proof: held, not lined up there.
        assert_eq!(deliver(&mut m, 2, a, now), Step::Held);
        assert_eq!(deliver(&mut m, 2, y, now), Step::Held);
        // 1 catches up: A, Y, then two more; 2's held A, Y and the next two match
        // at the second A, and the market never saw a false disagreement.
        for n in [a, y, 4, 5] {
            assert_eq!(msgs(deliver(&mut m, 1, n, now)), vec![(1, ev(n))]);
        }
        assert_eq!(deliver(&mut m, 2, 4, now), Step::Held);
        assert_eq!(deliver(&mut m, 2, 5, now), Step::Duplicate);
        assert!(m.in_line(2));
        assert_eq!(msgs(deliver(&mut m, 2, 6, now)), vec![(2, ev(6))]);
        assert_eq!(deliver(&mut m, 1, 6, now), Step::Duplicate);
    }

    #[test]
    fn a_run_is_not_rejected_for_a_shorter_fit_past_the_log_end() {
        // Log and held events are A B C D A: the second A also fits, with B C D A
        // past the log's end, but only the five in a row count.
        let now = Instant::now();
        let mut m = bootstrapped(now);
        for n in [1, 2, 3, 4, 1] {
            msgs(deliver(&mut m, 1, n, now));
        }
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        for n in [1, 2, 3] {
            assert_eq!(deliver(&mut m, 2, n, now), Step::Held);
        }
        assert_eq!(deliver(&mut m, 2, 4, now), Step::Duplicate);
        assert!(m.in_line(2));
        assert_eq!(deliver(&mut m, 2, 1, now), Step::Duplicate);
        assert_eq!(msgs(deliver(&mut m, 2, 5, now)), vec![(2, ev(5))]);
    }

    #[test]
    fn repeats_wait_until_one_place_fits() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        for n in [1, 2, 1, 2] {
            msgs(deliver(&mut m, 1, n, now));
        }
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        // [1, 2] fits at 0 and at 2: held.
        assert_eq!(deliver(&mut m, 2, 1, now), Step::Held);
        assert_eq!(deliver(&mut m, 2, 2, now), Step::Held);
        assert_eq!(msgs(deliver(&mut m, 1, 3, now)), vec![(1, ev(3))]);
        // [1, 2, 3] fits only at 2, three in a row: still held.
        assert_eq!(deliver(&mut m, 2, 3, now), Step::Held);
        assert_eq!(msgs(deliver(&mut m, 1, 4, now)), vec![(1, ev(4))]);
        // The fourth: in line, all were copies.
        assert_eq!(deliver(&mut m, 2, 4, now), Step::Duplicate);
        assert!(m.in_line(2));
        assert_eq!(msgs(deliver(&mut m, 1, 5, now)), vec![(1, ev(5))]);
        assert_eq!(deliver(&mut m, 2, 5, now), Step::Duplicate);
    }

    #[test]
    fn prints_are_counted_per_copy_whatever_their_order() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        second_in_line(&mut m, 1, now);
        let print = json!({"event_type": "last_trade_price", "asset_id": "U", "timestamp": "5"});
        let key = EventKey::of(&print);
        // Two identical prints (two fills): each connection's first and second copy.
        assert_eq!(msgs(m.other(2, key, &print, now)).len(), 1);
        assert_eq!(m.other(1, key, &print, now), Step::Duplicate);
        assert_eq!(msgs(m.other(1, key, &print, now)).len(), 1);
        assert_eq!(m.other(2, key, &print, now), Step::Duplicate);
        assert_eq!(m.other(3, key, &print, now), Step::Ignored);
        // A pending line's copies count: lined up, its next copy applies them all.
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        assert_eq!(m.other(2, key, &print, now), Step::Held);
        for n in 1..=ALIGN_RUN as u64 {
            msgs(deliver(&mut m, 1, n, now));
            deliver(&mut m, 2, n, now);
        }
        assert!(m.in_line(2));
        // Lining up applied the held copy; the next is the second occurrence.
        assert_eq!(msgs(m.other(2, key, &print, now)).len(), 1);
        assert_eq!(m.other(1, key, &print, now), Step::Duplicate);
        assert_eq!(m.other(1, key, &print, now), Step::Duplicate);
        // A print held while pending is applied when its line enters line, if the
        // line in line has not delivered it.
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        assert_eq!(m.other(2, key, &print, now), Step::Held);
        for n in 1..ALIGN_RUN as u64 {
            msgs(deliver(&mut m, 1, n, now));
            deliver(&mut m, 2, n, now);
        }
        msgs(deliver(&mut m, 1, ALIGN_RUN as u64, now));
        // Lining up applies the held print (its event was a copy).
        assert_eq!(msgs(deliver(&mut m, 2, ALIGN_RUN as u64, now)), vec![(2, print.clone())]);
        assert!(m.in_line(2));
        assert_eq!(m.other(1, key, &print, now), Step::Duplicate);
        // Too many held prints: out of line, not taken over with some missing.
        let mut m2 = bootstrapped(now);
        m2.subscribe(2, now);
        m2.snapshot(2, 0, &snap("U"), now);
        m2.snapshot(2, 1, &snap("D"), now);
        for n in 0..MAX_HELD {
            let p = json!({"event_type": "last_trade_price", "asset_id": "U", "timestamp": n.to_string()});
            assert_eq!(m2.other(2, EventKey::of(&p), &p, now), Step::Held);
        }
        let p = json!({"event_type": "last_trade_price", "asset_id": "U", "timestamp": "x"});
        assert_eq!(m2.other(2, EventKey::of(&p), &p, now), Step::Failed);
        assert_eq!(m2.drop_conn(1, now), Lost::Gone);
        // And when the holder takes the market over from a closed source.
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        assert_eq!(m.other(2, key, &print, now), Step::Held);
        let Lost::Rebuild(applied) = m.drop_conn(1, now) else {
            panic!("expected a rebuild");
        };
        assert_eq!(applied.iter().filter(|a| a.msg == print).count(), 1);
    }

    #[test]
    fn a_closed_source_hands_the_market_to_a_connection_with_a_snapshot() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        m.subscribe(2, now);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        deliver(&mut m, 2, 7, now); // held: 1 never delivered it
        let Lost::Rebuild(applied) = m.drop_conn(1, now) else {
            panic!("expected a rebuild");
        };
        let order: Vec<(bool, Value)> = applied.into_iter().map(|a| (a.snapshot, a.msg)).collect();
        assert_eq!(order, vec![(true, snap("U")), (true, snap("D")), (false, ev(7))]);
        assert!(m.in_line(2));
        assert_eq!(m.drop_conn(2, now), Lost::Gone);
        assert!(!m.any_in_line());
        // The next connection's snapshot bootstraps the market again.
        m.subscribe(3, now);
        assert_eq!(msgs(m.snapshot(3, 1, &snap("D"), now)), vec![(3, snap("D"))]);
        assert!(m.in_line(3));
        assert_eq!(msgs(m.snapshot(3, 0, &snap("U"), now)), vec![(3, snap("U"))]);
    }

    #[test]
    fn after_an_integrity_failure_only_a_new_snapshot_rebuilds() {
        let now = Instant::now();
        let mut m = bootstrapped(now);
        second_in_line(&mut m, 1, now);
        m.reset(now);
        assert!(!m.any_in_line());
        // Old connections hold their events; their snapshots no longer count.
        assert_eq!(deliver(&mut m, 1, 5, now), Step::Held);
        assert_eq!(m.snapshot(2, 0, &snap("U"), now), Step::Ignored);
        // A fresh connection bootstraps; the old ones line up behind it.
        m.subscribe(3, now);
        assert_eq!(msgs(m.snapshot(3, 0, &snap("U"), now)).len(), 1);
        assert_eq!(msgs(m.snapshot(3, 1, &snap("D"), now)).len(), 1);
        for n in 6..=9 {
            assert_eq!(msgs(deliver(&mut m, 3, n, now)), vec![(3, ev(n))]);
        }
        // 1 held 5, older than the new log, then 6-9: the log starts inside its
        // held events, and it lines up after four.
        for n in 6..=8 {
            assert_eq!(deliver(&mut m, 1, n, now), Step::Held);
        }
        assert_eq!(deliver(&mut m, 1, 9, now), Step::Duplicate);
        assert!(m.in_line(1));
        for n in 6..=8 {
            assert_eq!(deliver(&mut m, 2, n, now), Step::Held);
        }
        assert_eq!(deliver(&mut m, 2, 9, now), Step::Duplicate);
        assert!(m.in_line(2));
        assert_eq!(msgs(deliver(&mut m, 2, 10, now)), vec![(2, ev(10))]);
        assert_eq!(deliver(&mut m, 1, 10, now), Step::Duplicate);
    }

    #[test]
    fn a_line_does_not_line_up_while_the_source_waits_for_a_snapshot() {
        // 1 bootstraps from its Up snapshot; its Down snapshot has not come. 2 has
        // both: it waits, so that if 1 closes, 2's snapshots rebuild the market.
        let now = Instant::now();
        let mut m = MarketMerge::default();
        m.subscribe(1, now);
        m.subscribe(2, now);
        assert_eq!(msgs(m.snapshot(1, 0, &snap("U"), now)).len(), 1);
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        for n in 1..=5 {
            msgs(deliver(&mut m, 1, n, now));
            deliver(&mut m, 2, n, now);
        }
        assert!(!m.in_line(2));
        let Lost::Rebuild(applied) = m.drop_conn(1, now) else {
            panic!("expected a rebuild");
        };
        assert_eq!(applied.iter().filter(|a| a.snapshot).count(), 2);
        // Once the source has both, the other lines up as usual.
        let mut m = bootstrapped(now);
        second_in_line(&mut m, 1, now);
        // Held back while the source awaited its Down snapshot, a line that holds a
        // run lines up when that snapshot comes, and its newer events are applied.
        let mut m = MarketMerge::default();
        m.subscribe(1, now);
        m.subscribe(2, now);
        msgs(m.snapshot(1, 0, &snap("U"), now));
        m.snapshot(2, 0, &snap("U"), now);
        m.snapshot(2, 1, &snap("D"), now);
        for n in 1..=4 {
            msgs(deliver(&mut m, 1, n, now));
        }
        for n in 1..=5 {
            assert_eq!(deliver(&mut m, 2, n, now), Step::Held);
        }
        let order: Vec<(ConnId, bool)> = match m.snapshot(1, 1, &snap("D"), now) {
            Step::Apply(list) => list.into_iter().map(|a| (a.conn, a.snapshot)).collect(),
            other => panic!("{other:?}"),
        };
        assert_eq!(order, vec![(1, true), (2, false)]);
        assert!(m.in_line(2));
        assert_eq!(deliver(&mut m, 1, 5, now), Step::Duplicate);
    }

    #[test]
    fn quiet_markets_line_up_by_snapshot_and_stuck_lines_time_out() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut m = bootstrapped(t0);
        m.subscribe(2, at(100));
        m.snapshot(2, 0, &snap("U"), at(100));
        m.snapshot(2, 1, &snap("D"), at(100));
        let same = |_: &[&Value]| true;
        let differs = |_: &[&Value]| false;
        // Not quiet long enough yet.
        assert!(m.tick(at(900), &same).is_empty());
        assert!(!m.in_line(2));
        assert!(m.tick(at(1_100), &same).is_empty());
        assert!(m.in_line(2));
        // A snapshot that shows another book: after ALIGN_TIMEOUT the market is
        // rebuilt from it, and the old source lines up behind.
        m.subscribe(3, at(2_000));
        m.snapshot(3, 0, &snap("U"), at(2_000));
        m.snapshot(3, 1, &snap("D"), at(2_000));
        assert!(m.tick(at(5_000), &differs).is_empty());
        let expired = m.tick(at(12_000), &differs);
        assert!(matches!(&expired[..], [Expired::Rebuild(3, applied)] if applied.len() == 2), "{expired:?}");
        assert!(m.in_line(3) && !m.in_line(1) && !m.in_line(2));
        // Fewer than ALIGN_RUN held events that match the log's last ones line a
        // connection up once the market is quiet, and only when its own view
        // (snapshot and held events) is the applied book.
        m.subscribe(4, at(12_000));
        m.snapshot(4, 0, &snap("U"), at(12_000));
        m.snapshot(4, 1, &snap("D"), at(12_000));
        msgs(deliver(&mut m, 3, 1, at(12_100)));
        assert_eq!(deliver(&mut m, 4, 1, at(12_150)), Step::Held);
        assert!(m.tick(at(12_900), &same).is_empty(), "not quiet yet");
        assert!(m.tick(at(13_150), &differs).is_empty(), "its view is another book");
        assert!(!m.in_line(4));
        assert!(m.tick(at(13_200), &same).is_empty());
        assert!(m.in_line(4));
        // Lines without a snapshot never line up by silence: they leave the market.
        let mut expired = m.tick(at(23_000), &differs);
        expired.sort_by_key(|e| match e {
            Expired::Failed(conn) | Expired::Rebuild(conn, _) | Expired::Aligned(conn, _) => *conn,
        });
        assert_eq!(expired, vec![Expired::Failed(1), Expired::Failed(2)]);
        assert!(m.in_line(3) && m.in_line(4));
    }

    #[test]
    fn upkeep_compares_a_pending_lines_own_messages_only_on_a_quiet_market() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut m = bootstrapped(t0);
        msgs(deliver(&mut m, 1, 1, at(900)));
        m.subscribe(2, at(1_000));
        m.snapshot(2, 0, &snap("U"), at(1_000));
        m.snapshot(2, 1, &snap("D"), at(1_000));
        // Where the line keeps its snapshots.
        let stored: Vec<*const Value> = match m.lines.get(&2) {
            Some(Line::Pending { items, .. }) => items
                .iter()
                .map(|item| match item {
                    Item::Snapshot(_, msg) | Item::Event(_, msg) => msg as *const Value,
                })
                .collect(),
            other => panic!("{other:?}"),
        };
        assert_eq!(stored.len(), 2);
        let seen = std::cell::RefCell::new(Vec::new());
        let differs = |view: &[&Value]| {
            seen.borrow_mut().push(view.iter().map(|msg| *msg as *const Value).collect::<Vec<_>>());
            false
        };
        // Busy (an event applied within QUIET), then quiet with a young snapshot:
        // nothing to compare.
        assert!(m.tick(at(1_500), &differs).is_empty());
        assert!(m.tick(at(1_950), &differs).is_empty());
        assert!(seen.borrow().is_empty());
        // Quiet, its snapshot QUIET old: its own messages, in place, in order.
        assert!(m.tick(at(2_000), &differs).is_empty());
        assert_eq!(*seen.borrow(), vec![stored.clone()]);
        assert!(!m.in_line(2));
        // Equal books: in line.
        assert!(m.tick(at(2_250), &|view: &[&Value]| view.len() == 2).is_empty());
        assert!(m.in_line(2));
    }

    #[test]
    fn the_log_keeps_ten_seconds_and_a_cursor_behind_it_leaves_the_market() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut m = bootstrapped(t0);
        second_in_line(&mut m, 1, at(0));
        msgs(deliver(&mut m, 1, 5, at(100)));
        // The first four leave the log; 2's cursor (event 5, not delivered yet) is in it.
        assert!(m.tick(at(10_050), &|_: &[&Value]| false).is_empty());
        // Then 5 leaves too: 2 is behind the log.
        assert_eq!(m.tick(at(10_150), &|_: &[&Value]| false), vec![Expired::Failed(2)]);
        assert_eq!(deliver(&mut m, 2, 5, at(10_200)), Step::Ignored);
        assert!(m.servable());
        // A cursor found behind the log on delivery leaves too, and says so.
        let mut m = bootstrapped(t0);
        second_in_line(&mut m, 1, at(0));
        msgs(deliver(&mut m, 1, 5, at(100)));
        m.tick(at(10_050), &|_: &[&Value]| false);
        m.log.pop_front();
        m.base += 1;
        assert_eq!(deliver(&mut m, 2, 5, at(10_100)), Step::Failed);
        m.drop_conn(1, at(10_200));
        assert!(!m.servable(), "only a fresh subscription can start the book");
    }
}
