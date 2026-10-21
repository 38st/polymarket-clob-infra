# polymarket-clob-infra

Infrastructure from a Rust market maker I built for
[Polymarket](https://polymarket.com)'s central limit order book. This
repository holds the parts that are independent of any trading strategy: the
order book, the merge of redundant market-data connections, connection-lag
detection, EIP-712 order signing, and the execution ledger. Strategy, risk
rules, execution logic and research are not included.

## What's here

| Module | What it does |
|---|---|
| [`market_data`](src/market_data.rs) | One atomic snapshot of every subscribed book for each decision. Prices are integer millionths (exact for every tick size). Out-of-order and future-dated messages are refused. Books stay current on a quiet market only while the stream proves it is alive. |
| [`ws::feed_merge`](src/ws/feed_merge.rs) | Merges N websocket connections into one book per market. Each venue event is applied once, from whichever connection delivers it first, in the venue's order. A connection is "lined up" against a log of applied events by content key. If two connections disagree about the market's history, the market is rebuilt from a fresh snapshot. |
| [`ws::feed_lag`](src/ws/feed_lag.rs) | Delivery lag per connection over a rolling window (a histogram with quantiles), a check of the local clock against the venue's, and the settings that replace a connection that stays late. |
| [`polymarket::signing`](src/polymarket/signing.rs), [`abi`](src/abi.rs) | EIP-712 typed-data signing of CLOB orders with secp256k1. Checked against the official Python SDK's test vectors. The key's bytes are zeroized on drop. |
| [`polymarket::auth`](src/polymarket/auth.rs) | L1 (EIP-712) and L2 (HMAC) API authentication. |
| [`accounting`](src/accounting.rs), [`receipts`](src/receipts.rs) | A durable record of executions and an average-cost portfolio built from it. A missing fee or settlement stays *unknown* and is never treated as zero. Fills are checked against the on-chain settlement receipts, and archived history is compacted. |

## Design notes and measurements

These come from the module docs, where each is explained in full.

- **Why redundant feeds.** Measured over 140 two-minute probe runs: median delivery lag was 10 ms, but p90 was 62-83 ms and
  p99 about 370 ms. 3.6% of connections were 75-87 ms late for their whole
  life. A single long-lived socket can sit 80 ms behind without knowing it.
- **Why merge by position.** Two recorders compared over 1.39 M common book
  events delivered them in the same order on every connection, with no swaps.
  Trade prints float against them (2,757 inversions), and the venue repeats
  single messages verbatim (1,105 in 6.5 M events, never three in a row). So
  book events are merged by their position in a shared log, prints are
  counted by copy, and a connection lines up only after 4 consecutive matches.
- **Clock-free comparison.** Comparing two connections' lags needs no clock,
  because the local clock's error cancels out. An absolute lag limit does need
  one, so it applies only while a clock check bounds the offset to about one
  round trip. The check works even though the venue reports whole seconds: it
  times its requests across the venue's second boundaries.
- **Exact money.** Fill and receipt checks work in on-chain integer units.
  The venue rounds each partial fill to whole units, so the tolerance is
  exactly one unit, not a float epsilon.
- **Signing speed.** An order's EIP-712 hash plus ECDSA signature takes 44 µs at
  p50 and 62 µs at p99 (Apple M2 Max, release build).
- Release builds keep integer-overflow checks on. A test fails if they are
  ever turned off.

## Running

```bash
cargo test --release
```

The feed-merge tests replay every message of a recorded live market
(`tests/fixtures/*.jsonl.gz`, public venue data). Signing is checked against the
official SDK's vectors using the public test key `0x…01`.

Signing benchmark:

```bash
cargo test --release speed_bench_sign -- --ignored --nocapture
```

## Context

Some doc comments refer to components of the full engine that are not in this
repository: the websocket session layer that drives `feed_merge`, the
execution journal, and the venue client. Each module here builds and tests on
its own.
