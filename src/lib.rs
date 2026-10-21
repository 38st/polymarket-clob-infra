//! Infrastructure extracted from a Rust market maker on Polymarket's CLOB:
//! the order book, redundant-feed merging and lag detection, EIP-712 order
//! signing, and the execution ledger. The trading strategy is not included.
pub mod abi;
pub mod accounting;
pub mod execution;
pub mod market_data;
pub mod polymarket;
pub mod receipts;
pub mod ws;
