//! The parts of the execution journal that the ledger and receipt checks use:
//! an order intent as journaled before it is sent, and the archive ages.
use serde::{Deserialize, Serialize};

/// Settled history is archived once its last event is this old, so today's
/// UTC realizations always stay in the live journal.
pub const ARCHIVE_MIN_AGE_MS: u64 = 86_400_000;
/// The shortest archive age a caller may ask for.
pub const ARCHIVE_MIN_AGE_FLOOR_MS: u64 = 900_000;

/// One order of ours, journaled before it is sent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    pub order_id: String,
    pub token_id: String,
    pub action: String,
    pub price: f64,
    pub size: f64,
    pub state: String,
    #[serde(default)]
    pub filled: f64,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default)]
    pub reserve_unit_cost: Option<f64>,
}
