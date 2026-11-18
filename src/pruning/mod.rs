//! Memory retention and pruning policies.

pub mod ebbinghaus;

pub use ebbinghaus::{EbbinghausPolicy, PruneReport};
