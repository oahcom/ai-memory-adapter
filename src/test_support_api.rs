//! Testing-only observation facade.
//! This module is compiled only with `--features test-support`, which is not enabled
//! by default and is therefore excluded from the default build.
//! This is NOT a stable API; it may evolve or be removed as the testing mechanisms change.

#![cfg(feature = "test-support")]

use uuid::Uuid;

/// Returns the candidate ids that the most recent `SqliteStorage::query()` call
/// actually selected, before scoring. Empty if no query has run yet.
pub fn last_query_candidate_ids() -> Vec<Uuid> {
    crate::test_support::last_query_candidate_ids()
}
