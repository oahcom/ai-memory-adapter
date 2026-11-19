//! Test-only observation facade. Compiled only with `--features test-support`.
//! Never part of a production build.

#![cfg(feature = "test-support")]


use uuid::Uuid;

/// Returns the candidate ids that the most recent `SqliteStorage::query()` call
/// actually selected, before scoring. Empty if no query has run yet.
pub fn last_query_candidate_ids() -> Vec<Uuid> {
    crate::test_support::last_query_candidate_ids()
}
