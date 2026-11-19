//! Storage backends.

pub mod sqlite;

pub use sqlite::SqliteStorage;
#[doc(hidden)]
pub use sqlite::LAST_QUERY_CANDIDATE_COUNT;
