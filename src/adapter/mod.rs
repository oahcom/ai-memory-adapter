//! Memory adapter contracts and models.

pub mod error;
pub mod models;

pub use error::{AdapterError, AdapterResult};
pub use models::{MemoryEntry, Query};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Unified asynchronous memory adapter.
///
/// Implementations may use SQLite, Qdrant, or both.
#[async_trait]
pub trait MemoryAdapter: Send + Sync {
    /// Persist a memory entry.
    async fn store(&self, entry: MemoryEntry) -> AdapterResult<Uuid>;
    /// Retrieve relevant entries.
    async fn query(&self, query: &Query) -> AdapterResult<Vec<MemoryEntry>>;
    /// Build context for a session.
    async fn inject_context(&self, session_id: &str) -> AdapterResult<String>;
    /// Prune memories before a timestamp.
    async fn prune(&self, before: DateTime<Utc>) -> AdapterResult<usize>;
}
