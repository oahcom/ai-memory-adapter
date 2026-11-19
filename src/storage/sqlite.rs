//! SQLite-backed memory storage.

use std::{collections::HashSet, path::Path, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use uuid::Uuid;

use crate::adapter::{AdapterError, AdapterResult, MemoryAdapter, MemoryEntry, Query};
use crate::pruning::{EbbinghausPolicy, PruneReport};

/// SQLite implementation of MemoryAdapter.
/// Maximum number of candidate records fetched from the database for scoring.
/// The query() function loads at most this many records before applying
/// relevance scoring and truncating to the caller's requested limit.
/// This bounds memory usage and the number of candidates scored in Rust.
/// Note: this bounds fetched/scored rows, not necessarily database rows examined;
/// the composite indexes ensure the database can serve the bounded query efficiently.
pub const MAX_QUERY_CANDIDATES: usize = 1000;

/// Number of records processed per batch in Ebbinghaus pruning.
/// Each batch runs in its own short transaction to avoid long-running locks.
pub const PRUNE_BATCH_SIZE: usize = 256;

/// Test-only observer for the number of candidate rows fetched in query().
/// Not part of the public API; for integration testing only.
#[doc(hidden)]
pub static LAST_QUERY_CANDIDATE_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn open(path: impl AsRef<Path>) -> AdapterResult<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(storage_error)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding_json TEXT,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                access_count INTEGER NOT NULL CHECK (access_count >= 0),
                last_accessed TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .map_err(storage_error)?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_session_created
             ON memories(session_id, created_at DESC)",
        )
        .execute(&pool)
        .await
        .map_err(storage_error)?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at)")
            .execute(&pool)
            .await
            .map_err(storage_error)?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_session_last_accessed_id
             ON memories(session_id, last_accessed DESC, id DESC)",
        )
        .execute(&pool)
        .await
        .map_err(storage_error)?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_last_accessed_id
             ON memories(last_accessed DESC, id DESC)",
        )
        .execute(&pool)
        .await
        .map_err(storage_error)?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Prune memories using an Ebbinghaus retention policy with batched processing.
    ///
    /// Uses keyset pagination over the primary key to process records in bounded
    /// batches (PRUNE_BATCH_SIZE per batch). Each batch runs in its own short
    /// transaction. DELETE includes optimistic concurrency checks on
    /// `last_accessed` and `access_count` to avoid deleting records whose
    /// access metadata changed since they were read. Note: this protects
    /// access-state modifications (query/inject_context calls), not arbitrary
    /// field changes like content or metadata.
    ///
    /// **Important semantics:** This is NOT atomic across batches. If an error
    /// occurs mid-run, previously committed batches remain committed (NOT rolled back).
    /// The
    /// function returns an error (not a partial success count). Retries are safe:
    /// already-deleted records are simply not found again.
    ///
    /// # PruneReport Accounting Semantics
    ///
    /// - `entries_examined`: Total records read from the database across all batches.
    /// - `entries_deleted`: Records where the conditional DELETE affected exactly 1 row.
    /// - `entries_retained`: Records that were evaluated and determined to NOT need pruning.
    /// - **Skipped records**: If a conditional DELETE affects 0 rows (record was concurrently
    ///   modified between SELECT and DELETE), the record is counted in `entries_examined`
    ///   but in NEITHER `entries_deleted` NOR `entries_retained`. This means:
    ///   `entries_examined >= entries_deleted + entries_retained` (equality holds only
    ///   when no concurrent modifications occur).
    pub async fn prune_ebbinghaus(
        &self,
        policy: &EbbinghausPolicy,
        evaluated_at: DateTime<Utc>,
    ) -> AdapterResult<PruneReport> {
        policy.validate()?;

        // Read the max ID as scan upper bound to provide a finite scan boundary.
        // Note: UUID identifiers are not monotonic, so a record inserted during
        // pruning with an ID between the cursor and upper bound may still be
        // included. The upper bound prevents unbounded scan growth but does not
        // guarantee a consistent snapshot of the original record set.
        let upper_bound: Option<String> = sqlx::query("SELECT MAX(id) FROM memories")
            .fetch_one(&self.pool)
            .await
            .map_err(storage_error)?
            .get(0);

        let upper_bound = match upper_bound {
            Some(id) => id,
            None => {
                // Empty table
                return Ok(PruneReport {
                    entries_examined: 0,
                    entries_deleted: 0,
                    entries_retained: 0,
                    prune_threshold: policy.prune_threshold,
                    evaluated_at,
                });
            }
        };

        let mut report = PruneReport {
            entries_examined: 0,
            entries_deleted: 0,
            entries_retained: 0,
            prune_threshold: policy.prune_threshold,
            evaluated_at,
        };

        let mut last_seen_id = String::new(); // empty string sorts before all UUIDs

        loop {
            // Fetch one batch using keyset pagination.
            let rows = sqlx::query(
                "SELECT id, session_id, content, embedding_json, metadata_json,
                        created_at, access_count, last_accessed
                 FROM memories
                 WHERE id > ? AND id <= ?
                 ORDER BY id ASC
                 LIMIT ?",
            )
            .bind(&last_seen_id)
            .bind(&upper_bound)
            .bind(PRUNE_BATCH_SIZE as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;

            if rows.is_empty() {
                break;
            }

            let batch_len = rows.len();
            report.entries_examined += batch_len;

            // Advance the cursor before processing so the current invocation
            // does not revisit already-fetched rows. The cursor is in-memory
            // only; if this invocation fails, a subsequent prune call starts
            // scanning from the beginning. Previously committed deletions
            // remain absent, while uncommitted records are re-evaluated.
            let last_row_id: String = rows[batch_len - 1].get("id");
            last_seen_id = last_row_id;

            // Test-only hook: called after SELECT, before DELETE
            #[cfg(test)]
            {
                if let Some(hook) = crate::test_support::prune_hook() {
                    hook(batch_len).map_err(|e| {
                        AdapterError::Storage(format!("test hook injected failure: {e}"))
                    })?;
                }
            }

            // Collect IDs to delete with their read-time state for optimistic check.
            let mut to_delete: Vec<(String, String, i32)> = Vec::new(); // (id, last_accessed, access_count)

            for row in &rows {
                let entry = decode_entry(row)?;

                if policy.should_prune(&entry, evaluated_at)? {
                    to_delete.push((
                        entry.id.to_string(),
                        entry
                            .last_accessed
                            .to_rfc3339_opts(SecondsFormat::Nanos, true),
                        entry.access_count,
                    ));
                } else {
                    report.entries_retained += 1;
                }
            }

            // Process deletions in a short per-batch transaction.
            if !to_delete.is_empty() {
                let mut tx = self.pool.begin().await.map_err(storage_error)?;

                for (id, read_last_accessed, read_access_count) in &to_delete {
                    // Optimistic concurrency: only delete if the record has not
                    // changed since we read it. If it was concurrently updated
                    // (e.g. access_count incremented), skip it this round.
                    let result = sqlx::query(
                        "DELETE FROM memories WHERE id = ? AND last_accessed = ? AND access_count = ?",
                    )
                    .bind(id)
                    .bind(read_last_accessed)
                    .bind(read_access_count)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage_error)?;

                    if result.rows_affected() == 1 {
                        report.entries_deleted += 1;
                    }
                    // rows_affected == 0 means record was concurrently modified;
                    // skip it (will be re-evaluated in a future prune run).
                }

                tx.commit().await.map_err(storage_error)?;
            }
        }

        Ok(report)
    }
}

#[async_trait::async_trait]
impl MemoryAdapter for SqliteStorage {
    async fn store(&self, entry: MemoryEntry) -> AdapterResult<Uuid> {
        entry.validate()?;

        let embedding_json = entry
            .embedding
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let metadata_json = serde_json::to_string(&entry.metadata)?;

        sqlx::query(
            "INSERT INTO memories (id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.id.to_string())
        .bind(&entry.session_id)
        .bind(&entry.content)
        .bind(embedding_json)
        .bind(metadata_json)
        .bind(entry.created_at.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .bind(entry.access_count)
        .bind(entry.last_accessed.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        Ok(entry.id)
    }

    async fn query(&self, query: &Query) -> AdapterResult<Vec<MemoryEntry>> {
        query.validate()?;

        // P0-1: Bound the candidate set at the SQL level to prevent unbounded
        // full-table loading. candidate_limit = clamp(result_limit * 10, 100, MAX_QUERY_CANDIDATES).
        // Scoring runs only on this bounded set; see API docs for recall implications.
        let candidate_limit = query
            .limit
            .saturating_mul(10)
            .clamp(100, MAX_QUERY_CANDIDATES);

        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        let rows = if let Some(session_id) = &query.session_id {
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories WHERE session_id = ? ORDER BY last_accessed DESC, id DESC LIMIT ?")
                .bind(session_id)
                .bind(candidate_limit as i64)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage_error)?
        } else {
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories ORDER BY last_accessed DESC, id DESC LIMIT ?")
                .bind(candidate_limit as i64)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage_error)?
        };

        LAST_QUERY_CANDIDATE_COUNT.store(rows.len(), std::sync::atomic::Ordering::SeqCst);

        let query_tokens = tokenize(&query.text);
        let mut matches: Vec<(f32, MemoryEntry)> = Vec::new();

        for row in rows {
            let entry = decode_entry(&row)?;
            let score = lexical_score(&query_tokens, &entry.content);
            if score >= query.threshold {
                matches.push((score, entry));
            }
        }

        matches.sort_by(|(sa, ea), (sb, eb)| {
            sb.total_cmp(sa)
                .then_with(|| eb.created_at.cmp(&ea.created_at))
        });
        matches.truncate(query.limit);

        let now = Utc::now();
        let mut results = Vec::with_capacity(matches.len());

        for (_, mut entry) in matches {
            record_access(&mut entry, now)?;
            sqlx::query("UPDATE memories SET access_count = ?, last_accessed = ? WHERE id = ?")
                .bind(entry.access_count)
                .bind(
                    entry
                        .last_accessed
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                )
                .bind(entry.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
            results.push(entry);
        }

        tx.commit().await.map_err(storage_error)?;
        Ok(results)
    }

    async fn inject_context(&self, session_id: &str) -> AdapterResult<String> {
        if session_id.trim().is_empty() {
            return Err(AdapterError::Validation(
                "session_id must not be empty".into(),
            ));
        }

        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        let rows = sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories WHERE session_id = ? ORDER BY last_accessed DESC, created_at DESC LIMIT 10")
            .bind(session_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;

        let now = Utc::now();
        let mut entries = Vec::with_capacity(rows.len());

        for row in rows {
            let mut entry = decode_entry(&row)?;
            record_access(&mut entry, now)?;
            sqlx::query("UPDATE memories SET access_count = ?, last_accessed = ? WHERE id = ?")
                .bind(entry.access_count)
                .bind(
                    entry
                        .last_accessed
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                )
                .bind(entry.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
            entries.push(entry);
        }

        tx.commit().await.map_err(storage_error)?;

        let context = entries
            .iter()
            .map(|e| format!("[{}] {}", e.created_at.to_rfc3339(), e.content))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(context)
    }

    async fn prune(&self, before: DateTime<Utc>) -> AdapterResult<usize> {
        let result = sqlx::query("DELETE FROM memories WHERE created_at < ?")
            .bind(before.to_rfc3339_opts(SecondsFormat::Nanos, true))
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(result.rows_affected() as usize)
    }
}

fn storage_error(error: impl std::fmt::Display) -> AdapterError {
    AdapterError::Storage(error.to_string())
}

/// Increment access metadata without violating MemoryEntry invariants.
fn record_access(entry: &mut MemoryEntry, accessed_at: DateTime<Utc>) -> AdapterResult<()> {
    entry.access_count = entry
        .access_count
        .checked_add(1)
        .ok_or_else(|| AdapterError::Storage("access_count overflow".into()))?;

    entry.last_accessed = if accessed_at < entry.created_at {
        entry.created_at
    } else {
        accessed_at
    };

    Ok(())
}

fn parse_timestamp(value: String) -> AdapterResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|t| t.with_timezone(&Utc))
        .map_err(storage_error)
}

fn decode_entry(row: &sqlx::sqlite::SqliteRow) -> AdapterResult<MemoryEntry> {
    let id_text: String = row.try_get("id").map_err(storage_error)?;
    let id = Uuid::parse_str(&id_text).map_err(storage_error)?;

    let embedding_text: Option<String> = row.try_get("embedding_json").map_err(storage_error)?;
    let embedding = embedding_text
        .map(|v| serde_json::from_str::<Vec<f32>>(&v))
        .transpose()?;

    let metadata_text: String = row.try_get("metadata_json").map_err(storage_error)?;
    let metadata = serde_json::from_str(&metadata_text)?;

    let created_at = parse_timestamp(row.try_get("created_at").map_err(storage_error)?)?;
    let last_accessed = parse_timestamp(row.try_get("last_accessed").map_err(storage_error)?)?;

    Ok(MemoryEntry {
        id,
        session_id: row.try_get("session_id").map_err(storage_error)?,
        content: row.try_get("content").map_err(storage_error)?,
        embedding,
        metadata,
        created_at,
        access_count: row.try_get("access_count").map_err(storage_error)?,
        last_accessed,
    })
}

fn tokenize(text: &str) -> HashSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn lexical_score(query_tokens: &HashSet<String>, content: &str) -> f32 {
    if query_tokens.is_empty() {
        return 0.0;
    }
    let content_tokens = tokenize(content);
    let matched = query_tokens
        .iter()
        .filter(|t| content_tokens.contains(*t))
        .count();
    matched as f32 / query_tokens.len() as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::MemoryEntry;
    use crate::pruning::EbbinghausPolicy;
    use crate::test_support::{set_prune_hook, SyncBarrier};
    use chrono::{Duration as ChronoDuration, Utc};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_entry(session: &str, content: &str) -> MemoryEntry {
        use chrono::Utc;
        let now = Utc::now();
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            session_id: session.into(),
            content: content.into(),
            embedding: Some(vec![0.1, 0.2, 0.3]),
            metadata: serde_json::json!({"source": "unit-test"}),
            created_at: now,
            access_count: 0,
            last_accessed: now,
        }
    }

    async fn open_test_db(name: &str) -> (SqliteStorage, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("unit-test-{}-{}.db", name, Uuid::new_v4()));
        let storage = SqliteStorage::open(&path).await.unwrap();
        (storage, path)
    }

    /// Concurrency test: pause prune_ebbinghaus() between SELECT and DELETE,
    /// modify a candidate record via second connection, resume production DELETE.
    /// Assert modified record survives and report counts are correct.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "global hook; run with --ignored --test-threads=1"]
    async fn prune_concurrent_modification_via_production_path() {
        let (storage, path) = open_test_db("conc").await;
        let session = "conc-test";

        let old = Utc::now() - ChronoDuration::days(365);
        for i in 0..10 {
            let mut e = test_entry(session, &format!("victim {}", i));
            e.created_at = old;
            e.last_accessed = old;
            storage.store(e).await.unwrap();
        }

        // Set up sync barrier: hook fires after batch SELECT, waits for our signal
        let barrier = SyncBarrier::new();
        let _guard = set_prune_hook(barrier.hook());

        // Clone storage for second connection to modify records
        let storage2 = SqliteStorage::open(&path).await.unwrap();

        // Spawn prune on a blocking task so we can interact concurrently
        let storage_clone = SqliteStorage::open(&path).await.unwrap();
        let policy = EbbinghausPolicy::default();
        let handle =
            tokio::spawn(async move { storage_clone.prune_ebbinghaus(&policy, Utc::now()).await });

        // Wait for hook to fire (prune has done SELECT, paused before DELETE)
        assert!(
            barrier.wait_fired(std::time::Duration::from_secs(5)),
            "hook must fire within timeout"
        );

        // NOW modify a candidate record via second connection (between SELECT and DELETE)
        // Direct SQL UPDATE: change access_count and last_accessed
        let target_id: String =
            sqlx::query("SELECT id FROM memories WHERE content LIKE 'victim 3%' LIMIT 1")
                .fetch_one(storage2.pool())
                .await
                .unwrap()
                .get(0);
        sqlx::query(
            "UPDATE memories SET access_count = access_count + 1, last_accessed = ?1 WHERE id = ?2",
        )
        .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true))
        .bind(&target_id)
        .execute(storage2.pool())
        .await
        .unwrap();

        // Signal hook to proceed — production DELETE now runs with stale snapshot
        barrier.proceed();

        // Wait for prune to complete
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "prune must complete successfully");

        let report = result.unwrap();
        // 10 examined, 9 deleted, 0 retained (1 modified = skipped by conditional DELETE)
        assert_eq!(report.entries_examined, 10);
        assert_eq!(report.entries_deleted, 9);
        assert_eq!(report.entries_retained, 0); // modified record is skipped (not deleted, not retained)

        // Modified record must still exist (check via direct SQL)
        let count: i64 = sqlx::query("SELECT COUNT(*) FROM memories")
            .fetch_one(storage.pool())
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            count, 1,
            "concurrently modified record must survive production DELETE, got {} remaining",
            count
        );

        drop(_guard); // clear hook
        drop(storage);
        drop(storage2);
        let _ = std::fs::remove_file(path);
    }

    /// Failure injection: first batch commits, second batch fails after SELECT
    /// but before DELETE. Verify first batch remains deleted, second batch
    /// records remain (DELETE never executed). Retry cleans up remainder.
    #[tokio::test]
    #[ignore = "global hook; run with --ignored --test-threads=1"]
    async fn prune_partial_failure_batches() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        CALLS.store(0, Ordering::SeqCst);

        let _guard = set_prune_hook(Arc::new(move |_n: usize| {
            let c = CALLS.fetch_add(1, Ordering::SeqCst);
            if c >= 1 {
                Err(format!("injected failure at call {}", c))
            } else {
                Ok(())
            }
        }));

        let (storage, _path) = open_test_db("fail").await;
        let session = "fail-test";

        let old = Utc::now() - ChronoDuration::days(365);
        for i in 0..300 {
            let mut e = test_entry(session, &format!("fv {}", i));
            e.created_at = old;
            e.last_accessed = old;
            storage.store(e).await.unwrap();
        }

        let policy = EbbinghausPolicy::default();
        let result = storage.prune_ebbinghaus(&policy, Utc::now()).await;
        assert!(result.is_err(), "must fail on second batch");

        // Count remaining records via direct SQL (bypasses query scoring)
        let count: i64 = sqlx::query("SELECT COUNT(*) FROM memories")
            .fetch_one(storage.pool())
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            count, 44,
            "batch 1 (256) committed, batch 2 (44) DELETE not executed: got {} remaining",
            count
        );

        drop(_guard); // clear hook

        // Retry without hook — should clean up remaining 44
        let result2 = storage.prune_ebbinghaus(&policy, Utc::now()).await;
        assert!(result2.is_ok(), "retry must succeed");
        let report2 = result2.unwrap();
        assert_eq!(report2.entries_examined, 44);
        assert_eq!(report2.entries_deleted, 44);

        let count2: i64 = sqlx::query("SELECT COUNT(*) FROM memories")
            .fetch_one(storage.pool())
            .await
            .unwrap()
            .get(0);
        assert_eq!(count2, 0, "all records must be deleted after retry");
    }

    /// Report accounting identity: examined == deleted + retained (no skips).
    #[tokio::test]
    async fn report_accounting_identity() {
        let (storage, path) = open_test_db("acct").await;
        let session = "acct-test";

        let old = Utc::now() - ChronoDuration::days(365);
        for i in 0..10 {
            let mut e = test_entry(session, &format!("acc {}", i));
            e.created_at = old;
            e.last_accessed = old;
            storage.store(e).await.unwrap();
        }

        // Make 3 fresh via second connection
        let storage2 = SqliteStorage::open(&path).await.unwrap();
        for i in 0..3 {
            let q = Query {
                text: format!("acc {}", i),
                session_id: Some(session.into()),
                limit: 1,
                threshold: 0.0,
            };
            let _ = storage2.query(&q).await.unwrap();
        }

        let policy = EbbinghausPolicy::default();
        let report = storage.prune_ebbinghaus(&policy, Utc::now()).await.unwrap();

        assert_eq!(report.entries_examined, 10);
        assert_eq!(report.entries_retained, 3);
        assert_eq!(report.entries_deleted, 7);
        assert_eq!(
            report.entries_examined,
            report.entries_deleted + report.entries_retained,
            "examined == deleted + retained when no concurrent skips"
        );
    }
}
