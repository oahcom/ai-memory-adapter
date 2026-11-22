//! SQLite-backed memory storage.

use std::{collections::HashSet, path::Path, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Row, SqlitePool,
};
use uuid::Uuid;

use crate::adapter::{AdapterError, AdapterResult, MemoryAdapter, MemoryEntry, Query};
use crate::context::injector::escape_context_content;

/// SQLite implementation of MemoryAdapter.
/// Maximum number of candidate records fetched from the database for scoring.
/// The query() function loads at most this many records before applying
/// relevance scoring and truncating to the caller's requested limit.
/// This bounds memory usage and the number of candidates scored in Rust.
/// Note: this bounds fetched/scored rows, not necessarily database rows examined;
/// the composite indexes ensure the database can serve the bounded query efficiently.
pub const MAX_QUERY_CANDIDATES: usize = 1000;

pub struct SqliteStorage {
    pool: SqlitePool,
}

// Serialize schema creation and migration across independent processes. BEGIN IMMEDIATE
// acquires SQLite's reserved write lock before inspecting the schema, so a second
// opener re-checks the column only after the first migration has committed.
async fn initialize_schema(pool: &SqlitePool) -> AdapterResult<()> {
    let mut connection = pool.acquire().await.map_err(storage_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(storage_error)?;

    let migration = async {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                work_item_id TEXT,
                content TEXT NOT NULL,
                embedding_json TEXT,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                access_count INTEGER NOT NULL CHECK (access_count >= 0),
                last_accessed TEXT NOT NULL
            )",
        )
        .execute(&mut *connection)
        .await
        .map_err(storage_error)?;

        // This check intentionally happens after BEGIN IMMEDIATE. Another process
        // may have migrated while this connection was waiting for the write lock.
        let columns = sqlx::query("PRAGMA table_info(memories)")
            .fetch_all(&mut *connection)
            .await
            .map_err(storage_error)?;
        let has_work_item_id = columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "work_item_id");
        if !has_work_item_id {
            sqlx::query("ALTER TABLE memories ADD COLUMN work_item_id TEXT")
                .execute(&mut *connection)
                .await
                .map_err(storage_error)?;
            #[cfg(test)]
            {
                if let Some(hook) = crate::test_support::schema_migration_hook() {
                    hook("after_alter_work_item_id").map_err(|e| {
                        AdapterError::Storage(format!("test schema hook injected failure: {e}"))
                    })?;
                }
            }
        }

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_work_item_last_accessed_id ON memories(work_item_id, last_accessed DESC, id DESC)")
            .execute(&mut *connection)
            .await
            .map_err(storage_error)?;
        #[cfg(test)]
        {
            let indexes = sqlx::query("PRAGMA index_list(memories)")
                .fetch_all(&mut *connection)
                .await
                .map_err(storage_error)?;
            let first_index_exists = indexes
                .iter()
                .any(|row| row.get::<String, _>("name") == "idx_memories_work_item_last_accessed_id");
            if let Some(hook) = crate::test_support::schema_migration_index_hook() {
                hook("after_first_index_created", first_index_exists).map_err(|e| {
                    AdapterError::Storage(format!("test schema hook injected failure: {e}"))
                })?;
            }
        }
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_session_created
             ON memories(session_id, created_at DESC)",
        )
        .execute(&mut *connection)
        .await
        .map_err(storage_error)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at)")
            .execute(&mut *connection)
            .await
            .map_err(storage_error)?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_session_last_accessed_id
             ON memories(session_id, last_accessed DESC, id DESC)",
        )
        .execute(&mut *connection)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_memories_last_accessed_id
             ON memories(last_accessed DESC, id DESC)",
        )
        .execute(&mut *connection)
        .await
        .map_err(storage_error)?;
        Ok::<(), AdapterError>(())
    }
    .await;

    match migration {
        Ok(()) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(storage_error)?;
            Ok(())
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

impl SqliteStorage {
    /// Opens a SQLite-backed memory store.
    ///
    /// WAL journal mode and NORMAL synchronous mode are explicitly configured
    /// to avoid depending on sqlx defaults.
    ///
    /// Persistence contract:
    ///
    /// - `access_count` and `last_accessed` are non-critical access statistics,
    ///   rather than authoritative business data. Losing recent updates is
    ///   acceptable.
    /// - WAL with NORMAL synchronous mode preserves committed transactions
    ///   across application/process crashes.
    /// - An operating-system crash or sudden power loss may discard recently
    ///   committed transactions that have not yet reached durable storage.
    /// - This reduced durability guarantee does not mean SQLite may leave
    ///   the database corrupted. SQLite maintains database consistency.
    /// - These settings must not be applied to business data that requires
    ///   FULL synchronous durability.
    ///
    /// Deployment requirement:
    ///
    /// WAL relies on shared-memory coordination and is intended for local
    /// filesystems. Network filesystems must be independently validated
    /// for WAL compatibility before deployment.
    ///
    /// Explicit configuration is intentional: the previous implementation
    /// relied on sqlx defaults, which left journal_mode at DELETE and
    /// synchronous at FULL. Each retrieval therefore incurred a rollback-
    /// journal full synchronization during its access-statistics transaction.
    /// Explicit settings prevent future third-party default changes from
    /// silently restoring that performance behavior.
    pub async fn open(path: impl AsRef<Path>) -> AdapterResult<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        // Journal-mode migration from DELETE to WAL takes an exclusive lock that
        // SQLite cannot wait on with `busy_timeout()`. When two processes open a
        // non-WAL database at the same time, the loser gets SQLITE_BUSY before
        // `initialize_schema()` runs. Retry only that transient case.
        let mut busy_retries = 0_u32;
        let pool = loop {
            let result = SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(options.clone())
                .await;

            match result {
                Ok(pool) => break pool,
                Err(error) => {
                    // SQLite extended result codes keep the primary code in the
                    // low byte, so this also covers SQLITE_BUSY_RECOVERY and
                    // SQLITE_BUSY_SNAPSHOT. Negative values are not result codes
                    // and must not be masked into a match.
                    let is_sqlite_busy = match &error {
                        sqlx::Error::Database(db_error) => {
                            is_sqlite_busy_code(db_error.code().as_deref())
                        }
                        _ => false,
                    };

                    if !is_sqlite_busy || busy_retries >= 7 {
                        return Err(storage_error(error));
                    }

                    // Bounded backoff: 50, 100, 200, 400, then 800 ms.
                    let backoff_ms = 50_u64 * (1_u64 << busy_retries.min(4));
                    busy_retries += 1;
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        };

        initialize_schema(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
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
            "INSERT INTO memories (id, session_id, work_item_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(entry.id.to_string())
        .bind(&entry.session_id)
        .bind(&entry.work_item_id)
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

        // P0-1: Bound the candidate set at the SQL level.
        // candidate_limit = clamp(result_limit * 10, 100, MAX_QUERY_CANDIDATES).
        let candidate_limit = query
            .limit
            .saturating_mul(10)
            .clamp(100, MAX_QUERY_CANDIDATES);

        // Allocate 20% to older candidates; integer remainder goes to recent.
        let older_slots = ((candidate_limit * 20) / 100).max(1);
        let recent_slots = candidate_limit - older_slots;

        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        // At least one scope is mandatory (enforced by query.validate()).
        // A missing work_item_id permits session-scoped retrieval.
        let filter_clause = match (&query.work_item_id, &query.session_id) {
            (Some(_), Some(_)) => " WHERE work_item_id = ? AND session_id = ?",
            (Some(_), None) => " WHERE work_item_id = ?",
            (None, Some(_)) => " WHERE session_id = ?",
            (None, None) => {
                return Err(AdapterError::Validation(
                    "query must be scoped by work_item_id or session_id".into(),
                ));
            }
        };

        let recent_sql = format!(
            "SELECT id, session_id, work_item_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories{} ORDER BY last_accessed DESC, id DESC LIMIT ?",
            filter_clause
        );
        let older_sql = format!(
            "SELECT id, session_id, work_item_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories{} ORDER BY last_accessed ASC, id ASC LIMIT ?",
            filter_clause
        );

        let mut recent_query = sqlx::query(&recent_sql);
        if let Some(work_item_id) = &query.work_item_id {
            recent_query = recent_query.bind(work_item_id);
        }
        if let Some(session_id) = &query.session_id {
            recent_query = recent_query.bind(session_id);
        }
        let recent_rows = recent_query
            .bind(recent_slots as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;

        let mut older_query = sqlx::query(&older_sql);
        if let Some(work_item_id) = &query.work_item_id {
            older_query = older_query.bind(work_item_id);
        }
        if let Some(session_id) = &query.session_id {
            older_query = older_query.bind(session_id);
        }
        let older_rows = older_query
            .bind(older_slots as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;

        // Deduplicate by TEXT id. Recent-layer rows take priority.
        let mut seen_ids = std::collections::HashSet::<String>::new();
        let mut rows = Vec::with_capacity(candidate_limit);

        for row in recent_rows.into_iter().chain(older_rows) {
            let id: String = row.try_get("id").map_err(storage_error)?;

            if seen_ids.insert(id) {
                rows.push(row);
            }
        }

        #[cfg(test)]
        crate::test_support::LAST_QUERY_CANDIDATE_COUNT
            .store(rows.len(), std::sync::atomic::Ordering::SeqCst);

        #[cfg(any(test, feature = "test-support"))]
        {
            let ids: Vec<uuid::Uuid> = rows
                .iter()
                .filter_map(|row| row.try_get::<String, _>("id").ok())
                .filter_map(|text| uuid::Uuid::parse_str(&text).ok())
                .collect();
            crate::test_support::record_query_candidate_ids(ids);
        }

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

        let rows = sqlx::query("SELECT id, session_id, work_item_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories WHERE session_id = ? ORDER BY last_accessed DESC, created_at DESC LIMIT 10")
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
            .map(|e| {
                format!(
                    "[{}] {}",
                    e.created_at.to_rfc3339(),
                    escape_context_content(&e.content)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(context)
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
        work_item_id: row.try_get("work_item_id").map_err(storage_error)?,
        content: row.try_get("content").map_err(storage_error)?,
        embedding,
        metadata,
        created_at,
        access_count: row.try_get("access_count").map_err(storage_error)?,
        last_accessed,
    })
}

/// Returns whether a character belongs to one of the supported CJK
/// Unified Ideographs or Compatibility Ideographs Unicode blocks.
fn is_cjk(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

/// Tokenizes text while preserving the original alphanumeric segments.
///
/// Every segment is emitted as a complete lowercase token, preserving
/// the previous behavior for English-only text.
///
/// Additionally, each contiguous CJK run emits:
/// - Character unigrams, allowing single-character partial matches.
/// - Character bigrams, allowing multi-character partial matches.
///
/// Punctuation separates segments and CJK runs. No external dependencies
/// are required.
pub fn tokenize(text: &str) -> HashSet<String> {
    let mut tokens = HashSet::new();

    for segment in text.split(|c: char| !c.is_alphanumeric()) {
        if segment.is_empty() {
            continue;
        }

        // Preserve the complete segment token.
        tokens.insert(segment.to_lowercase());

        let chars: Vec<char> = segment.chars().collect();
        let mut index = 0;

        while index < chars.len() {
            if !is_cjk(chars[index]) {
                index += 1;
                continue;
            }

            // Find the end of this contiguous CJK run.
            let start = index;
            while index < chars.len() && is_cjk(chars[index]) {
                index += 1;
            }

            let run = &chars[start..index];

            // Emit character unigrams.
            for ch in run {
                tokens.insert(ch.to_string());
            }

            // Emit character bigrams.
            for pair in run.windows(2) {
                tokens.insert(pair.iter().collect());
            }
        }
    }

    tokens
}

/// Classification used only by [`lexical_score`] when deciding whether a token
/// carries enough matching span to count as evidence.
///
/// Deliberately separate from [`is_cjk`], which drives `tokenize()` n-gram
/// expansion. That function is a tokenizer contract and is intentionally left
/// unchanged here: widening it would alter how supplementary-plane runs are
/// expanded into unigrams and bigrams. Keeping the two classifications apart is
/// a recorded trade-off, not an oversight.
fn is_cjk_unigram_char(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
            | 0x2CEB0..=0x2EE5F
            | 0x2F800..=0x2FA1F
            | 0x30000..=0x3134F
            | 0x31350..=0x323AF
    )
}

/// Scores the fraction of effective query tokens found in the content.
///
/// Single-character CJK tokens are excluded because they are unigrams,
/// not sufficient evidence of lexical relevance. Multi-character tokens
/// and non-CJK tokens, including single-character English words, remain
/// eligible to contribute to the score.
pub fn lexical_score(query_tokens: &HashSet<String>, content: &str) -> f32 {
    if query_tokens.is_empty() {
        return 0.0;
    }

    // Single-character CJK tokens are unigrams, not sufficient evidence
    // of lexical relevance. Keep multi-character tokens and all non-CJK
    // tokens, including single-character English words.
    let effective_tokens: Vec<&String> = query_tokens
        .iter()
        .filter(|token| {
            let mut chars = token.chars();
            !matches!(
                (chars.next(), chars.next()),
                (Some(ch), None) if is_cjk_unigram_char(ch)
            )
        })
        .collect();

    // A query containing only single-character CJK unigrams has no
    // effective matching span.
    if effective_tokens.is_empty() {
        return 0.0;
    }

    let content_tokens = tokenize(content);
    let matched = effective_tokens
        .iter()
        .copied()
        .filter(|t| content_tokens.contains(*t))
        .count();
    matched as f32 / effective_tokens.len() as f32
}

/// Returns whether a SQLite result code means the primary `SQLITE_BUSY` error,
/// including its extended variants.
///
/// Only the primary code in the low byte is inspected, so
/// `SQLITE_BUSY_RECOVERY` (261) and `SQLITE_BUSY_SNAPSHOT` (517) match while
/// `SQLITE_LOCKED` and its variants do not. Negative values are not result
/// codes: masking them would turn `-251` into `5` and retry a non-BUSY error.
fn is_sqlite_busy_code(code: Option<&str>) -> bool {
    code.and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| code >= 0 && code & 0xff == 5)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::MemoryEntry;
    use chrono::Utc;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn is_sqlite_busy_code_accepts_primary_and_extended_busy() {
        for code in ["5", "261", "517"] {
            assert!(
                is_sqlite_busy_code(Some(code)),
                "SQLite BUSY result code {code} must be retried"
            );
        }
    }

    #[test]
    fn is_sqlite_busy_code_rejects_negative_garbage_and_locked() {
        // -251 masks to 5 and -250 to 6 under `& 0xff`; neither is a result code.
        for code in [
            "-1",
            "-251",
            "-250",
            "6",
            "262",
            "1",
            "11",
            "not-a-code",
            "",
        ] {
            assert!(
                !is_sqlite_busy_code(Some(code)),
                "SQLite result code {code:?} must not be retried"
            );
        }
        assert!(!is_sqlite_busy_code(None));
    }

    fn test_entry(session: &str, content: &str) -> MemoryEntry {
        use chrono::Utc;
        let now = Utc::now();
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            session_id: session.into(),
            work_item_id: None,
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

    /// Verify MAX_QUERY_CANDIDATES=1000 hard cap at the SQL fetch boundary.
    /// Uses LAST_QUERY_CANDIDATE_COUNT observer (in test_support, cfg(test) only).
    #[tokio::test]
    #[ignore = "global observer; run with --ignored --test-threads=1"]
    async fn query_max_candidate_limit_1000_is_enforced() {
        let (storage, _path) = open_test_db("max-cand").await;
        let session = "max-candidate";

        for i in 0..1500 {
            let e = test_entry(
                session,
                &format!("unique special keyword item number {}", i),
            );
            storage.store(e).await.unwrap();
        }

        // limit=200 -> candidate_limit = clamp(2000, 100, 1000) = 1000
        let q = Query {
            text: "special keyword".into(),
            session_id: Some(session.into()),
            work_item_id: None,
            limit: 200,
            threshold: 0.0,
        };
        let results = storage.query(&q).await.unwrap();
        assert!(results.len() <= 200, "must respect limit=200");

        // Directly verify the SQL candidate fetch count
        let observed = crate::test_support::LAST_QUERY_CANDIDATE_COUNT
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 1000,
            "SQL must fetch exactly MAX_QUERY_CANDIDATES=1000, got {}",
            observed
        );
    }

    #[test]
    fn lexical_score_rejects_single_character_cjk_queries() {
        let query_tokens = tokenize("天");

        for content in &["今天天气很好", "天气晴朗", "无关内容"] {
            let score = lexical_score(&query_tokens, content);
            assert_eq!(score, 0.0, "content: {content}");

            for threshold in [0.10_f32, 0.20, 0.25] {
                assert!(
                    score < threshold,
                    "single-character CJK query must be rejected at threshold {threshold}"
                );
            }
        }
    }

    #[test]
    fn lexical_score_preserves_multi_character_cjk_matches() {
        let cases = [("天气", "今天天气很好"), ("机器学", "机器学习是人工智能")];

        for (query, content) in cases {
            let score = lexical_score(&tokenize(query), content);
            assert!(
                score > 0.34,
                "expected effective CJK matching span: {query} -> {content}, score={score}"
            );
        }
    }

    #[test]
    fn lexical_score_preserves_english_and_mixed_queries() {
        // A one-character English token remains valid.
        assert_eq!(lexical_score(&tokenize("a"), "a useful example"), 1.0);

        // English and multi-character CJK tokens both contribute.
        assert!(lexical_score(&tokenize("Rust 天气"), "Rust 天气很好") > 0.34);
    }

    #[test]
    fn lexical_score_documents_repeated_cjk_bigram_boundary() {
        // "天天" produces { "天", "天天" }.
        // The unigram is excluded, but the bigram remains valid.
        // This behavior is explicitly outside the single-character fix.
        let score = lexical_score(&tokenize("天天"), "今天天气很好");
        assert_eq!(score, 1.0);
    }
    #[test]
    fn lexical_score_rejects_supplementary_plane_cjk_unigrams() {
        // Extension B and Extension G, so a fix that only widens one plane fails.
        let cases = [
            ("\u{20000}", "\u{20000}"),         // Ext B
            ("\u{30000}", "\u{30000}"),         // Ext G
            ("\u{2A700}", "\u{2A700}\u{4E00}"), // Ext C vs mixed content
        ];

        for (query, content) in cases {
            let score = lexical_score(&tokenize(query), content);
            assert_eq!(
                score, 0.0,
                "supplementary-plane CJK unigram must not match: {query:?} -> {content:?}"
            );
        }
    }

    #[test]
    fn lexical_score_keeps_ascii_single_character_tokens() {
        // Widening the CJK range must not sweep in ASCII.
        assert_eq!(lexical_score(&tokenize("a"), "a useful example"), 1.0);
        assert_eq!(lexical_score(&tokenize("1"), "item 1 of 3"), 1.0);
    }

    #[test]
    fn lexical_score_keeps_multi_character_cjk_after_range_widening() {
        assert!(lexical_score(&tokenize("天气"), "今天天气很好") > 0.34);
        assert!(lexical_score(&tokenize("机器学"), "机器学习是人工智能") > 0.34);
    }
    // --------------------------------------------------------
    // Test helpers
    // --------------------------------------------------------

    fn temp_db_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("ai-memory-{}-{}.sqlite3", tag, Uuid::new_v4()))
    }

    fn remove_db_files(path: &std::path::Path) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut p = path.as_os_str().to_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    // --------------------------------------------------------
    // Test 1: New connections explicitly use WAL + NORMAL
    // --------------------------------------------------------

    #[tokio::test]
    async fn open_sets_wal_and_normal_synchronous() {
        let path = temp_db_path("pragma-new");
        let storage = SqliteStorage::open(&path).await.unwrap();

        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(mode.to_lowercase(), "wal", "journal_mode must be WAL");

        let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(sync, 1, "synchronous must be NORMAL (1)");

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 2: Reopening an existing database retains WAL
    // --------------------------------------------------------

    #[tokio::test]
    async fn reopening_existing_database_keeps_wal() {
        let path = temp_db_path("pragma-reopen");

        {
            let _storage = SqliteStorage::open(&path).await.unwrap();
        }

        let storage = SqliteStorage::open(&path).await.unwrap();

        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(
            mode.to_lowercase(),
            "wal",
            "reopened database must retain WAL journal mode"
        );

        let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(
            sync, 1,
            "new connections must explicitly use NORMAL synchronous mode"
        );

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 3: query() and inject_context() still update
    // access statistics
    // --------------------------------------------------------

    #[tokio::test]
    async fn query_and_inject_context_update_access_statistics() {
        let path = temp_db_path("access-stats");
        let storage = SqliteStorage::open(&path).await.unwrap();

        let session = "wal-access-test";
        let entry = test_entry(session, "unique wal access statistics verification content");

        let id = entry.id;
        storage.store(entry).await.unwrap();

        let (initial_count, initial_last_accessed): (i64, String) = sqlx::query_as(
            "SELECT access_count, last_accessed
                 FROM memories WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&storage.pool)
        .await
        .unwrap();

        assert_eq!(initial_count, 0);

        // Ensure the timestamp comparison is not affected by clock
        // resolution or two operations occurring in the same instant.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let query = Query {
            text: "unique wal access statistics verification".into(),
            work_item_id: None,
            session_id: Some(session.into()),
            limit: 1,
            threshold: 0.0,
        };

        let results = storage.query(&query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);

        let (query_count, query_last_accessed): (i64, String) = sqlx::query_as(
            "SELECT access_count, last_accessed
                 FROM memories WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&storage.pool)
        .await
        .unwrap();

        assert_eq!(
            query_count,
            initial_count + 1,
            "query() must increment access_count"
        );

        assert_ne!(
            query_last_accessed, initial_last_accessed,
            "query() must update last_accessed"
        );

        tokio::time::sleep(Duration::from_millis(5)).await;

        let context = storage.inject_context(session).await.unwrap();

        assert!(
            context.contains("unique wal access statistics"),
            "inject_context() must return the stored memory"
        );

        let (inject_count, inject_last_accessed): (i64, String) = sqlx::query_as(
            "SELECT access_count, last_accessed
                 FROM memories WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&storage.pool)
        .await
        .unwrap();

        assert_eq!(
            inject_count,
            query_count + 1,
            "inject_context() must increment access_count"
        );

        assert_ne!(
            inject_last_accessed, query_last_accessed,
            "inject_context() must update last_accessed"
        );

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 4: CRUD and deletion continue working under WAL
    // --------------------------------------------------------

    #[tokio::test]
    async fn crud_operations_work_with_wal() {
        let path = temp_db_path("crud-wal");
        let storage = SqliteStorage::open(&path).await.unwrap();

        let session = "wal-crud-test";
        let entry = test_entry(session, "wal crud operation verification content");

        let id = entry.id;

        // Create.
        let stored_id = storage.store(entry).await.unwrap();

        assert_eq!(stored_id, id);

        // Read.
        let query = Query {
            text: "wal crud operation verification".into(),
            work_item_id: None,
            session_id: Some(session.into()),
            limit: 1,
            threshold: 0.0,
        };

        let results = storage.query(&query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
        assert_eq!(
            results[0].content,
            "wal crud operation verification content"
        );

        // Delete (v0.1 has no pruning API; delete the row directly).
        let deleted = sqlx::query("DELETE FROM memories WHERE id = ?")
            .bind(id.to_string())
            .execute(&storage.pool)
            .await
            .unwrap()
            .rows_affected();

        assert_eq!(deleted, 1, "the stored memory must be deletable under WAL");

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(remaining, 0, "deleted memory must no longer exist");

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 5: Committed transactions survive normal process exit
    // --------------------------------------------------------

    #[tokio::test]
    async fn committed_data_survives_reopening_database() {
        let path = temp_db_path("persistence");

        let session = "wal-persistence-test";

        let expected_contents = vec![
            "persistent memory alpha".to_string(),
            "persistent memory beta".to_string(),
            "persistent memory gamma".to_string(),
        ];

        // First connection: commit several independent store operations.
        {
            let storage = SqliteStorage::open(&path).await.unwrap();

            for content in &expected_contents {
                let entry = test_entry(session, content);
                storage.store(entry).await.unwrap();
            }
        }

        // Second connection: all committed records must still exist.
        let storage = SqliteStorage::open(&path).await.unwrap();

        let query = Query {
            text: "persistent memory".into(),
            work_item_id: None,
            session_id: Some(session.into()),
            limit: expected_contents.len(),
            threshold: 0.0,
        };

        let results = storage.query(&query).await.unwrap();

        assert_eq!(
            results.len(),
            expected_contents.len(),
            "all committed records must survive reopening"
        );

        let actual: HashSet<String> = results.into_iter().map(|entry| entry.content).collect();

        let expected: HashSet<String> = expected_contents.into_iter().collect();

        assert_eq!(
            actual, expected,
            "reopened database must preserve all committed contents"
        );

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 7: Transaction rollback discards uncommitted changes
    // --------------------------------------------------------

    #[tokio::test]
    async fn waldb_transaction_rollback_discards_changes() {
        let path = temp_db_path("wal-rollback");
        let storage = SqliteStorage::open(&path).await.unwrap();

        let session = "wal-rollback-test";
        let committed = test_entry(session, "committed before rollback");
        let committed_id = committed.id;
        storage.store(committed).await.unwrap();

        let rolled_back = test_entry(session, "must be discarded");
        let rolled_back_id = rolled_back.id;

        let mut tx = storage.pool.begin().await.unwrap();

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);

        sqlx::query(
            "INSERT INTO memories (
                id, session_id, content, embedding_json, metadata_json,
                created_at, access_count, last_accessed
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(rolled_back_id.to_string())
        .bind(session)
        .bind("must be discarded")
        .bind(Option::<String>::None)
        .bind("{}")
        .bind(&timestamp)
        .bind(0_i64)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await
        .unwrap();

        tx.rollback().await.unwrap();

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE id = ?")
            .bind(rolled_back_id.to_string())
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(remaining, 0, "rolled-back insert must not persist");

        let survivors: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE id = ?")
            .bind(committed_id.to_string())
            .fetch_one(&storage.pool)
            .await
            .unwrap();

        assert_eq!(survivors, 1, "committed row must survive a later rollback");

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 8: Child-process helper for process-exit persistence
    // --------------------------------------------------------

    #[tokio::test]
    #[ignore = "invoked only by committed_data_survives_process_exit"]
    async fn write_committed_data_for_process_exit_test() {
        let Ok(db_path) = std::env::var("AI_MEMORY_PROCESS_EXIT_DB") else {
            return;
        };

        let storage = SqliteStorage::open(std::path::PathBuf::from(db_path))
            .await
            .unwrap();

        for content in ["first", "second", "third"] {
            storage
                .store(test_entry("process-exit-session", content))
                .await
                .unwrap();
        }

        // Explicitly close the pool before the child process exits.
        storage.pool.close().await;
        drop(storage);
    }

    // --------------------------------------------------------
    // Test 9: Committed transactions survive actual process exit
    // --------------------------------------------------------

    #[tokio::test]
    async fn committed_data_survives_process_exit() {
        let path = temp_db_path("process-exit");

        let exe = std::env::current_exe().unwrap();

        let status = std::process::Command::new(exe)
            .args([
                "--exact",
                "storage::sqlite::tests::write_committed_data_for_process_exit_test",
                "--ignored",
            ])
            .env("AI_MEMORY_PROCESS_EXIT_DB", &path)
            .status()
            .unwrap();

        assert!(status.success(), "child process must exit successfully");

        // The child process has terminated. Reopen the same database
        // from the parent process and verify the committed records.
        let storage = SqliteStorage::open(&path).await.unwrap();

        let contents: Vec<String> = sqlx::query_scalar(
            "SELECT content FROM memories
             WHERE session_id = ?
             ORDER BY content",
        )
        .bind("process-exit-session")
        .fetch_all(&storage.pool)
        .await
        .unwrap();

        assert_eq!(
            contents,
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
            ],
            "all committed rows must survive child-process termination"
        );

        drop(storage);
        remove_db_files(&path);
    }

    // --------------------------------------------------------
    // Test 6: Ignored performance benchmark
    //
    // 488 memories, one session.
    // Independent database for each write-back row count.
    // 3 warmups + 30 measured iterations.
    // Nearest-rank p50.
    // --------------------------------------------------------

    #[tokio::test]
    #[ignore = "manual P1-08 WAL/NORMAL performance benchmark"]
    async fn bench_query_write_back_latency() {
        use std::time::Instant;

        const MEMORY_COUNT: usize = 488;
        const WARMUP_COUNT: usize = 3;
        const SAMPLE_COUNT: usize = 30;

        const MATCHING_QUERY: &str = "benchmark wal memory shared query keyword absent";

        let session = "wal-benchmark-session";

        // All tiers use identical query text and identical corpus.
        //
        // Query tokens:
        // benchmark, wal, memory, shared, query, keyword, absent
        //
        // Each seeded memory contains the first six tokens, producing
        // a lexical score of 6/7 = 0.857.
        //
        // threshold=0.9 rejects all results.
        // threshold=0.8 accepts all results.
        //
        // Only limit and threshold differ between tiers.
        // Query tokenization and candidate scoring therefore operate
        // on the same query and corpus in all three tiers.
        //
        // (query limit, threshold, expected rows written back)
        let tiers = [(10usize, 0.9f32, 0usize), (1, 0.8, 1), (10, 0.8, 10)];

        for (query_limit, threshold, expected_rows) in tiers {
            let path = temp_db_path(&format!("benchmark-{}", expected_rows));

            // Each measurement tier receives a fresh independent database.
            let storage = SqliteStorage::open(&path).await.unwrap();

            // Seed 488 records in the same session.
            for i in 0..MEMORY_COUNT {
                let entry = test_entry(
                    session,
                    &format!("benchmark wal memory shared query keyword item {}", i),
                );

                storage.store(entry).await.unwrap();
            }

            let query = Query {
                text: MATCHING_QUERY.into(),
                work_item_id: None,
                session_id: Some(session.into()),
                limit: query_limit,
                threshold,
            };

            // Warmup: excluded from measured samples.
            for _ in 0..WARMUP_COUNT {
                let results = storage.query(&query).await.unwrap();

                assert_eq!(
                    results.len(),
                    expected_rows,
                    "query must return the expected number of rows"
                );
            }

            let mut samples: Vec<u128> = Vec::with_capacity(SAMPLE_COUNT);

            for _ in 0..SAMPLE_COUNT {
                let started = Instant::now();

                let results = storage.query(&query).await.unwrap();

                let elapsed = started.elapsed().as_micros();

                assert_eq!(
                    results.len(),
                    expected_rows,
                    "query must return the expected number of rows"
                );

                samples.push(elapsed);
            }

            // Nearest-rank p50:
            // rank = ceil(0.50 * SAMPLE_COUNT).
            samples.sort_unstable();

            let rank = (SAMPLE_COUNT * 50).div_ceil(100);
            let p50 = samples[rank - 1];

            println!("rows={} p50={}us", expected_rows, p50);

            drop(storage);
            remove_db_files(&path);
        }
    }

    /// Failure injection after ALTER TABLE verifies transactional DDL rollback,
    /// legacy-row preservation, and successful retry of the migration.
    #[tokio::test]
    #[ignore = "global hook; run with --ignored --test-threads=1"]
    async fn initialize_schema_rolls_back_ddl_failure_after_alter() {
        let path =
            std::env::temp_dir().join(format!("unit-test-schema-rollback-{}.db", Uuid::new_v4()));
        let legacy_rows = vec![
            (
                Uuid::from_u128(0x1001).to_string(),
                "legacy-session-a".to_string(),
                "legacy migration row alpha".to_string(),
                None,
                r#"{"source":"legacy-a"}"#.to_string(),
                "2024-01-01T00:00:00.000000000Z".to_string(),
                3_i32,
                "2024-03-03T00:00:00.000000000Z".to_string(),
            ),
            (
                Uuid::from_u128(0x1002).to_string(),
                "legacy-session-b".to_string(),
                "legacy migration row beta".to_string(),
                Some("[0.125,-0.5,0.75]".to_string()),
                r#"{"source":"legacy-b","rev":2}"#.to_string(),
                "2024-02-02T00:00:00.000000000Z".to_string(),
                7_i32,
                "2024-04-04T00:00:00.000000000Z".to_string(),
            ),
        ];

        let legacy_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE memories (
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
        .execute(&legacy_pool)
        .await
        .unwrap();
        for (id, session, content, embedding, metadata, created_at, access_count, last_accessed) in
            &legacy_rows
        {
            sqlx::query(
                "INSERT INTO memories
                 (id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(session)
            .bind(content)
            .bind(embedding)
            .bind(metadata)
            .bind(created_at)
            .bind(access_count)
            .bind(last_accessed)
            .execute(&legacy_pool)
            .await
            .unwrap();
        }
        legacy_pool.close().await;

        const FAILURE: &str = "F07_SCHEMA_FAILURE_AFTER_ALTER";
        let _guard = crate::test_support::set_schema_migration_hook(Arc::new(|stage| {
            if stage == "after_alter_work_item_id" {
                Err(FAILURE.to_string())
            } else {
                Ok(())
            }
        }));
        let open_error = match SqliteStorage::open(&path).await {
            Ok(_) => panic!("open must fail after the injected schema change"),
            Err(error) => error,
        };
        assert!(
            open_error.to_string().contains(FAILURE),
            "open error must preserve injected failure identifier: {open_error}"
        );
        drop(_guard);

        let inspect_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
        let rolled_back_columns = sqlx::query("PRAGMA table_info(memories)")
            .fetch_all(&inspect_pool)
            .await
            .unwrap();
        let rolled_back_names = rolled_back_columns
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        assert_eq!(
            rolled_back_names,
            vec![
                "id",
                "session_id",
                "content",
                "embedding_json",
                "metadata_json",
                "created_at",
                "access_count",
                "last_accessed"
            ],
            "ALTER must be rolled back without duplicate or partial columns"
        );
        let rolled_back_indexes = sqlx::query("PRAGMA index_list(memories)")
            .fetch_all(&inspect_pool)
            .await
            .unwrap();
        let expected_indexes = [
            "idx_memories_work_item_last_accessed_id",
            "idx_memories_session_created",
            "idx_memories_created",
            "idx_memories_session_last_accessed_id",
            "idx_memories_last_accessed_id",
        ];
        for expected in expected_indexes {
            assert!(
                !rolled_back_indexes
                    .iter()
                    .any(|row| row.get::<String, _>("name") == expected),
                "failed migration must not leave index {expected}"
            );
        }

        let read_legacy_rows = |rows: Vec<sqlx::sqlite::SqliteRow>| {
            rows.iter()
                .map(|row| {
                    (
                        row.get::<String, _>("id"),
                        row.get::<String, _>("session_id"),
                        row.get::<String, _>("content"),
                        row.get::<Option<String>, _>("embedding_json"),
                        row.get::<String, _>("metadata_json"),
                        row.get::<String, _>("created_at"),
                        row.get::<i32, _>("access_count"),
                        row.get::<String, _>("last_accessed"),
                    )
                })
                .collect::<Vec<_>>()
        };
        let before_retry =
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories ORDER BY id")
                .fetch_all(&inspect_pool)
                .await
                .unwrap();
        assert_eq!(
            read_legacy_rows(before_retry),
            legacy_rows,
            "all legacy rows and values must survive failed migration"
        );
        inspect_pool.close().await;

        let storage = SqliteStorage::open(&path)
            .await
            .expect("second open must complete migration");
        let migrated_columns = sqlx::query("PRAGMA table_info(memories)")
            .fetch_all(storage.pool())
            .await
            .unwrap();
        let migrated_names = migrated_columns
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        let unique_names = migrated_names
            .iter()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            migrated_names.len(),
            unique_names.len(),
            "column names must be unique"
        );
        assert_eq!(
            migrated_names.len(),
            9,
            "legacy schema must contain its eight original columns plus work_item_id"
        );
        assert_eq!(
            migrated_names
                .iter()
                .filter(|name| name.as_str() == "work_item_id")
                .count(),
            1
        );

        let migrated_indexes = sqlx::query("PRAGMA index_list(memories)")
            .fetch_all(storage.pool())
            .await
            .unwrap();
        for expected in expected_indexes {
            assert!(
                migrated_indexes
                    .iter()
                    .any(|row| row.get::<String, _>("name") == expected),
                "successful retry must create index {expected}"
            );
        }

        let migrated_rows =
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories ORDER BY id")
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(
            read_legacy_rows(migrated_rows),
            legacy_rows,
            "all legacy rows must remain intact after retry"
        );

        let new_entry = test_entry("legacy-session-a", "store after migration rollback probe");
        storage.store(new_entry.clone()).await.unwrap();
        let query_results = storage
            .query(&Query {
                text: "migration rollback probe".into(),
                session_id: Some("legacy-session-a".into()),
                work_item_id: None,
                limit: 5,
                threshold: 1.0,
            })
            .await
            .unwrap();
        assert_eq!(query_results.len(), 1);
        assert_eq!(query_results[0].id, new_entry.id);

        let context = storage.inject_context("legacy-session-a").await.unwrap();
        assert!(context.contains("legacy migration row alpha"));
        assert!(context.contains("store after migration rollback probe"));

        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Failure injection after the first migration index exists in the active DDL transaction.
    /// The hook receives an in-transaction PRAGMA observation proving the index was created.
    #[tokio::test]
    #[ignore = "global hook; run with --ignored --test-threads=1"]
    async fn initialize_schema_rolls_back_index_failure_after_first_index() {
        let path =
            std::env::temp_dir().join(format!("unit-test-index-rollback-{}.db", Uuid::new_v4()));
        let legacy_rows = vec![
            (
                Uuid::from_u128(0x2001).to_string(),
                "index-session-a".to_string(),
                "legacy index row alpha".to_string(),
                None,
                r#"{"source":"legacy-a"}"#.to_string(),
                "2024-05-05T00:00:00.000000000Z".to_string(),
                3_i32,
                "2024-07-07T00:00:00.000000000Z".to_string(),
            ),
            (
                Uuid::from_u128(0x2002).to_string(),
                "index-session-b".to_string(),
                "legacy index row beta".to_string(),
                Some("[0.125,-0.5,0.75]".to_string()),
                r#"{"source":"legacy-b","rev":2}"#.to_string(),
                "2024-06-06T00:00:00.000000000Z".to_string(),
                7_i32,
                "2024-08-08T00:00:00.000000000Z".to_string(),
            ),
        ];

        let legacy_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE memories (
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
        .execute(&legacy_pool)
        .await
        .unwrap();
        for (id, session, content, embedding, metadata, created_at, access_count, last_accessed) in
            &legacy_rows
        {
            sqlx::query(
                "INSERT INTO memories
                 (id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(session)
            .bind(content)
            .bind(embedding)
            .bind(metadata)
            .bind(created_at)
            .bind(access_count)
            .bind(last_accessed)
            .execute(&legacy_pool)
            .await
            .unwrap();
        }
        legacy_pool.close().await;

        const FAILURE: &str = "F08_SCHEMA_FAILURE_AFTER_FIRST_INDEX";
        let _guard = crate::test_support::set_schema_migration_index_hook(Arc::new(
            |stage, first_index_exists| {
                assert_eq!(stage, "after_first_index_created");
                assert!(
                    first_index_exists,
                    "first migration index must be visible on the same transaction connection before injection"
                );
                Err(FAILURE.to_string())
            },
        ));
        let open_error = match SqliteStorage::open(&path).await {
            Ok(_) => panic!("open must fail after the first migration index is created"),
            Err(error) => error,
        };
        assert!(
            open_error.to_string().contains(FAILURE),
            "open error must preserve injected failure identifier: {open_error}"
        );
        drop(_guard);

        let expected_indexes = [
            "idx_memories_work_item_last_accessed_id",
            "idx_memories_session_created",
            "idx_memories_created",
            "idx_memories_session_last_accessed_id",
            "idx_memories_last_accessed_id",
        ];
        let inspect_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
        let rolled_back_indexes = sqlx::query("PRAGMA index_list(memories)")
            .fetch_all(&inspect_pool)
            .await
            .unwrap();
        for expected in expected_indexes {
            assert!(
                !rolled_back_indexes
                    .iter()
                    .any(|row| row.get::<String, _>("name") == expected),
                "failed migration must roll back index {expected}"
            );
        }
        let rolled_back_columns = sqlx::query("PRAGMA table_info(memories)")
            .fetch_all(&inspect_pool)
            .await
            .unwrap();
        assert!(
            !rolled_back_columns
                .iter()
                .any(|row| row.get::<String, _>("name") == "work_item_id"),
            "ALTER must roll back together with index creation"
        );

        let read_legacy_rows = |rows: Vec<sqlx::sqlite::SqliteRow>| {
            rows.iter()
                .map(|row| {
                    (
                        row.get::<String, _>("id"),
                        row.get::<String, _>("session_id"),
                        row.get::<String, _>("content"),
                        row.get::<Option<String>, _>("embedding_json"),
                        row.get::<String, _>("metadata_json"),
                        row.get::<String, _>("created_at"),
                        row.get::<i32, _>("access_count"),
                        row.get::<String, _>("last_accessed"),
                    )
                })
                .collect::<Vec<_>>()
        };
        let expected_legacy_rows = legacy_rows.clone();
        let before_retry =
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories ORDER BY id")
                .fetch_all(&inspect_pool)
                .await
                .unwrap();
        assert_eq!(
            read_legacy_rows(before_retry),
            expected_legacy_rows,
            "legacy rows and all requested values must survive failed migration"
        );
        inspect_pool.close().await;

        let storage = SqliteStorage::open(&path)
            .await
            .expect("reopening without the hook must complete migration");
        let migrated_columns = sqlx::query("PRAGMA table_info(memories)")
            .fetch_all(storage.pool())
            .await
            .unwrap();
        assert!(
            migrated_columns
                .iter()
                .any(|row| row.get::<String, _>("name") == "work_item_id"),
            "successful retry must add work_item_id"
        );
        let migrated_indexes = sqlx::query("PRAGMA index_list(memories)")
            .fetch_all(storage.pool())
            .await
            .unwrap();
        for expected in expected_indexes {
            assert!(
                migrated_indexes
                    .iter()
                    .any(|row| row.get::<String, _>("name") == expected),
                "successful retry must create index {expected}"
            );
        }
        let after_retry =
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories ORDER BY id")
                .fetch_all(storage.pool())
                .await
                .unwrap();
        assert_eq!(
            read_legacy_rows(after_retry),
            expected_legacy_rows,
            "legacy rows must remain intact after successful retry"
        );

        let mut new_entry = test_entry("index-session-a", "scoped index migration probe");
        new_entry.work_item_id = Some("work-item-f08".into());
        storage.store(new_entry.clone()).await.unwrap();
        let scoped_results = storage
            .query(&Query {
                text: "scoped index migration probe".into(),
                session_id: Some("index-session-a".into()),
                work_item_id: Some("work-item-f08".into()),
                limit: 5,
                threshold: 1.0,
            })
            .await
            .unwrap();
        assert_eq!(scoped_results.len(), 1);
        assert_eq!(scoped_results[0].id, new_entry.id);
        assert_eq!(
            scoped_results[0].work_item_id.as_deref(),
            Some("work-item-f08")
        );

        let context = storage.inject_context("index-session-a").await.unwrap();
        assert!(context.contains("legacy index row alpha"));
        assert!(context.contains("scoped index migration probe"));

        drop(storage);
        let _ = std::fs::remove_file(path);
    }
}
