//! SQLite-backed memory storage.

use std::{collections::HashSet, path::Path, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use uuid::Uuid;

use crate::adapter::{AdapterError, AdapterResult, MemoryAdapter, MemoryEntry, Query};

/// SQLite implementation of MemoryAdapter.
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
        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        let rows = if let Some(session_id) = &query.session_id {
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories WHERE session_id = ?")
                .bind(session_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage_error)?
        } else {
            sqlx::query("SELECT id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed FROM memories")
                .fetch_all(&mut *tx)
                .await
                .map_err(storage_error)?
        };

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
