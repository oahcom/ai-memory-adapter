use ai_memory_adapter::{
    adapter::{AdapterError, MemoryAdapter, MemoryEntry, Query},
    storage::SqliteStorage,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use uuid::Uuid;

fn database_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ai-memory-adapter-{label}-{}.sqlite3",
        Uuid::new_v4()
    ))
}

fn entry(session_id: &str, content: &str) -> MemoryEntry {
    let now = Utc::now();
    MemoryEntry {
        id: Uuid::new_v4(),
        session_id: session_id.into(),
        content: content.into(),
        embedding: Some(vec![0.1, 0.2, 0.3]),
        metadata: json!({"source": "integration-test"}),
        created_at: now,
        access_count: 0,
        last_accessed: now,
    }
}

async fn open_db(label: &str) -> (SqliteStorage, PathBuf) {
    let path = database_path(label);
    let storage = SqliteStorage::open(&path).await.unwrap();
    (storage, path)
}

#[tokio::test]
async fn store_and_query_round_trip() {
    let (storage, path) = open_db("round-trip").await;
    let original = entry("session-a", "Rust memory adapter persistence");
    let id = original.id;
    assert_eq!(storage.store(original.clone()).await.unwrap(), id);

    let results = storage
        .query(&Query {
            text: "Rust memory".into(),
            session_id: Some("session-a".into()),
            limit: 10,
            threshold: 1.0,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id);
    assert_eq!(results[0].content, original.content);
    assert_eq!(results[0].embedding, original.embedding);
    assert_eq!(results[0].metadata, original.metadata);
    assert_eq!(results[0].access_count, 1);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn query_isolates_sessions() {
    let (storage, path) = open_db("session-isolation").await;
    storage
        .store(entry("session-a", "shared Rust memory topic"))
        .await
        .unwrap();
    storage
        .store(entry("session-b", "shared Rust memory topic"))
        .await
        .unwrap();

    let results = storage
        .query(&Query {
            text: "Rust memory".into(),
            session_id: Some("session-a".into()),
            limit: 10,
            threshold: 1.0,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].session_id, "session-a");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn query_applies_threshold_and_limit() {
    let (storage, path) = open_db("threshold").await;
    storage
        .store(entry("s", "Rust memory adapter"))
        .await
        .unwrap();
    storage.store(entry("s", "Rust database")).await.unwrap();
    storage.store(entry("s", "Python database")).await.unwrap();

    let results = storage
        .query(&Query {
            text: "Rust memory".into(),
            session_id: Some("s".into()),
            limit: 1,
            threshold: 0.5,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].content, "Rust memory adapter");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_context_returns_only_requested_session() {
    let (storage, path) = open_db("inject-context").await;
    storage
        .store(entry("session-a", "context for session A"))
        .await
        .unwrap();
    storage
        .store(entry("session-b", "context for session B"))
        .await
        .unwrap();

    let context = storage.inject_context("session-a").await.unwrap();
    assert!(context.contains("context for session A"));
    assert!(!context.contains("context for session B"));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn prune_deletes_entries_before_timestamp() {
    let (storage, path) = open_db("prune").await;
    let mut old_entry = entry("s", "old memory record");
    let old_time = Utc::now() - ChronoDuration::days(10);
    old_entry.created_at = old_time;
    old_entry.last_accessed = old_time;
    storage.store(old_entry).await.unwrap();
    storage
        .store(entry("s", "recent memory record"))
        .await
        .unwrap();

    let deleted = storage
        .prune(Utc::now() - ChronoDuration::days(5))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let results = storage
        .query(&Query {
            text: "memory record".into(),
            session_id: Some("s".into()),
            limit: 10,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].content, "recent memory record");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn invalid_entry_is_rejected_before_storage() {
    let (storage, path) = open_db("invalid-entry").await;
    let mut invalid = entry("s", "valid content");
    invalid.session_id = "  ".into();
    let result = storage.store(invalid).await;
    assert!(matches!(result, Err(AdapterError::Validation(_))));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn duplicate_id_returns_storage_error() {
    let (storage, path) = open_db("duplicate-id").await;
    let record = entry("s", "duplicate record");
    storage.store(record.clone()).await.unwrap();
    let result = storage.store(record).await;
    assert!(matches!(result, Err(AdapterError::Storage(_))));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn empty_session_context_is_rejected() {
    let (storage, path) = open_db("empty-session").await;
    let result = storage.inject_context(" ").await;
    assert!(matches!(result, Err(AdapterError::Validation(_))));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn database_can_be_reopened() {
    let path = database_path("reopen");
    {
        let storage = SqliteStorage::open(&path).await.unwrap();
        storage
            .store(entry("s", "persistent across reopen"))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    {
        let storage = SqliteStorage::open(&path).await.unwrap();
        let results = storage
            .query(&Query {
                text: "persistent reopen".into(),
                session_id: Some("s".into()),
                limit: 5,
                threshold: 1.0,
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "persistent across reopen");
    }
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn access_count_overflow_returns_error_without_wrapping() {
    let (storage, path) = open_db("access-overflow").await;
    let mut record = entry("s", "overflow guard memory");
    record.access_count = i32::MAX;
    let id = record.id;

    storage.store(record).await.unwrap();

    let result = storage
        .query(&Query {
            text: "overflow guard".into(),
            session_id: Some("s".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await;

    assert!(matches!(result, Err(AdapterError::Storage(_))));

    let stored_count: i32 = sqlx::query_scalar("SELECT access_count FROM memories WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await
        .unwrap();

    assert_eq!(stored_count, i32::MAX);

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn query_preserves_future_created_at_invariant() {
    let (storage, path) = open_db("future-created-query").await;
    let mut record = entry("s", "future timestamp memory");
    let future = Utc::now() + ChronoDuration::days(2);
    record.created_at = future;
    record.last_accessed = future;
    let id = record.id;

    storage.store(record).await.unwrap();

    let results = storage
        .query(&Query {
            text: "future timestamp".into(),
            session_id: Some("s".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id);
    assert!(results[0].last_accessed >= results[0].created_at);
    assert!(results[0].validate().is_ok());

    // Verify DB persisted values
    let row: (String, String) =
        sqlx::query_as("SELECT created_at, last_accessed FROM memories WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let persisted_created: chrono::DateTime<Utc> = chrono::DateTime::parse_from_rfc3339(&row.0)
        .unwrap()
        .with_timezone(&Utc);
    let persisted_accessed: chrono::DateTime<Utc> = chrono::DateTime::parse_from_rfc3339(&row.1)
        .unwrap()
        .with_timezone(&Utc);
    assert!(
        persisted_accessed >= persisted_created,
        "DB invariant violated: last_accessed < created_at"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_context_preserves_future_created_at_invariant() {
    let (storage, path) = open_db("future-created-inject").await;
    let mut record = entry("s", "future context memory");
    let future = Utc::now() + ChronoDuration::days(2);
    record.created_at = future;
    record.last_accessed = future;
    let id = record.id;

    storage.store(record).await.unwrap();

    let context = storage.inject_context("s").await.unwrap();
    assert!(context.contains("future context memory"));

    let results = storage
        .query(&Query {
            text: "future context".into(),
            session_id: Some("s".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id);
    assert!(results[0].last_accessed >= results[0].created_at);
    assert!(results[0].validate().is_ok());

    // Verify DB persisted values
    let row: (String, String) =
        sqlx::query_as("SELECT created_at, last_accessed FROM memories WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(storage.pool())
            .await
            .unwrap();
    let persisted_created: chrono::DateTime<Utc> = chrono::DateTime::parse_from_rfc3339(&row.0)
        .unwrap()
        .with_timezone(&Utc);
    let persisted_accessed: chrono::DateTime<Utc> = chrono::DateTime::parse_from_rfc3339(&row.1)
        .unwrap()
        .with_timezone(&Utc);
    assert!(
        persisted_accessed >= persisted_created,
        "DB invariant violated: last_accessed < created_at"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}
