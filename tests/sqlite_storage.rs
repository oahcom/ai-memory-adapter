use ai_memory_adapter::{
    adapter::{AdapterError, MemoryAdapter, MemoryEntry, Query},
    pruning::EbbinghausPolicy,
    storage::SqliteStorage,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
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
        work_item_id: None,
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
            work_item_id: None,
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
            work_item_id: None,
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
            work_item_id: None,
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
            work_item_id: None,
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
                work_item_id: None,
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
            work_item_id: None,
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
            work_item_id: None,
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
            work_item_id: None,
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

// === P0-1 Regression Tests: Bounded Query Candidates ===

#[tokio::test]
async fn query_candidate_limit_is_respected() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "candidate-limit-test";

    // Insert 50 entries (well under MAX_QUERY_CANDIDATES but enough to verify LIMIT works)
    for i in 0..50 {
        let entry = entry(session, &format!("memory number {} about rust", i));
        storage.store(entry).await.unwrap();
    }

    // Query with limit=5 — should only get 5 results despite 50 candidates
    let q = Query {
        text: "rust".into(),
        session_id: Some(session.into()),
        work_item_id: None,
        limit: 5,
        threshold: 0.0,
    };
    let results = storage.query(&q).await.unwrap();
    assert!(
        results.len() <= 5,
        "results must not exceed limit: got {}",
        results.len()
    );
}

#[tokio::test]
async fn query_deterministic_ordering() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "deterministic-test";

    // Insert entries with same content but different creation times
    for i in 0..10 {
        let mut entry = entry(session, "deterministic content");
        entry.created_at = chrono::Utc::now() - chrono::Duration::seconds(100 - i);
        entry.last_accessed = entry.created_at;
        storage.store(entry).await.unwrap();
    }

    let q = Query {
        text: "deterministic".into(),
        session_id: Some(session.into()),
        work_item_id: None,
        limit: 5,
        threshold: 0.0,
    };

    // Query twice — results must be identical
    let r1 = storage.query(&q).await.unwrap();
    let r2 = storage.query(&q).await.unwrap();

    assert_eq!(r1.len(), r2.len(), "same query should return same count");
    for (a, b) in r1.iter().zip(r2.iter()) {
        assert_eq!(a.id, b.id, "deterministic ordering: same IDs in same order");
    }
}

#[tokio::test]
async fn query_with_many_entries_stays_bounded() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "bounded-test";

    // Insert 200 entries
    for i in 0..200 {
        let entry = entry(session, &format!("entry {} with searchable keyword", i));
        storage.store(entry).await.unwrap();
    }

    let q = Query {
        text: "searchable".into(),
        session_id: Some(session.into()),
        work_item_id: None,
        limit: 10,
        threshold: 0.0,
    };
    let results = storage.query(&q).await.unwrap();
    assert!(
        results.len() <= 10,
        "must respect limit even with 200 entries: got {}",
        results.len()
    );
}

#[tokio::test]
async fn query_session_isolation_with_candidates() {
    let (storage, _path) = open_db("p0-test").await;

    // Session A: 20 entries
    for i in 0..20 {
        let entry = entry("session-a", &format!("alpha entry {}", i));
        storage.store(entry).await.unwrap();
    }
    // Session B: 20 entries
    for i in 0..20 {
        let entry = entry("session-b", &format!("beta entry {}", i));
        storage.store(entry).await.unwrap();
    }

    let q = Query {
        text: "entry".into(),
        session_id: Some("session-a".into()),
        work_item_id: None,
        limit: 50,
        threshold: 0.0,
    };
    let results = storage.query(&q).await.unwrap();
    for r in &results {
        assert_eq!(r.session_id, "session-a", "session isolation must hold");
    }
}

// === P0-2 Regression Tests: Batch Ebbinghaus Pruning ===

#[tokio::test]
async fn prune_ebbinghaus_empty_table() {
    let (storage, _path) = open_db("p0-test").await;
    let policy = EbbinghausPolicy::default();
    let report = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(report.entries_examined, 0);
    assert_eq!(report.entries_deleted, 0);
    assert_eq!(report.entries_retained, 0);
}

#[tokio::test]
async fn prune_ebbinghaus_single_batch() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "single-batch";

    // Insert 10 entries with old timestamps (all should be pruned)
    for i in 0..10 {
        let mut entry = entry(session, &format!("old memory {}", i));
        let old = chrono::Utc::now() - chrono::Duration::days(365);
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(report.entries_examined, 10);
    assert_eq!(report.entries_deleted, 10);
    assert_eq!(report.entries_retained, 0);
}

#[tokio::test]
async fn prune_ebbinghaus_multi_batch() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "multi-batch";

    // Insert 600 entries (will span 3 batches of 256)
    let old = chrono::Utc::now() - chrono::Duration::days(365);
    for i in 0..600 {
        let mut entry = entry(session, &format!("batch memory {}", i));
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(report.entries_examined, 600);
    assert_eq!(report.entries_deleted, 600);
    assert_eq!(report.entries_retained, 0);
}

#[tokio::test]
async fn prune_ebbinghaus_partial_batch_mixed() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "mixed-batch";

    let old = chrono::Utc::now() - chrono::Duration::days(365);
    let now = chrono::Utc::now();

    // 5 old (prune) + 5 recent (keep)
    for i in 0..5 {
        let mut entry = entry(session, &format!("old mixed {}", i));
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }
    for i in 0..5 {
        let mut entry = entry(session, &format!("recent mixed {}", i));
        entry.created_at = now;
        entry.last_accessed = now;
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage.prune_ebbinghaus(&policy, now).await.unwrap();
    assert_eq!(report.entries_examined, 10);
    assert_eq!(report.entries_deleted, 5);
    assert_eq!(report.entries_retained, 5);
}

#[tokio::test]
async fn prune_ebbinghaus_nothing_to_prune() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "nothing-to-prune";

    // All recent entries — none should be pruned
    for i in 0..10 {
        let entry = entry(session, &format!("recent {}", i));
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(report.entries_examined, 10);
    assert_eq!(report.entries_deleted, 0);
    assert_eq!(report.entries_retained, 10);
}

#[tokio::test]
async fn prune_ebbinghaus_all_qualify() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "all-qualify";

    let old = chrono::Utc::now() - chrono::Duration::days(365);
    for i in 0..20 {
        let mut entry = entry(session, &format!("forgotten {}", i));
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(report.entries_deleted, 20);
    assert_eq!(report.entries_retained, 0);
}

#[tokio::test]
async fn prune_ebbinghaus_delete_count_accurate() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "count-accurate";

    let old = chrono::Utc::now() - chrono::Duration::days(365);
    let now = chrono::Utc::now();

    // 15 old + 5 recent
    for i in 0..15 {
        let mut entry = entry(session, &format!("old count {}", i));
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }
    for i in 0..5 {
        let entry = entry(session, &format!("new count {}", i));
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    let report = storage.prune_ebbinghaus(&policy, now).await.unwrap();

    // Verify by counting remaining
    let q = Query {
        text: "count".into(),
        session_id: Some(session.into()),
        work_item_id: None,
        limit: 100,
        threshold: 0.0,
    };
    let remaining = storage.query(&q).await.unwrap();

    assert_eq!(report.entries_deleted, 15);
    assert_eq!(report.entries_retained, 5);
    assert_eq!(
        remaining.len(),
        5,
        "remaining entries must match retained count"
    );
}

#[tokio::test]
async fn prune_ebbinghaus_retry_after_completion() {
    let (storage, _path) = open_db("p0-test").await;
    let session = "retry-test";

    let old = chrono::Utc::now() - chrono::Duration::days(365);
    for i in 0..10 {
        let mut entry = entry(session, &format!("retry {}", i));
        entry.created_at = old;
        entry.last_accessed = old;
        storage.store(entry).await.unwrap();
    }

    let policy = EbbinghausPolicy::default();
    // First prune: deletes all
    let r1 = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(r1.entries_deleted, 10);

    // Second prune (retry): should be safe, nothing left
    let r2 = storage
        .prune_ebbinghaus(&policy, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(r2.entries_examined, 0);
    assert_eq!(r2.entries_deleted, 0);
}

#[tokio::test]
async fn query_scopes_to_work_item_across_sessions_without_cross_item_leakage() {
    let (storage, path) = open_db("work-item-isolation").await;
    let mut a1 = entry("session-a1", "shared retrieval topic for item alpha");
    a1.work_item_id = Some("WI-A".into());
    let mut a2 = entry(
        "session-a2",
        "shared retrieval topic for item alpha second session",
    );
    a2.work_item_id = Some("WI-A".into());
    let mut b1 = entry("session-b1", "shared retrieval topic for item beta");
    b1.work_item_id = Some("WI-B".into());
    storage.store(a1).await.unwrap();
    storage.store(a2).await.unwrap();
    storage.store(b1).await.unwrap();
    let results = storage
        .query(&Query {
            text: "shared retrieval topic".into(),
            session_id: None,
            work_item_id: Some("WI-A".into()),
            limit: 20,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|e| e.work_item_id.as_deref() == Some("WI-A")));
    assert_eq!(
        results
            .iter()
            .map(|e| e.session_id.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn opening_legacy_database_adds_nullable_work_item_scope_without_losing_rows() {
    let path = database_path("legacy-migration");
    let now = Utc::now();
    let legacy_id = Uuid::new_v4();
    {
        let pool = SqlitePoolOptions::new()
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
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO memories
             (id, session_id, content, embedding_json, metadata_json, created_at, access_count, last_accessed)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(legacy_id.to_string())
        .bind("legacy-session")
        .bind("legacy memory remains readable")
        .bind(Option::<String>::None)
        .bind("{}")
        .bind(now.to_rfc3339())
        .bind(0_i32)
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    let storage = SqliteStorage::open(&path).await.unwrap();
    let legacy = storage
        .query(&Query {
            text: "legacy memory remains readable".into(),
            session_id: Some("legacy-session".into()),
            work_item_id: None,
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(legacy.len(), 1);
    assert_eq!(legacy[0].id, legacy_id);
    assert_eq!(legacy[0].work_item_id, None);

    let scoped = storage
        .query(&Query {
            text: "legacy memory remains readable".into(),
            session_id: None,
            work_item_id: Some("WI-A".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert!(
        scoped.is_empty(),
        "legacy unscoped records must not enter a named Work Item"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn work_item_scope_persists_across_database_reopen() {
    let path = database_path("work-item-reopen");
    let mut record = entry("session-before-reopen", "persistent work item memory");
    record.work_item_id = Some("WI-PERSIST".into());
    let record_id = record.id;
    {
        let storage = SqliteStorage::open(&path).await.unwrap();
        storage.store(record).await.unwrap();
    }
    {
        let storage = SqliteStorage::open(&path).await.unwrap();
        let results = storage
            .query(&Query {
                text: "persistent work item".into(),
                session_id: None,
                work_item_id: Some("WI-PERSIST".into()),
                limit: 5,
                threshold: 1.0,
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, record_id);
        assert_eq!(results[0].work_item_id.as_deref(), Some("WI-PERSIST"));
        assert_eq!(results[0].session_id, "session-before-reopen");
    }
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn work_item_scope_is_applied_before_candidate_limit() {
    let (storage, path) = open_db("work-item-candidate-filter").await;
    let mut relevant = entry("session-a", "distinctive retrieval phrase");
    relevant.work_item_id = Some("WI-A".into());
    relevant.created_at = Utc::now() - ChronoDuration::days(2);
    relevant.last_accessed = relevant.created_at;
    storage.store(relevant).await.unwrap();

    for index in 0..120 {
        let mut unrelated = entry("session-b", &format!("unrelated recent record {index}"));
        unrelated.work_item_id = Some("WI-B".into());
        storage.store(unrelated).await.unwrap();
    }

    let results = storage
        .query(&Query {
            text: "distinctive retrieval phrase".into(),
            session_id: None,
            work_item_id: Some("WI-A".into()),
            limit: 1,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].work_item_id.as_deref(), Some("WI-A"));
    assert!(results[0].content.contains("distinctive retrieval phrase"));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn query_supports_chinese_and_mixed_language_content() {
    let (storage, path) = open_db("chinese-mixed-language").await;
    let mut chinese = entry(
        "session-zh",
        "跨会话检索需要 explicit work item retrieval scope",
    );
    chinese.work_item_id = Some("WI-LANG".into());
    storage.store(chinese).await.unwrap();

    let chinese_results = storage
        .query(&Query {
            text: "跨会话检索".into(),
            session_id: None,
            work_item_id: Some("WI-LANG".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(chinese_results.len(), 1);

    let mixed_results = storage
        .query(&Query {
            text: "跨会话 retrieval".into(),
            session_id: None,
            work_item_id: Some("WI-LANG".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(mixed_results.len(), 1);

    let single_character_results = storage
        .query(&Query {
            text: "会".into(),
            session_id: None,
            work_item_id: Some("WI-LANG".into()),
            limit: 5,
            threshold: 1.0,
        })
        .await
        .unwrap();
    assert_eq!(single_character_results.len(), 1);

    drop(storage);
    let _ = std::fs::remove_file(path);
}

// === Additional P0 Evidence Tests: Concurrency, Failure Injection, Scale ===
