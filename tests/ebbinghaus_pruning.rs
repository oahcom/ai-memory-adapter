use ai_memory_adapter::{
    adapter::{MemoryAdapter, MemoryEntry},
    pruning::EbbinghausPolicy,
    storage::SqliteStorage,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use std::{path::PathBuf, time::Duration as StdDuration};
use uuid::Uuid;

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!("ai-memory-ebbinghaus-{}.sqlite3", Uuid::new_v4()))
}

fn memory(
    content: &str,
    created_at: DateTime<Utc>,
    last_accessed: DateTime<Utc>,
    access_count: i32,
) -> MemoryEntry {
    MemoryEntry {
        id: Uuid::new_v4(),
        session_id: "pruning-test".into(),
        content: content.into(),
        embedding: None,
        metadata: json!({"source": "ebbinghaus-test"}),
        created_at,
        access_count,
        last_accessed,
    }
}

async fn setup() -> (SqliteStorage, PathBuf) {
    let path = database_path();
    let storage = SqliteStorage::open(&path).await.unwrap();
    (storage, path)
}

#[tokio::test]
async fn prune_ebbinghaus_removes_forgotten_and_preserves_relevant_memories() {
    let (storage, path) = setup().await;
    let now = Utc::now();

    let stale_time = now - Duration::days(120);
    let stale = memory("stale memory", stale_time, stale_time, 0);
    let stale_id = stale.id;

    let recent_time = now - Duration::days(5);
    let recent = memory("recent memory", recent_time, recent_time, 0);
    let recent_id = recent.id;

    let frequent_time = now - Duration::days(120);
    let frequent = memory(
        "frequently accessed memory",
        frequent_time,
        frequent_time,
        20,
    );
    let frequent_id = frequent.id;

    let future_created = now - Duration::days(1);
    let future_access = now + Duration::days(1);
    let future = memory("future access memory", future_created, future_access, 0);
    let future_id = future.id;

    for entry in [stale, recent, frequent, future] {
        storage.store(entry).await.unwrap();
    }

    let report = storage
        .prune_ebbinghaus(&EbbinghausPolicy::default(), now)
        .await
        .unwrap();

    assert_eq!(report.entries_examined, 4);
    assert_eq!(report.entries_deleted, 1);
    assert_eq!(report.entries_retained, 3);
    assert_eq!(report.prune_threshold, 0.1);
    assert_eq!(report.evaluated_at, now);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(count, 3);

    for id in [recent_id, frequent_id, future_id] {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(exists, 1);
    }

    let stale_exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories WHERE id = ?")
        .bind(stale_id.to_string())
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(stale_exists, 0);

    drop(storage);
    tokio::time::sleep(StdDuration::from_millis(10)).await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn prune_ebbinghaus_rejects_invalid_policy() {
    let (storage, path) = setup().await;
    let invalid = EbbinghausPolicy {
        half_life_days: 0.0,
        ..EbbinghausPolicy::default()
    };
    let result = storage.prune_ebbinghaus(&invalid, Utc::now()).await;
    assert!(result.is_err());
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn legacy_prune_remains_created_at_based() {
    let (storage, path) = setup().await;
    let now = Utc::now();

    let old_created = now - Duration::days(120);
    let recently_accessed = now - Duration::days(1);
    let old = memory(
        "old creation recent access",
        old_created,
        recently_accessed,
        5,
    );
    storage.store(old).await.unwrap();

    let deleted = storage.prune(now - Duration::days(30)).await.unwrap();
    assert_eq!(deleted, 1);

    drop(storage);
    let _ = std::fs::remove_file(path);
}
