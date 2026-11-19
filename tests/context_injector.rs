use ai_memory_adapter::{
    adapter::{AdapterError, MemoryAdapter, MemoryEntry},
    context::{ContextConfig, ContextInjector},
    storage::SqliteStorage,
};
use chrono::Utc;
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use uuid::Uuid;

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ai-memory-context-injector-{}.sqlite3",
        Uuid::new_v4()
    ))
}

fn memory(session_id: &str, content: &str) -> MemoryEntry {
    let now = Utc::now();
    MemoryEntry {
        id: Uuid::new_v4(),
        session_id: session_id.into(),
        work_item_id: None,
        content: content.into(),
        embedding: None,
        metadata: json!({"source": "context-injector-test"}),
        created_at: now,
        access_count: 0,
        last_accessed: now,
    }
}

async fn setup() -> (SqliteStorage, PathBuf) {
    let path = database_path();
    let storage = SqliteStorage::open(&path).await.unwrap();
    (storage, path)
}

fn test_config(max_entries: usize, max_chars: usize) -> ContextConfig {
    ContextConfig {
        max_entries,
        min_score: 0.5,
        max_chars,
    }
}

#[tokio::test]
async fn inject_returns_relevant_memories_for_session() {
    let (storage, path) = setup().await;
    storage
        .store(memory("session-a", "Rust memory architecture"))
        .await
        .unwrap();
    storage
        .store(memory("session-a", "Rust SQLite persistence"))
        .await
        .unwrap();
    storage
        .store(memory("session-a", "Python web application"))
        .await
        .unwrap();

    let injector = ContextInjector::new(&storage, test_config(10, 8_000)).unwrap();
    let result = injector.inject("session-a", "Rust memory").await.unwrap();

    assert_eq!(result.entries_included, 2);
    assert_eq!(result.entries_omitted, 0);
    assert!(result.text.contains("Rust memory architecture"));
    assert!(result.text.contains("Rust SQLite persistence"));
    assert!(!result.text.contains("Python web application"));
    assert_eq!(result.chars_used, result.text.chars().count());

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_isolates_sessions() {
    let (storage, path) = setup().await;
    storage
        .store(memory("session-a", "Rust private session A"))
        .await
        .unwrap();
    storage
        .store(memory("session-b", "Rust private session B"))
        .await
        .unwrap();

    let injector = ContextInjector::new(&storage, test_config(10, 8_000)).unwrap();
    let result = injector.inject("session-a", "Rust").await.unwrap();

    assert!(result.text.contains("session A"));
    assert!(!result.text.contains("session B"));

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_respects_max_entries() {
    let (storage, path) = setup().await;
    for index in 0..5 {
        storage
            .store(memory("session-a", &format!("Rust memory record {index}")))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let injector = ContextInjector::new(&storage, test_config(2, 8_000)).unwrap();
    let result = injector.inject("session-a", "Rust memory").await.unwrap();

    assert_eq!(result.entries_included, 2);
    assert_eq!(result.entries_omitted, 0);

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_respects_character_budget_without_partial_records() {
    let (storage, path) = setup().await;
    storage
        .store(memory("session-a", "Rust memory content"))
        .await
        .unwrap();

    let injector = ContextInjector::new(&storage, test_config(10, 8)).unwrap();
    let result = injector.inject("session-a", "Rust").await.unwrap();

    assert_eq!(result.entries_included, 0);
    assert_eq!(result.entries_omitted, 1);
    assert!(result.text.is_empty());
    assert_eq!(result.chars_used, 0);

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_reports_actual_unicode_character_count() {
    let (storage, path) = setup().await;
    storage
        .store(memory("session-a", "Rust 记忆系统"))
        .await
        .unwrap();

    let injector = ContextInjector::new(&storage, test_config(10, 8_000)).unwrap();
    let result = injector.inject("session-a", "Rust").await.unwrap();

    assert_eq!(result.chars_used, result.text.chars().count());
    assert!(result.chars_used <= 8_000);

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_rejects_empty_session_and_query() {
    let (storage, path) = setup().await;
    let injector = ContextInjector::new(&storage, ContextConfig::default()).unwrap();

    let empty_session = injector.inject(" ", "Rust").await;
    assert!(matches!(empty_session, Err(AdapterError::Validation(_))));

    let empty_query = injector.inject("session-a", " ").await;
    assert!(matches!(empty_query, Err(AdapterError::Validation(_))));

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn config_rejects_invalid_values() {
    let (storage, path) = setup().await;

    assert!(ContextInjector::new(&storage, test_config(0, 100)).is_err());
    assert!(ContextInjector::new(&storage, test_config(10, 0)).is_err());

    let invalid_score = ContextConfig {
        max_entries: 10,
        min_score: f32::NAN,
        max_chars: 100,
    };
    assert!(ContextInjector::new(&storage, invalid_score).is_err());

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inject_work_item_retrieves_across_sessions_but_excludes_other_items() {
    let (storage, path) = setup().await;
    let mut a1 = memory("session-a1", "Rust work item alpha first session");
    a1.work_item_id = Some("WI-A".into());
    let mut a2 = memory("session-a2", "Rust work item alpha second session");
    a2.work_item_id = Some("WI-A".into());
    let mut b1 = memory("session-b1", "Rust work item beta private content");
    b1.work_item_id = Some("WI-B".into());
    storage.store(a1).await.unwrap();
    storage.store(a2).await.unwrap();
    storage.store(b1).await.unwrap();
    let injector = ContextInjector::new(&storage, test_config(10, 8_000)).unwrap();
    let result = injector
        .inject_work_item("WI-A", "Rust work item")
        .await
        .unwrap();
    assert_eq!(result.entries_included, 2);
    assert!(result.text.contains("alpha first session"));
    assert!(result.text.contains("alpha second session"));
    assert!(!result.text.contains("beta private content"));
    drop(storage);
    let _ = std::fs::remove_file(path);
}