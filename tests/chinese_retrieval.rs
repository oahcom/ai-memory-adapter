// tests/chinese_retrieval.rs
// P1-02: Chinese retrieval compatibility tests (produced by execution AI 6aff30bd)

use ai_memory_adapter::{
    adapter::{MemoryAdapter, MemoryEntry, Query},
    storage::SqliteStorage,
};
use chrono::Utc;
use serde_json::json;
use std::path::PathBuf;
use uuid::Uuid;

fn database_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ai-memory-chinese-retrieval-{name}-{}.sqlite3",
        Uuid::new_v4()
    ))
}

fn entry(session_id: &str, content: &str) -> MemoryEntry {
    let now = Utc::now();

    MemoryEntry {
        id: Uuid::new_v4(),
        session_id: session_id.into(),
        content: content.into(),
        embedding: None,
        metadata: json!({"source": "chinese-retrieval-test"}),
        created_at: now,
        access_count: 0,
        last_accessed: now,
    }
}

async fn setup(name: &str) -> (SqliteStorage, PathBuf) {
    let path = database_path(name);
    let storage = SqliteStorage::open(&path).await.unwrap();
    (storage, path)
}

async fn search(storage: &SqliteStorage, text: &str) -> Vec<MemoryEntry> {
    storage
        .query(&Query {
            text: text.into(),
            session_id: None,
            limit: 20,
            threshold: 0.01,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn chinese_partial_phrase_matches() {
    let (storage, path) = setup("partial-phrase").await;

    storage
        .store(entry("session-zh", "今天天气很好"))
        .await
        .unwrap();

    storage
        .store(entry("session-zh", "明天天气预报"))
        .await
        .unwrap();

    let results = search(&storage, "天气").await;

    assert_eq!(
        results.len(),
        2,
        "a short Chinese phrase must match longer CJK runs"
    );

    assert!(results.iter().any(|e| e.content == "今天天气很好"));
    assert!(results.iter().any(|e| e.content == "明天天气预报"));

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn chinese_multi_char_partial() {
    let (storage, path) = setup("multi-char-partial").await;

    storage
        .store(entry("session-zh", "机器学习是人工智能"))
        .await
        .unwrap();

    let results = search(&storage, "机器学").await;

    assert!(
        results
            .iter()
            .any(|e| e.content == "机器学习是人工智能"),
        "multi-character partial Chinese queries must match longer phrases"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn chinese_english_mixed() {
    let (storage, path) = setup("mixed-language").await;

    storage
        .store(entry(
            "session-mixed",
            "Rust 的所有权系统保证内存安全",
        ))
        .await
        .unwrap();

    let results = search(&storage, "Rust 所有权").await;

    assert!(
        results
            .iter()
            .any(|e| e.content == "Rust 的所有权系统保证内存安全"),
        "mixed Chinese and English queries must match"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn chinese_punctuation_variants() {
    let (storage, path) = setup("punctuation").await;

    let contents = [
        "设计完成，开发中",
        "设计完成, 开发中",
        "设计完成",
    ];

    for content in contents {
        storage
            .store(entry("session-punctuation", content))
            .await
            .unwrap();
    }

    for query in [
        "设计完成，开发中",
        "设计完成, 开发中",
        "设计完成",
    ] {
        let results = search(&storage, query).await;

        assert!(
            !results.is_empty(),
            "punctuation variant must match: {query}"
        );

        assert!(
            results.iter().any(|e| e.content == "设计完成，开发中"),
            "Chinese comma variant must match: {query}"
        );

        assert!(
            results.iter().any(|e| e.content == "设计完成, 开发中"),
            "ASCII comma variant must match: {query}"
        );

        assert!(
            results.iter().any(|e| e.content == "设计完成"),
            "short phrase variant must match: {query}"
        );
    }

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn english_baseline() {
    let (storage, path) = setup("english-baseline").await;

    storage
        .store(entry("session-en", "quick brown fox"))
        .await
        .unwrap();

    storage
        .store(entry("session-en", "THE FOX jumps over the fence"))
        .await
        .unwrap();

    storage
        .store(entry("session-en", "foxes are clever animals"))
        .await
        .unwrap();

    storage
        .store(entry("session-en", "quick brown dog"))
        .await
        .unwrap();

    let results = search(&storage, "fox").await;

    assert_eq!(
        results.len(),
        2,
        "fox must match exact tokens without matching foxes"
    );

    assert!(results.iter().any(|e| e.content == "quick brown fox"));

    assert!(
        results
            .iter()
            .any(|e| e.content == "THE FOX jumps over the fence")
    );

    assert!(
        !results.iter().any(|e| e.content.contains("foxes")),
        "English substring matches must not be introduced"
    );

    assert!(
        !results.iter().any(|e| e.content == "quick brown dog"),
        "unrelated English content must not match"
    );

    let uppercase_results = search(&storage, "FOX").await;

    assert_eq!(uppercase_results.len(), 2);

    assert!(
        uppercase_results
            .iter()
            .any(|e| e.content == "quick brown fox")
    );

    assert!(
        uppercase_results
            .iter()
            .any(|e| e.content == "THE FOX jumps over the fence")
    );

    let unrelated_results = search(&storage, "elephant").await;

    assert!(
        unrelated_results.is_empty(),
        "unrelated English queries must not match"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}
