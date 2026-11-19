// tests/chinese_retrieval.rs
// P1-02: Chinese retrieval compatibility tests
// Produced by ChatGPT execution AI (session 6aff37ee), coordinator applied verbatim.

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

// Threshold 0.34: "天气" produces 3 tokens {天气, 天, 气}.
// A single-character overlap scores 1/3 ~= 0.3333 and is rejected.
// Genuine phrase matches score above this threshold; therefore 0.34
// rejects incidental unigram matches while preserving partial CJK retrieval.
async fn search(storage: &SqliteStorage, text: &str) -> Vec<MemoryEntry> {
    storage
        .query(&Query {
            text: text.into(),
            session_id: None,
            limit: 20,
            threshold: 0.34,
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
async fn chinese_single_unigram_overlap_is_rejected() {
    let (storage, path) = setup("negative-cjk").await;

    // Both distractors share only the character 天 with query 天气.
    storage
        .store(entry("session-zh", "明天要下雨"))
        .await
        .unwrap();

    storage
        .store(entry("session-zh", "今天开会"))
        .await
        .unwrap();

    let results = search(&storage, "天气").await;

    assert!(
        results.is_empty(),
        "single-unigram overlap must not qualify as a phrase match"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn mixed_query_requires_chinese_concept_overlap() {
    let (storage, path) = setup("negative-mixed").await;

    // These records contain Rust but have no 所有权-related content.
    storage
        .store(entry(
            "session-mixed",
            "Rust is a fast programming language",
        ))
        .await
        .unwrap();

    storage
        .store(entry(
            "session-mixed",
            "Rust provides excellent performance",
        ))
        .await
        .unwrap();

    let results = search(&storage, "Rust 所有权").await;

    assert!(
        results.is_empty(),
        "matching Rust alone must not satisfy a mixed-language query requiring 所有权"
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
