// tests/retrieval_bias.rs
//
// Measurement-only harness for recency-biased SQLite candidate selection.
// No production source changes or additional dependencies.
//
// Run:
//   cargo test --test retrieval_bias -- --nocapture
//
// Candidate-stage measurement:
// Integration tests compile the library without cfg(test), so the
// crate::test_support::LAST_QUERY_CANDIDATE_COUNT symbol is unavailable.
// We independently execute the exact candidate SELECT used by query(),
// including its ORDER BY and LIMIT, immediately before calling query().
// We then compare those candidate IDs against explicit ground-truth IDs.
// After each query, timestamps are restored so query()'s access updates
// cannot contaminate subsequent measurements.

use std::{
    collections::HashSet,
    env,
    time::{Duration, Instant},
};

use ai_memory_adapter::{
    adapter::{MemoryAdapter, MemoryEntry, Query},
    storage::SqliteStorage,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

const QUERY_LIMIT: usize = 10;
const CANDIDATE_LIMIT: usize = 100;
const THRESHOLD: f32 = 0.8;

const QUERY_COUNT: usize = 4;
const OLD_RELEVANT_PER_QUERY: usize = 5;
const RECENT_RELEVANT_PER_QUERY: usize = 2;
const RECENT_DISTRACTORS_PER_QUERY: usize = 105;
const OLD_IRRELEVANT_PER_QUERY: usize = 10;

const QUERIES: [(&str, &str); QUERY_COUNT] = [
    ("aurora telemetry", "aurora"),
    ("quantum storage", "quantum"),
    ("nebula routing", "nebula"),
    ("crystal indexing", "crystal"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    OlderRelevant,
    RecentRelevant,
    RecentIrrelevant,
    OlderIrrelevant,
}

impl Category {
    fn label(self) -> &'static str {
        match self {
            Self::OlderRelevant => "older_relevant",
            Self::RecentRelevant => "recent_relevant",
            Self::RecentIrrelevant => "recent_irrelevant",
            Self::OlderIrrelevant => "older_irrelevant",
        }
    }
}

#[derive(Debug)]
struct GroundTruth {
    query_text: &'static str,
    topic: &'static str,
    expected_relevant: HashSet<Uuid>,
    older_relevant: HashSet<Uuid>,
    recent_relevant: HashSet<Uuid>,
}

#[derive(Debug)]
struct Measurement {
    query_text: String,
    relevant_total: usize,
    candidate_relevant: usize,
    candidate_older_relevant: usize,
    older_relevant_total: usize,
    result_ids: Vec<Uuid>,
    elapsed: Duration,
}

#[derive(Debug)]
struct Dataset {
    entries: Vec<MemoryEntry>,
    ground_truth: Vec<GroundTruth>,
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("valid deterministic timestamp")
        .with_timezone(&Utc)
}

fn deterministic_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn make_entry(
    id: Uuid,
    content: String,
    category: Category,
    timestamp: DateTime<Utc>,
) -> MemoryEntry {
    MemoryEntry {
        id,
        session_id: "retrieval-bias-benchmark".to_string(),
        content,
        embedding: None,
        metadata: json!({
            "benchmark": "retrieval_bias",
            "category": category.label(),
            "ground_truth": matches!(
                category,
                Category::OlderRelevant | Category::RecentRelevant
            ),
        }),
        created_at: timestamp,
        access_count: 0,
        last_accessed: timestamp,
    }
}

fn build_dataset() -> Dataset {
    let old_relevant_time = timestamp("2020-01-01T00:00:00Z");
    let old_irrelevant_time = timestamp("2021-01-01T00:00:00Z");
    let distractor_base = timestamp("2024-12-01T00:00:00Z");
    let recent_relevant_time = timestamp("2025-01-01T00:00:00Z");

    let mut entries = Vec::new();
    let mut ground_truth = Vec::new();

    // UUID namespaces are deterministic and disjoint by category.
    let mut old_relevant_counter = 1u128;
    let mut recent_relevant_counter = 10_000u128;
    let mut distractor_counter = 20_000u128;
    let mut old_irrelevant_counter = 30_000u128;

    for (query_index, (query_text, topic)) in QUERIES.iter().enumerate() {
        let mut expected_relevant = HashSet::new();
        let mut older_relevant = HashSet::new();
        let mut recent_relevant = HashSet::new();

        // Category A: older relevant memories.
        // These have exact query terms but substantially older access times.
        for index in 0..OLD_RELEVANT_PER_QUERY {
            let id = deterministic_id(old_relevant_counter);
            old_relevant_counter += 1;

            entries.push(make_entry(
                id,
                format!(
                    "Historical {topic} telemetry architecture report record {index}"
                ),
                Category::OlderRelevant,
                old_relevant_time + ChronoDuration::seconds(index as i64),
            ));

            expected_relevant.insert(id);
            older_relevant.insert(id);
        }

        // Category B: recent relevant memories.
        // Their timestamps put them ahead of all distractors.
        for index in 0..RECENT_RELEVANT_PER_QUERY {
            let id = deterministic_id(recent_relevant_counter);
            recent_relevant_counter += 1;

            entries.push(make_entry(
                id,
                format!(
                    "Current {topic} telemetry architecture analysis record {index}"
                ),
                Category::RecentRelevant,
                recent_relevant_time + ChronoDuration::seconds(index as i64),
            ));

            expected_relevant.insert(id);
            recent_relevant.insert(id);
        }

        // Category C: recent irrelevant distractors.
        // They intentionally do not contain any query's distinctive topic.
        // There are more than 100 newer distractors per query.
        for index in 0..RECENT_DISTRACTORS_PER_QUERY {
            let id = deterministic_id(distractor_counter);
            distractor_counter += 1;

            let accessed = distractor_base
                + ChronoDuration::seconds(
                    (query_index * RECENT_DISTRACTORS_PER_QUERY + index) as i64,
                );

            entries.push(make_entry(
                id,
                format!(
                    "Recent unrelated operational maintenance report \
                     generic system status document {query_index}-{index}"
                ),
                Category::RecentIrrelevant,
                accessed,
            ));
        }

        // Category D: older irrelevant memories.
        for index in 0..OLD_IRRELEVANT_PER_QUERY {
            let id = deterministic_id(old_irrelevant_counter);
            old_irrelevant_counter += 1;

            entries.push(make_entry(
                id,
                format!(
                    "Archived unrelated historical maintenance document \
                     obsolete system notes {query_index}-{index}"
                ),
                Category::OlderIrrelevant,
                old_irrelevant_time + ChronoDuration::seconds(index as i64),
            ));
        }

        ground_truth.push(GroundTruth {
            query_text,
            topic,
            expected_relevant,
            older_relevant,
            recent_relevant,
        });
    }

    Dataset {
        entries,
        ground_truth,
    }
}

// Reproduce query()'s candidate-stage SELECT exactly.
// This must execute before query(), because query() updates last_accessed
// for returned entries.
async fn fetch_candidate_ids(
    storage: &SqliteStorage,
    candidate_limit: usize,
) -> Vec<Uuid> {
    let rows = sqlx::query(
        "SELECT id
         FROM memories
         ORDER BY last_accessed DESC, id DESC
         LIMIT ?",
    )
    .bind(candidate_limit as i64)
    .fetch_all(storage.pool())
    .await
    .expect("candidate-stage SQL must succeed");

    rows.iter()
        .map(|row| {
            let id: String = row.get("id");
            Uuid::parse_str(&id).expect("valid stored UUID")
        })
        .collect()
}

// Restore the original deterministic access timestamps after query()
// has performed its normal access-accounting updates.
async fn restore_timestamps(
    storage: &SqliteStorage,
    entries: &[MemoryEntry],
) {
    let mut tx = storage.pool().begin().await.unwrap();

    for entry in entries {
        sqlx::query(
            "UPDATE memories
             SET last_accessed = ?
             WHERE id = ?",
        )
        .bind(
            entry
                .last_accessed
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .bind(entry.id.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    }

    tx.commit().await.unwrap();
}

fn recall_at_k(
    results: &[Uuid],
    expected: &HashSet<Uuid>,
    k: usize,
) -> f64 {
    if expected.is_empty() {
        return 0.0;
    }

    let hits = results
        .iter()
        .take(k)
        .filter(|id| expected.contains(id))
        .count();

    hits as f64 / expected.len() as f64
}

fn percentile(values: &[u128], percentile: f64) -> u128 {
    assert!(!values.is_empty());

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let rank = ((percentile * sorted.len() as f64).ceil() as usize)
        .max(1)
        .min(sorted.len());

    sorted[rank - 1]
}

fn percentage(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        return 0.0;
    }

    numerator as f64 * 100.0 / denominator as f64
}

fn category_count(entries: &[MemoryEntry], category: Category) -> usize {
    entries
        .iter()
        .filter(|entry| {
            entry.metadata["category"]
                .as_str()
                .is_some_and(|value| value == category.label())
        })
        .count()
}

#[tokio::test]
async fn measure_recency_candidate_selection_bias() {
    let dataset = build_dataset();

    let expected_corpus_size = QUERY_COUNT
        * (OLD_RELEVANT_PER_QUERY
            + RECENT_RELEVANT_PER_QUERY
            + RECENT_DISTRACTORS_PER_QUERY
            + OLD_IRRELEVANT_PER_QUERY);

    assert_eq!(dataset.entries.len(), expected_corpus_size);

    assert_eq!(
        CANDIDATE_LIMIT,
        QUERY_LIMIT.saturating_mul(10).clamp(100, 1000)
    );

    let database_path = env::temp_dir().join(format!(
        "ai-memory-adapter-retrieval-bias-{}.sqlite",
        std::process::id()
    ));

    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));

    let storage = SqliteStorage::open(&database_path)
        .await
        .expect("open benchmark database");

    // Insert the entire deterministic corpus.
    for entry in &dataset.entries {
        storage
            .store(entry.clone())
            .await
            .expect("store benchmark entry");
    }

    // Verify all four dataset categories are represented.
    assert_eq!(
        category_count(&dataset.entries, Category::OlderRelevant),
        QUERY_COUNT * OLD_RELEVANT_PER_QUERY
    );
    assert_eq!(
        category_count(&dataset.entries, Category::RecentRelevant),
        QUERY_COUNT * RECENT_RELEVANT_PER_QUERY
    );
    assert_eq!(
        category_count(&dataset.entries, Category::RecentIrrelevant),
        QUERY_COUNT * RECENT_DISTRACTORS_PER_QUERY
    );
    assert_eq!(
        category_count(&dataset.entries, Category::OlderIrrelevant),
        QUERY_COUNT * OLD_IRRELEVANT_PER_QUERY
    );

    let mut measurements = Vec::new();

    for truth in &dataset.ground_truth {
        let query = Query {
            text: truth.query_text.to_string(),
            session_id: Some("retrieval-bias-benchmark".to_string()),
            limit: QUERY_LIMIT,
            threshold: THRESHOLD,
        };

        // Stage 1: Measure the actual candidate set before scoring.
        let candidate_ids =
            fetch_candidate_ids(&storage, CANDIDATE_LIMIT).await;

        let candidate_set: HashSet<Uuid> =
            candidate_ids.iter().copied().collect();

        assert_eq!(
            candidate_ids.len(),
            CANDIDATE_LIMIT,
            "candidate-stage row count for {}",
            truth.query_text
        );

        let candidate_relevant = truth
            .expected_relevant
            .intersection(&candidate_set)
            .count();

        let candidate_older_relevant = truth
            .older_relevant
            .intersection(&candidate_set)
            .count();

        // Stage 2: Measure actual end-to-end query execution.
        let start = Instant::now();

        let results = storage
            .query(&query)
            .await
            .expect("query execution must succeed");

        let elapsed = start.elapsed();

        assert!(
            results.len() <= QUERY_LIMIT,
            "query returned more than its requested limit"
        );

        let result_ids: Vec<Uuid> =
            results.iter().map(|entry| entry.id).collect();

        // Validate that all results were members of the candidate set.
        for id in &result_ids {
            assert!(
                candidate_set.contains(id),
                "result {} was not in the measured candidate set",
                id
            );
        }

        measurements.push(Measurement {
            query_text: truth.query_text.to_string(),
            relevant_total: truth.expected_relevant.len(),
            candidate_relevant,
            candidate_older_relevant,
            older_relevant_total: truth.older_relevant.len(),
            result_ids,
            elapsed,
        });

        // query() updates last_accessed for returned records.
        // Restore fixture timestamps to ensure independent, reproducible
        // candidate-stage measurements for every subsequent query.
        restore_timestamps(&storage, &dataset.entries).await;
    }

    // Aggregate candidate-stage recall.
    let total_relevant: usize = measurements
        .iter()
        .map(|m| m.relevant_total)
        .sum();

    let total_candidate_relevant: usize = measurements
        .iter()
        .map(|m| m.candidate_relevant)
        .sum();

    let total_older_relevant: usize = measurements
        .iter()
        .map(|m| m.older_relevant_total)
        .sum();

    let total_candidate_older_relevant: usize = measurements
        .iter()
        .map(|m| m.candidate_older_relevant)
        .sum();

    let candidate_recall = percentage(
        total_candidate_relevant,
        total_relevant,
    );

    let older_candidate_recall = percentage(
        total_candidate_older_relevant,
        total_older_relevant,
    );

    // End-to-end Recall@K.
    let recall_ks = [1usize, 3, 5, 10];

    let mut recall_totals = [0usize; 4];

    for measurement in &measurements {
        let truth = dataset
            .ground_truth
            .iter()
            .find(|truth| truth.query_text == measurement.query_text)
            .unwrap();

        for (index, k) in recall_ks.iter().enumerate() {
            recall_totals[index] += measurement
                .result_ids
                .iter()
                .take(*k)
                .filter(|id| truth.expected_relevant.contains(id))
                .count();
        }
    }

    let elapsed_micros: Vec<u128> = measurements
        .iter()
        .map(|m| m.elapsed.as_micros())
        .collect();

    let p50 = percentile(&elapsed_micros, 0.50);
    let p95 = percentile(&elapsed_micros, 0.95);

    // Per-query report.
    println!();
    println!("========== RETRIEVAL BIAS MEASUREMENT ==========");
    println!("Repository baseline: a5b9039");
    println!("Corpus size: {}", dataset.entries.len());
    println!("Query count: {}", measurements.len());
    println!("Candidate limit: {}", CANDIDATE_LIMIT);
    println!("Query limit: {}", QUERY_LIMIT);
    println!("Threshold: {:.2}", THRESHOLD);
    println!("Session filter: retrieval-bias-benchmark");
    println!();

    println!("CATEGORY COUNTS");
    println!(
        "  Older relevant:   {}",
        category_count(&dataset.entries, Category::OlderRelevant)
    );
    println!(
        "  Recent relevant:  {}",
        category_count(&dataset.entries, Category::RecentRelevant)
    );
    println!(
        "  Recent irrelevant:{}",
        category_count(&dataset.entries, Category::RecentIrrelevant)
    );
    println!(
        "  Older irrelevant: {}",
        category_count(&dataset.entries, Category::OlderIrrelevant)
    );

    println!();
    println!("PER-QUERY RESULTS");

    for measurement in &measurements {
        let truth = dataset
            .ground_truth
            .iter()
            .find(|truth| truth.query_text == measurement.query_text)
            .unwrap();

        let elapsed_us = measurement.elapsed.as_micros();

        println!("------------------------------------------------");
        println!("Query: {}", measurement.query_text);
        println!("Topic: {}", truth.topic);
        println!(
            "Ground-truth relevant: {}",
            measurement.relevant_total
        );
        println!(
            "Candidate-stage relevant: {}/{} ({:.2}%)",
            measurement.candidate_relevant,
            measurement.relevant_total,
            percentage(
                measurement.candidate_relevant,
                measurement.relevant_total
            )
        );
        println!(
            "Older relevant in candidates: {}/{} ({:.2}%)",
            measurement.candidate_older_relevant,
            measurement.older_relevant_total,
            percentage(
                measurement.candidate_older_relevant,
                measurement.older_relevant_total
            )
        );

        for (index, k) in recall_ks.iter().enumerate() {
            let hits = measurement
                .result_ids
                .iter()
                .take(*k)
                .filter(|id| truth.expected_relevant.contains(id))
                .count();

            println!(
                "Recall@{}: {:.4} ({}/{})",
                k,
                hits as f64 / measurement.relevant_total as f64,
                hits,
                measurement.relevant_total
            );

            let _ = index;
        }

        println!("Elapsed: {} us", elapsed_us);
        println!("Result IDs: {:?}", measurement.result_ids);
    }

    println!();
    println!("AGGREGATE RESULTS");
    println!(
        "Candidate-stage recall: {}/{} ({:.2}%)",
        total_candidate_relevant,
        total_relevant,
        candidate_recall
    );
    println!(
        "Older-relevant candidate recall: {}/{} ({:.2}%)",
        total_candidate_older_relevant,
        total_older_relevant,
        older_candidate_recall
    );

    for (index, k) in recall_ks.iter().enumerate() {
        println!(
            "Recall@{}: {:.4} ({}/{})",
            k,
            recall_totals[index] as f64
                / (total_relevant * measurements.len() as f64),
            recall_totals[index],
            total_relevant * measurements.len()
        );
    }

    println!();
    println!("LATENCY");
    println!("p50: {} us", p50);
    println!("p95: {} us", p95);

    println!();
    println!("ENVIRONMENT");
    println!("OS: {}", env::consts::OS);
    println!("Architecture: {}", env::consts::ARCH);
    println!("Rust: {}", env!("CARGO_PKG_RUST_VERSION"));
    println!("SQLite: sqlx SQLite backend");
    println!("Runtime: Tokio");
    println!("Dependencies added: none");
    println!("Timestamps: fixed RFC3339 fixture values");
    println!("IDs: deterministic UUIDs");
    println!("Candidate observation: replicated SQL SELECT");
    println!("Scoring: production SqliteStorage::query()");
    println!("Measurement: query execution elapsed time");
    println!("Percentiles: nearest-rank");

    println!();
    println!("DECISION CRITERION");
    println!(
        "Older-relevant candidate recall below 95% is considered \
         materially degraded."
    );

    if older_candidate_recall < 95.0 {
        println!("DECISION: CHANGE_REQUIRED");
        println!(
            "Measured older-relevant candidate recall: {:.2}%",
            older_candidate_recall
        );
        println!(
            "Older relevant entries excluded before scoring: {}/{}",
            total_older_relevant - total_candidate_older_relevant,
            total_older_relevant
        );
        println!(
            "Overall candidate-stage recall: {:.2}%",
            candidate_recall
        );
        println!(
            "End-to-end Recall@10: {:.4}",
            recall_totals[3] as f64
                / (total_relevant * measurements.len() as f64)
        );
    } else {
        println!("DECISION: NO_CHANGE");
        println!(
            "Older-relevant candidate recall: {:.2}%",
            older_candidate_recall
        );
    }

    println!("================================================");
    println!();

    // Structural assertions ensure the benchmark actually exercises the
    // intended failure mode rather than silently measuring an undersized set.
    assert_eq!(total_relevant, QUERY_COUNT * (
        OLD_RELEVANT_PER_QUERY + RECENT_RELEVANT_PER_QUERY
    ));

    assert_eq!(
        total_older_relevant,
        QUERY_COUNT * OLD_RELEVANT_PER_QUERY
    );

    assert!(
        total_candidate_older_relevant < total_older_relevant,
        "fixture failed to exclude older relevant entries from candidates"
    );

    assert!(
        older_candidate_recall < 95.0,
        "expected materially degraded older-relevant candidate recall"
    );

    assert!(
        recall_totals[3] < total_relevant * measurements.len(),
        "expected end-to-end retrieval to miss some relevant entries"
    );

    drop(storage);

    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));
}
