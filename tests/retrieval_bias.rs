#![cfg(feature = "test-support")]

use std::{collections::HashSet, env, time::Instant};

use ai_memory_adapter::{
    adapter::{MemoryAdapter, MemoryEntry, Query},
    storage::SqliteStorage,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::json;
use uuid::Uuid;

const SESSION_ID: &str = "retrieval-bias-benchmark";

const QUERY_LIMIT: usize = 10;
const CANDIDATE_LIMIT: usize = 100;
const THRESHOLD: f32 = 0.8;
const PRODUCTION_DEFAULT_THRESHOLD: f32 = 0.25;
const HARNESS_PRIOR: f32 = 0.8;

fn latency_measurement_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

const QUERY_COUNT: usize = 4;

const OLD_RELEVANT_PER_QUERY: usize = 5;
const RECENT_RELEVANT_PER_QUERY: usize = 2;
const RECENT_DISTRACTORS_PER_QUERY: usize = 105;
const OLD_IRRELEVANT_PER_QUERY: usize = 10;

const WARMUP_ITERATIONS: usize = 3;
const MEASURED_ITERATIONS: usize = 30;

const OLDER_CANDIDATE_RECALL_THRESHOLD_PERCENT: f64 = 95.0;

const QUERIES: [(&str, &str); QUERY_COUNT] = [
    ("aurora telemetry", "aurora"),
    ("quantum storage", "quantum"),
    ("nebula routing", "nebula"),
    ("crystal indexing", "crystal"),
];

const PHASE_A_OLDER_RELEVANT_TOTAL: usize = QUERY_COUNT * OLD_RELEVANT_PER_QUERY;

const PHASE_A_RELEVANT_TOTAL: usize =
    QUERY_COUNT * (OLD_RELEVANT_PER_QUERY + RECENT_RELEVANT_PER_QUERY);

#[derive(Clone, Copy)]
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

struct GroundTruth {
    query_text: &'static str,
    topic: &'static str,
    expected_relevant: HashSet<Uuid>,
    older_relevant: HashSet<Uuid>,
}

struct Measurement {
    query_text: String,
    relevant_total: usize,
    candidate_relevant: usize,
    candidate_older_relevant: usize,
    older_relevant_total: usize,
    result_ids: Vec<Uuid>,
}

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
        session_id: SESSION_ID.to_string(),
        work_item_id: Some("wi-scope".into()),
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

    let mut old_relevant_counter = 1u128;
    let mut recent_relevant_counter = 10_000u128;
    let mut distractor_counter = 20_000u128;
    let mut old_irrelevant_counter = 30_000u128;

    for (query_index, (query_text, topic)) in QUERIES.iter().enumerate() {
        let mut expected_relevant = HashSet::new();
        let mut older_relevant = HashSet::new();

        for index in 0..OLD_RELEVANT_PER_QUERY {
            let id = deterministic_id(old_relevant_counter);
            old_relevant_counter += 1;

            entries.push(make_entry(
                id,
                format!("Historical {topic} telemetry architecture report record {index}"),
                Category::OlderRelevant,
                old_relevant_time + ChronoDuration::seconds(index as i64),
            ));

            expected_relevant.insert(id);
            older_relevant.insert(id);
        }

        for index in 0..RECENT_RELEVANT_PER_QUERY {
            let id = deterministic_id(recent_relevant_counter);
            recent_relevant_counter += 1;

            entries.push(make_entry(
                id,
                format!("Current {topic} telemetry architecture analysis record {index}"),
                Category::RecentRelevant,
                recent_relevant_time + ChronoDuration::seconds(index as i64),
            ));

            expected_relevant.insert(id);
        }

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
        });
    }

    Dataset {
        entries,
        ground_truth,
    }
}

async fn restore_timestamps(storage: &SqliteStorage, entries: &[MemoryEntry]) {
    let mut tx = storage.pool().begin().await.unwrap();

    for entry in entries {
        sqlx::query(
            "UPDATE memories
             SET last_accessed = ?, access_count = ?
             WHERE id = ?",
        )
        .bind(
            entry
                .last_accessed
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .bind(entry.access_count)
        .bind(entry.id.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    }

    tx.commit().await.unwrap();
}

fn percentile(values: &[u128], percentile: f64) -> u128 {
    assert!(!values.is_empty());
    assert!((0.0..=1.0).contains(&percentile));

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

// -----------------------------------------------------------------------------
// Original P1-03 measurement. Retained independently of Phase A.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn measure_recency_candidate_selection_bias() {
    let dataset = build_dataset();

    let expected_corpus_size = QUERY_COUNT
        * (OLD_RELEVANT_PER_QUERY
            + RECENT_RELEVANT_PER_QUERY
            + RECENT_DISTRACTORS_PER_QUERY
            + OLD_IRRELEVANT_PER_QUERY);

    assert_eq!(expected_corpus_size, 488);
    assert_eq!(dataset.entries.len(), expected_corpus_size);

    assert_eq!(
        CANDIDATE_LIMIT,
        QUERY_LIMIT.saturating_mul(10).clamp(100, 1000)
    );

    let _measurement_guard = latency_measurement_lock().lock().await;

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

    for entry in &dataset.entries {
        storage
            .store(entry.clone())
            .await
            .expect("store benchmark entry");
    }

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
    let mut latency_samples_micros: Vec<u128> = Vec::new();

    for truth in &dataset.ground_truth {
        let query = Query {
            text: truth.query_text.to_string(),
            session_id: Some(SESSION_ID.to_string()),
            work_item_id: Some("wi-scope".into()),
            limit: QUERY_LIMIT,
            threshold: THRESHOLD,
        };

        let results = storage
            .query(&query)
            .await
            .expect("query execution must succeed");

        let candidate_ids = ai_memory_adapter::test_support_api::last_query_candidate_ids();

        let candidate_set: HashSet<Uuid> = candidate_ids.iter().copied().collect();

        assert!(
            candidate_ids.len() <= CANDIDATE_LIMIT,
            "candidate-stage row count exceeded candidate_limit for {}",
            truth.query_text
        );

        let candidate_relevant = truth.expected_relevant.intersection(&candidate_set).count();

        let candidate_older_relevant = truth.older_relevant.intersection(&candidate_set).count();

        assert!(
            results.len() <= query.limit,
            "query returned more than its requested limit"
        );

        let result_ids: Vec<Uuid> = results.iter().map(|entry| entry.id).collect();

        for id in &result_ids {
            assert!(
                candidate_set.contains(id),
                "result {} was not in the measured candidate set",
                id
            );
        }

        restore_timestamps(&storage, &dataset.entries).await;

        measurements.push(Measurement {
            query_text: truth.query_text.to_string(),
            relevant_total: truth.expected_relevant.len(),
            candidate_relevant,
            candidate_older_relevant,
            older_relevant_total: truth.older_relevant.len(),
            result_ids,
        });

        for _ in 0..WARMUP_ITERATIONS {
            let results = storage
                .query(&query)
                .await
                .expect("warm-up query execution must succeed");

            assert!(
                results.len() <= query.limit,
                "warm-up query returned more than its requested limit"
            );

            restore_timestamps(&storage, &dataset.entries).await;
        }

        for _ in 0..MEASURED_ITERATIONS {
            let start = Instant::now();

            let results = storage
                .query(&query)
                .await
                .expect("measured query execution must succeed");

            let elapsed = start.elapsed();

            assert!(
                results.len() <= query.limit,
                "measured query returned more than its requested limit"
            );

            latency_samples_micros.push(elapsed.as_micros());

            restore_timestamps(&storage, &dataset.entries).await;
        }
    }

    let total_relevant: usize = measurements
        .iter()
        .map(|measurement| measurement.relevant_total)
        .sum();

    let total_candidate_relevant: usize = measurements
        .iter()
        .map(|measurement| measurement.candidate_relevant)
        .sum();

    let total_older_relevant: usize = measurements
        .iter()
        .map(|measurement| measurement.older_relevant_total)
        .sum();

    let total_candidate_older_relevant: usize = measurements
        .iter()
        .map(|measurement| measurement.candidate_older_relevant)
        .sum();

    let candidate_recall = percentage(total_candidate_relevant, total_relevant);

    let older_candidate_recall = percentage(total_candidate_older_relevant, total_older_relevant);

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

    let expected_latency_samples = QUERY_COUNT * MEASURED_ITERATIONS;

    assert_eq!(latency_samples_micros.len(), expected_latency_samples);

    let p50 = percentile(&latency_samples_micros, 0.50);
    let p95 = percentile(&latency_samples_micros, 0.95);

    println!();
    println!("========== RETRIEVAL BIAS MEASUREMENT ==========");
    println!("Repository baseline: a5b9039");
    println!("Corpus size: {}", dataset.entries.len());
    println!("Query count: {}", measurements.len());
    println!("Candidate limit: {}", CANDIDATE_LIMIT);
    println!("Query limit: {}", QUERY_LIMIT);
    println!("Threshold: {:.2}", THRESHOLD);
    println!("Session filter: {}", SESSION_ID);
    println!("Warm-up iterations per query: {}", WARMUP_ITERATIONS);
    println!("Measured iterations per query: {}", MEASURED_ITERATIONS);
    println!();

    println!("CATEGORY COUNTS");
    println!(
        "  Older relevant:    {}",
        category_count(&dataset.entries, Category::OlderRelevant)
    );
    println!(
        "  Recent relevant:   {}",
        category_count(&dataset.entries, Category::RecentRelevant)
    );
    println!(
        "  Recent irrelevant: {}",
        category_count(&dataset.entries, Category::RecentIrrelevant)
    );
    println!(
        "  Older irrelevant:  {}",
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

        println!("------------------------------------------------");
        println!("Query: {}", measurement.query_text);
        println!("Topic: {}", truth.topic);
        println!("Ground-truth relevant: {}", measurement.relevant_total);
        println!(
            "Candidate-stage relevant: {}/{} ({:.2}%)",
            measurement.candidate_relevant,
            measurement.relevant_total,
            percentage(measurement.candidate_relevant, measurement.relevant_total)
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

        for k in &recall_ks {
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
        }

        println!("Result IDs: {:?}", measurement.result_ids);
    }

    println!();
    println!("AGGREGATE RESULTS");
    println!(
        "Candidate-stage recall: {}/{} ({:.2}%)",
        total_candidate_relevant, total_relevant, candidate_recall
    );
    println!(
        "Older-relevant candidate recall: {}/{} ({:.2}%)",
        total_candidate_older_relevant, total_older_relevant, older_candidate_recall
    );

    for (index, k) in recall_ks.iter().enumerate() {
        println!(
            "Recall@{}: {:.4} ({}/{})",
            k,
            recall_totals[index] as f64 / total_relevant as f64,
            recall_totals[index],
            total_relevant
        );
    }

    println!();
    println!("LATENCY");
    println!(
        "Warm-up samples discarded: {}",
        QUERY_COUNT * WARMUP_ITERATIONS
    );
    println!("Measured sample count: {}", latency_samples_micros.len());
    println!("p50: {} us", p50);
    println!("p95: {} us", p95);
    println!("Percentile method: nearest-rank");

    println!();
    println!("ENVIRONMENT");
    println!("OS: {}", env::consts::OS);
    println!("Architecture: {}", env::consts::ARCH);
    println!("SQLite: sqlx SQLite backend");
    println!("Runtime: Tokio");
    println!("Dependencies added: none");
    println!("Timestamps: fixed RFC3339 fixture values");
    println!("IDs: deterministic UUIDs");
    println!("Candidate observation: production query observer");
    println!("Scoring: production SqliteStorage::query()");
    println!("Measurement: query execution elapsed time");
    println!("Fixture state: restored after every execution");

    println!();
    println!("DECISION CRITERION");
    println!(
        "Older-relevant candidate recall below {:.2}% is considered \
         materially degraded.",
        OLDER_CANDIDATE_RECALL_THRESHOLD_PERCENT
    );

    if older_candidate_recall < OLDER_CANDIDATE_RECALL_THRESHOLD_PERCENT {
        println!("DECISION: CHANGE_REQUIRED");
        println!(
            "Older-relevant candidate recall: {:.2}%",
            older_candidate_recall
        );
        println!(
            "Older relevant entries excluded before scoring: {}/{}",
            total_older_relevant - total_candidate_older_relevant,
            total_older_relevant
        );
        println!("Overall candidate-stage recall: {:.2}%", candidate_recall);
        println!(
            "End-to-end Recall@10: {:.4} ({}/{})",
            recall_totals[3] as f64 / total_relevant as f64,
            recall_totals[3],
            total_relevant
        );
    } else {
        println!("DECISION: NO_CHANGE");
        println!(
            "Older-relevant candidate recall: {:.2}% ({}/{})",
            older_candidate_recall, total_candidate_older_relevant, total_older_relevant
        );
        println!(
            "Older relevant entries excluded before scoring: {}/{}",
            total_older_relevant - total_candidate_older_relevant,
            total_older_relevant
        );
        println!("Overall candidate-stage recall: {:.2}%", candidate_recall);
        println!(
            "End-to-end Recall@10: {:.4} ({}/{})",
            recall_totals[3] as f64 / total_relevant as f64,
            recall_totals[3],
            total_relevant
        );
    }

    println!("================================================");
    println!();

    assert_eq!(
        total_relevant,
        QUERY_COUNT * (OLD_RELEVANT_PER_QUERY + RECENT_RELEVANT_PER_QUERY)
    );

    assert_eq!(total_older_relevant, QUERY_COUNT * OLD_RELEVANT_PER_QUERY);

    assert_eq!(measurements.len(), QUERY_COUNT);

    assert_eq!(
        latency_samples_micros.len(),
        QUERY_COUNT * MEASURED_ITERATIONS
    );

    assert!(
        measurements
            .iter()
            .all(|measurement| measurement.candidate_relevant <= measurement.relevant_total),
        "candidate relevant count cannot exceed ground-truth relevant count"
    );

    assert!(
        measurements
            .iter()
            .all(|measurement| measurement.candidate_older_relevant
                <= measurement.older_relevant_total),
        "candidate older-relevant count cannot exceed older-relevant total"
    );

    drop(storage);

    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));
}

// -----------------------------------------------------------------------------
// Phase A: production candidate quota measurement.
// Only the frozen production quota (80, 20) is measured.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn measure_layered_candidate_quota_sensitivity() {
    let dataset = build_dataset();

    assert_eq!(dataset.entries.len(), 488);
    assert_eq!(PHASE_A_OLDER_RELEVANT_TOTAL, 20);
    assert_eq!(PHASE_A_RELEVANT_TOTAL, 28);

    let _measurement_guard = latency_measurement_lock().lock().await;

    let database_path = env::temp_dir().join(format!(
        "ai-memory-adapter-quota-sensitivity-{}.sqlite",
        std::process::id()
    ));

    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));

    let storage = SqliteStorage::open(&database_path)
        .await
        .expect("open quota sensitivity database");

    for entry in &dataset.entries {
        storage
            .store(entry.clone())
            .await
            .expect("store quota sensitivity fixture");
    }

    let mut candidate_relevant_total = 0usize;
    let mut candidate_older_relevant_total = 0usize;
    let mut recall_at_10_total = 0usize;
    let mut latency_samples_micros = Vec::with_capacity(MEASURED_ITERATIONS * QUERY_COUNT);

    for truth in &dataset.ground_truth {
        let query = Query {
            text: truth.query_text.to_string(),
            session_id: Some(SESSION_ID.to_string()),
            work_item_id: Some("wi-scope".into()),
            limit: QUERY_LIMIT,
            threshold: THRESHOLD,
        };

        let results = storage
            .query(&query)
            .await
            .expect("production query execution must succeed");

        let candidate_ids = ai_memory_adapter::test_support_api::last_query_candidate_ids();
        let candidate_set: HashSet<Uuid> = candidate_ids.iter().copied().collect();

        assert!(
            candidate_ids.len() <= CANDIDATE_LIMIT,
            "candidate-stage row count exceeded candidate_limit for {}",
            truth.query_text
        );

        candidate_relevant_total += truth.expected_relevant.intersection(&candidate_set).count();

        candidate_older_relevant_total += truth.older_relevant.intersection(&candidate_set).count();

        let result_ids: Vec<Uuid> = results.iter().map(|entry| entry.id).collect();

        for id in &result_ids {
            assert!(
                candidate_set.contains(id),
                "result {} was not in the measured candidate set",
                id
            );
        }

        recall_at_10_total += result_ids
            .iter()
            .filter(|id| truth.expected_relevant.contains(id))
            .count();

        restore_timestamps(&storage, &dataset.entries).await;

        for _ in 0..WARMUP_ITERATIONS {
            let warmup_results = storage
                .query(&query)
                .await
                .expect("warm-up query execution must succeed");

            assert!(
                warmup_results.len() <= query.limit,
                "warm-up query returned more than its requested limit"
            );

            restore_timestamps(&storage, &dataset.entries).await;
        }

        for _ in 0..MEASURED_ITERATIONS {
            let start = Instant::now();

            let measured_results = storage
                .query(&query)
                .await
                .expect("measured query execution must succeed");

            latency_samples_micros.push(start.elapsed().as_micros());

            assert!(
                measured_results.len() <= query.limit,
                "measured query returned more than its requested limit"
            );

            restore_timestamps(&storage, &dataset.entries).await;
        }
    }

    assert_eq!(
        latency_samples_micros.len(),
        MEASURED_ITERATIONS * QUERY_COUNT
    );

    let older_recall = percentage(candidate_older_relevant_total, PHASE_A_OLDER_RELEVANT_TOTAL);
    let candidate_recall = percentage(candidate_relevant_total, PHASE_A_RELEVANT_TOTAL);
    let recall_at_10 = percentage(recall_at_10_total, PHASE_A_RELEVANT_TOTAL);
    let p50_us = percentile(&latency_samples_micros, 0.50);
    let p95_us = percentile(&latency_samples_micros, 0.95);

    println!();
    println!("========== PHASE A: PRODUCTION QUOTA MEASUREMENT ==========");
    println!("Corpus size: {}", dataset.entries.len());
    println!("Candidate limit: {}", CANDIDATE_LIMIT);
    println!("Query count: {}", QUERY_COUNT);
    println!(
        "Older-relevant denominator: {}",
        PHASE_A_OLDER_RELEVANT_TOTAL
    );
    println!("Candidate-stage denominator: {}", PHASE_A_RELEVANT_TOTAL);
    println!("Query limit: {}", QUERY_LIMIT);
    println!("Threshold: {:.2}", THRESHOLD);
    println!("Warm-up iterations per query: {}", WARMUP_ITERATIONS);
    println!("Measured iterations per query: {}", MEASURED_ITERATIONS);
    println!("Latency sample count: {}", latency_samples_micros.len());
    println!("Percentile method: nearest-rank");
    println!();

    println!(
        "| recent_slots | older_slots | older_recall% | cand_recall% | Recall@10 | p50us | p95us |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    println!(
        "| 80 | 20 | {:.2}% | {:.2}% | {:.2}% | {} | {} |",
        older_recall, candidate_recall, recall_at_10, p50_us, p95_us
    );

    println!("================================================");
    println!();

    drop(storage);

    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(format!("{}-shm", database_path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", database_path.display()));
}

// -----------------------------------------------------------------------------
// P1-05: threshold sweep. Measurement only; no production scoring changes.
// -----------------------------------------------------------------------------

const FIXED_THRESHOLDS: [f32; 6] = [0.8, 0.5, 0.34, 0.25, 0.2, 0.1];

struct SweepResult {
    label: String,
    threshold: Option<f32>,
    recall_at_10: f64,
    candidate_recall: f64,
    returned_rows_total: usize,
    returned_rows_mean: f64,
    p50_us: u128,
    p95_us: u128,
    chinese_pass: usize,
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

// Token count mirrors the P1-02 tokenizer structure:
// full alphanumeric segment + CJK unigrams + CJK bigrams.
fn query_token_count(text: &str) -> usize {
    let mut tokens = HashSet::new();

    for segment in text.split(|ch: char| !ch.is_alphanumeric()) {
        if segment.is_empty() {
            continue;
        }

        tokens.insert(segment.to_lowercase());

        let chars: Vec<char> = segment.chars().collect();
        let mut index = 0;

        while index < chars.len() {
            if !is_cjk(chars[index]) {
                index += 1;
                continue;
            }

            let start = index;
            while index < chars.len() && is_cjk(chars[index]) {
                index += 1;
            }

            let run = &chars[start..index];

            for ch in run {
                tokens.insert(ch.to_string());
            }

            for pair in run.windows(2) {
                tokens.insert(pair.iter().collect::<String>());
            }
        }
    }

    tokens.len()
}

// For n > 1, a one-token overlap scores 1/n.
// Adding 0.01 places the threshold strictly above that score.
// For a single-token query, preserving exact-match retrieval takes priority.
fn adaptive_threshold(token_count: usize) -> f32 {
    if token_count <= 1 {
        0.0
    } else {
        (1.0 / token_count as f32 + 0.01).min(1.0)
    }
}

fn remove_database_files(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
}

async fn store_chinese_entry(storage: &SqliteStorage, session: &str, content: &str) {
    let now = Utc::now();

    storage
        .store(MemoryEntry {
            id: Uuid::new_v4(),
            session_id: session.to_string(),
            work_item_id: Some("wi-scope".into()),
            content: content.to_string(),
            embedding: None,
            metadata: json!({"source": "chinese-retrieval-test"}),
            created_at: now,
            access_count: 0,
            last_accessed: now,
        })
        .await
        .unwrap();
}

async fn chinese_search(storage: &SqliteStorage, text: &str, threshold: f32) -> Vec<MemoryEntry> {
    storage
        .query(&Query {
            text: text.to_string(),
            session_id: None,
            work_item_id: Some("wi-scope".into()),
            limit: 20,
            threshold,
        })
        .await
        .unwrap()
}

// Each scenario gets an isolated database, preserving the original test
// isolation and preventing fixtures from one scenario affecting another.
async fn run_chinese_scenarios(threshold: Option<f32>) -> usize {
    let mut passed = 0usize;

    // 1. chinese_partial_phrase_matches
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-partial-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        store_chinese_entry(&storage, "session-zh", "今天天气很好").await;
        store_chinese_entry(&storage, "session-zh", "明天天气预报").await;

        let query = "天气";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
        let results = chinese_search(&storage, query, cutoff).await;

        let ok = results.len() == 2
            && results.iter().any(|e| e.content == "今天天气很好")
            && results.iter().any(|e| e.content == "明天天气预报");

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 1 chinese_partial_phrase_matches: {}  (query=\"{}\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            query,
            threshold.map_or_else(
                || "adaptive".to_string(),
                |value| format!("{value:.2}")
            ),
            results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    // 2. chinese_multi_char_partial
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-multi-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        store_chinese_entry(&storage, "session-zh", "机器学习是人工智能").await;

        let query = "机器学";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
        let results = chinese_search(&storage, query, cutoff).await;

        let ok = results.iter().any(|e| e.content == "机器学习是人工智能");

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 2 chinese_multi_char_partial: {}  (query=\"{}\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            query,
            threshold.map_or_else(|| "adaptive".to_string(), |value| format!("{value:.2}")),
            results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    // 3. chinese_english_mixed
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-mixed-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        store_chinese_entry(&storage, "session-mixed", "Rust 的所有权系统保证内存安全").await;

        let query = "Rust 所有权";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
        let results = chinese_search(&storage, query, cutoff).await;

        let ok = results
            .iter()
            .any(|e| e.content == "Rust 的所有权系统保证内存安全");

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 3 chinese_english_mixed: {}  (query=\"{}\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            query,
            threshold.map_or_else(|| "adaptive".to_string(), |value| format!("{value:.2}")),
            results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    // 4. chinese_single_unigram_overlap_is_rejected
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-negative-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        store_chinese_entry(&storage, "session-zh", "明天要下雨").await;
        store_chinese_entry(&storage, "session-zh", "今天开会").await;

        let query = "天气";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
        let results = chinese_search(&storage, query, cutoff).await;

        let ok = results.is_empty();

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 4 chinese_single_unigram_overlap_is_rejected: {}  (query=\"{}\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            query,
            threshold.map_or_else(
                || "adaptive".to_string(),
                |value| format!("{value:.2}")
            ),
            results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    // 5. mixed_query_requires_chinese_concept_overlap
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-negative-mixed-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        store_chinese_entry(
            &storage,
            "session-mixed",
            "Rust is a fast programming language",
        )
        .await;
        store_chinese_entry(
            &storage,
            "session-mixed",
            "Rust provides excellent performance",
        )
        .await;

        let query = "Rust 所有权";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
        let results = chinese_search(&storage, query, cutoff).await;

        let ok = results.is_empty();

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 5 mixed_query_requires_chinese_concept_overlap: {}  (query=\"{}\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            query,
            threshold.map_or_else(
                || "adaptive".to_string(),
                |value| format!("{value:.2}")
            ),
            results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    // 6. chinese_punctuation_variants
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-punctuation-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        let contents = ["设计完成，开发中", "设计完成, 开发中", "设计完成"];

        for content in contents {
            store_chinese_entry(&storage, "session-punctuation", content).await;
        }

        let mut ok = true;

        for query in ["设计完成，开发中", "设计完成, 开发中", "设计完成"] {
            let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));
            let results = chinese_search(&storage, query, cutoff).await;

            ok &= !results.is_empty();
            ok &= results.iter().any(|e| e.content == "设计完成，开发中");
            ok &= results.iter().any(|e| e.content == "设计完成, 开发中");
            ok &= results.iter().any(|e| e.content == "设计完成");

            if query == "设计完成" {
                println!(
                    "  scenario 6 chinese_punctuation_variants: {}  (query=\"设计完成，开发中 | 设计完成, 开发中 | 设计完成\", cutoff={}, returned={})",
                    if ok { "PASS" } else { "FAIL" },
                    threshold.map_or_else(
                        || "adaptive".to_string(),
                        |value| format!("{value:.2}")
                    ),
                    results.len()
                );
            }
        }

        if ok {
            passed += 1;
        }

        drop(storage);
        remove_database_files(&path);
    }

    // 7. english_baseline
    {
        let path = env::temp_dir().join(format!(
            "ai-memory-threshold-zh-english-{}.sqlite3",
            Uuid::new_v4()
        ));
        let storage = SqliteStorage::open(&path).await.unwrap();

        for content in [
            "quick brown fox",
            "THE FOX jumps over the fence",
            "foxes are clever animals",
            "quick brown dog",
        ] {
            store_chinese_entry(&storage, "session-en", content).await;
        }

        let query = "fox";
        let cutoff = threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(query)));

        let results = chinese_search(&storage, query, cutoff).await;

        let mut ok = results.len() == 2;
        ok &= results.iter().any(|e| e.content == "quick brown fox");
        ok &= results
            .iter()
            .any(|e| e.content == "THE FOX jumps over the fence");
        ok &= !results.iter().any(|e| e.content.contains("foxes"));
        ok &= !results.iter().any(|e| e.content == "quick brown dog");

        let uppercase_results = chinese_search(&storage, "FOX", cutoff).await;

        ok &= uppercase_results.len() == 2;
        ok &= uppercase_results
            .iter()
            .any(|e| e.content == "quick brown fox");
        ok &= uppercase_results
            .iter()
            .any(|e| e.content == "THE FOX jumps over the fence");

        let unrelated_results = chinese_search(&storage, "elephant", cutoff).await;

        ok &= unrelated_results.is_empty();

        if ok {
            passed += 1;
        }

        println!(
            "  scenario 7 english_baseline: {}  (query=\"fox / FOX / elephant\", cutoff={}, returned={})",
            if ok { "PASS" } else { "FAIL" },
            threshold.map_or_else(
                || "adaptive".to_string(),
                |value| format!("{value:.2}")
            ),
            results.len() + uppercase_results.len() + unrelated_results.len()
        );

        drop(storage);
        remove_database_files(&path);
    }

    passed
}

async fn measure_threshold(dataset: &Dataset, threshold: Option<f32>, label: &str) -> SweepResult {
    let database_path = env::temp_dir().join(format!(
        "ai-memory-adapter-threshold-{}-{}.sqlite",
        label,
        std::process::id()
    ));

    remove_database_files(&database_path);

    let storage = SqliteStorage::open(&database_path)
        .await
        .expect("open threshold sweep database");

    for entry in &dataset.entries {
        storage
            .store(entry.clone())
            .await
            .expect("store threshold sweep fixture");
    }

    let mut relevant_hits = 0usize;
    let mut candidate_hits = 0usize;
    let mut returned_rows_total = 0usize;
    let mut latency_samples = Vec::with_capacity(QUERY_COUNT * MEASURED_ITERATIONS);

    for truth in &dataset.ground_truth {
        let effective_threshold =
            threshold.unwrap_or_else(|| adaptive_threshold(query_token_count(truth.query_text)));

        let query = Query {
            text: truth.query_text.to_string(),
            session_id: Some(SESSION_ID.to_string()),
            work_item_id: Some("wi-scope".into()),
            limit: QUERY_LIMIT,
            threshold: effective_threshold,
        };

        // One untimed result query establishes recall and candidate recall.
        let results = storage
            .query(&query)
            .await
            .expect("untimed threshold query must succeed");

        let candidate_ids = ai_memory_adapter::test_support_api::last_query_candidate_ids();

        let candidate_set: HashSet<Uuid> = candidate_ids.iter().copied().collect();

        candidate_hits += truth.expected_relevant.intersection(&candidate_set).count();

        relevant_hits += results
            .iter()
            .filter(|entry| truth.expected_relevant.contains(&entry.id))
            .count();

        restore_timestamps(&storage, &dataset.entries).await;

        for _ in 0..WARMUP_ITERATIONS {
            let warmup = storage
                .query(&query)
                .await
                .expect("threshold warm-up query must succeed");

            if warmup.len() > QUERY_LIMIT {
                eprintln!(
                    "Measurement warning: warm-up exceeded limit for {}",
                    truth.query_text
                );
            }

            restore_timestamps(&storage, &dataset.entries).await;
        }

        for _ in 0..MEASURED_ITERATIONS {
            let start = Instant::now();

            let results = storage
                .query(&query)
                .await
                .expect("measured threshold query must succeed");

            latency_samples.push(start.elapsed().as_micros());
            returned_rows_total += results.len();

            restore_timestamps(&storage, &dataset.entries).await;
        }
    }

    let p50_us = percentile(&latency_samples, 0.50);
    let p95_us = percentile(&latency_samples, 0.95);

    let recall_at_10 = percentage(relevant_hits, PHASE_A_RELEVANT_TOTAL);

    let candidate_recall = percentage(candidate_hits, PHASE_A_RELEVANT_TOTAL);

    let chinese_pass = run_chinese_scenarios(threshold).await;

    let result = SweepResult {
        label: label.to_string(),
        threshold,
        recall_at_10,
        candidate_recall,
        returned_rows_total,
        returned_rows_mean: returned_rows_total as f64 / (QUERY_COUNT * MEASURED_ITERATIONS) as f64,
        p50_us,
        p95_us,
        chinese_pass,
    };

    drop(storage);
    remove_database_files(&database_path);

    result
}

#[tokio::test]
async fn measure_threshold_sweep() {
    let _measurement_guard = latency_measurement_lock().lock().await;

    let dataset = build_dataset();

    println!();
    println!("========== P1-05 THRESHOLD SWEEP ==========");
    println!("Corpus size: {}", dataset.entries.len());
    println!("Relevant denominator: {}", PHASE_A_RELEVANT_TOTAL);
    println!("Candidate-stage denominator: {}", PHASE_A_RELEVANT_TOTAL);
    println!("Candidate quota: recent=80, older=20");
    println!("Query count: {}", QUERY_COUNT);
    println!("Query limit: {}", QUERY_LIMIT);
    println!("Warm-up: {} per query", WARMUP_ITERATIONS);
    println!("Measured: {} per query", MEASURED_ITERATIONS);
    println!(
        "Latency samples per threshold: {}",
        QUERY_COUNT * MEASURED_ITERATIONS
    );
    println!("Percentile method: nearest-rank");
    println!();

    let mut results = Vec::with_capacity(7);

    for threshold in FIXED_THRESHOLDS {
        let label = format!("{threshold:.2}");

        let result = measure_threshold(&dataset, Some(threshold), &label).await;

        println!(
            "Completed threshold {:.2}: chinese {}/7",
            threshold, result.chinese_pass
        );

        results.push(result);
    }

    let adaptive = measure_threshold(&dataset, None, "adaptive").await;

    println!("Completed adaptive: chinese {}/7", adaptive.chinese_pass);

    results.push(adaptive);

    println!();
    println!(
        "| threshold | Recall@10 | cand_recall% | returned_rows | mean_rows | p50us | p95us |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");

    for result in &results {
        println!(
            "| {} | {:.2}% | {:.2}% | {} | {:.2} | {} | {} |",
            result.label,
            result.recall_at_10,
            result.candidate_recall,
            result.returned_rows_total,
            result.returned_rows_mean,
            result.p50_us,
            result.p95_us
        );
    }

    println!();
    println!("| threshold | mean_rows | p50us |");
    println!("|---|---:|---:|");

    for result in &results {
        println!(
            "| {} | {:.2} | {} |",
            result.label, result.returned_rows_mean, result.p50_us
        );
    }

    println!();
    println!("| threshold | chinese_pass |");
    println!("|---|---:|");

    for result in &results {
        println!("| {} | {}/7 |", result.label, result.chinese_pass);
    }

    let qualifying: Vec<&SweepResult> = results
        .iter()
        .filter(|result| {
            result.recall_at_10 >= 95.0 && result.chinese_pass == 7 && result.p95_us <= 6000
        })
        .collect();

    println!();
    println!("PRODUCTION_DEFAULT: {:.2}", PRODUCTION_DEFAULT_THRESHOLD);
    println!("HARNESS_PRIOR: {:.2}", HARNESS_PRIOR);

    let qualifying_labels: Vec<&str> = qualifying
        .iter()
        .map(|result| result.label.as_str())
        .collect();

    println!("QUALIFYING_THRESHOLDS: {:?}", qualifying_labels);

    let recommended = qualifying
        .iter()
        .filter(|result| result.threshold.is_some())
        .max_by(|a, b| {
            a.threshold
                .partial_cmp(&b.threshold)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

    match recommended {
        Some(result) => println!(
            "RECOMMENDED_PRODUCTION_THRESHOLD: {:.2}",
            result.threshold.unwrap()
        ),
        None => println!("RECOMMENDED_PRODUCTION_THRESHOLD: NONE — escalate to Owner"),
    }

    let fixed_results: Vec<&SweepResult> = results
        .iter()
        .filter(|result| result.threshold.is_some())
        .collect();

    let best_fixed = fixed_results.iter().max_by(|a, b| {
        a.recall_at_10
            .partial_cmp(&b.recall_at_10)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.p95_us.cmp(&a.p95_us))
            .then_with(|| b.chinese_pass.cmp(&a.chinese_pass))
    });

    let adaptive_result = results
        .iter()
        .find(|result| result.threshold.is_none())
        .unwrap();

    let adaptive_better = best_fixed.is_some_and(|fixed| {
        adaptive_result.recall_at_10 >= fixed.recall_at_10
            && adaptive_result.p95_us <= fixed.p95_us
            && adaptive_result.chinese_pass >= fixed.chinese_pass
            && (adaptive_result.recall_at_10 > fixed.recall_at_10
                || adaptive_result.p95_us < fixed.p95_us
                || adaptive_result.chinese_pass > fixed.chinese_pass)
    });

    println!(
        "ADAPTIVE_BETTER_THAN_BEST_FIXED: {}",
        if adaptive_better { "YES" } else { "NO" }
    );

    if let Some(fixed) = best_fixed {
        println!(
            "BEST_FIXED_FOR_COMPARISON: {} (Recall@10={:.2}%, p95={}us, chinese={}/7)",
            fixed.label, fixed.recall_at_10, fixed.p95_us, fixed.chinese_pass
        );
    }

    println!("===========================================");
}
