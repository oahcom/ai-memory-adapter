//! Process-level integration tests for the JSON-over-stdio binary (T154-18).
//!
//! Every test spawns the real compiled executable and speaks to it through
//! stdin/stdout. Nothing is mocked: the SQLite database is a real file on disk.

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use serde_json::Value;
use uuid::Uuid;

use ai_memory_adapter::{adapter::MemoryAdapter, storage::SqliteStorage};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_memory-stdio")
}

fn database_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ma-stdio-{label}-{}.sqlite3", Uuid::new_v4()))
}

struct Removal(PathBuf);

impl Drop for Removal {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", self.0.display(), suffix));
        }
    }
}

struct Outcome {
    stdout: String,
    stderr: String,
    code: i32,
}

fn invoke(db: &Path, input: &str) -> Outcome {
    let mut command = Command::new(binary());
    command
        .arg("--db")
        .arg(db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().expect("binary must start");
    child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(input.as_bytes())
        .expect("request must be writable");
    let output = child.wait_with_output().expect("binary must exit");

    Outcome {
        stdout: String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        stderr: String::from_utf8(output.stderr).expect("stderr is UTF-8"),
        code: output.status.code().expect("exit code"),
    }
}

/// Exactly one JSON response line, per contract §5.
fn response(outcome: &Outcome) -> Value {
    let lines: Vec<&str> = outcome
        .stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "stdout must hold exactly one line: {:?}",
        outcome.stdout
    );
    assert_eq!(
        outcome.code, 0,
        "protocol responses exit 0: {:?}",
        outcome.stderr
    );
    serde_json::from_str(lines[0]).expect("response is valid JSON")
}

fn work_item() -> String {
    format!("wi_{}", Uuid::new_v4())
}

fn store_request(request_id: &str, work_item: &str, session: &str, content: &str) -> String {
    serde_json::json!({
        "contract_version": "0.1",
        "request_id": request_id,
        "operation": "store",
        "payload": {
            "work_item_id": work_item,
            "session_id": session,
            "content": content,
            "provenance": {
                "source_type": "execution_result",
                "source_id": session,
                "captured_at": "2026-11-21T10:00:00Z"
            },
            "metadata": {"owner": "so"}
        }
    })
    .to_string()
}

fn retrieve_request(request_id: &str, work_item: &str, session: Option<&str>) -> String {
    let mut payload = serde_json::json!({
        "work_item_id": work_item,
        "query": "migration completed",
        "limit": 10,
        "threshold": 0.0
    });
    if let Some(session) = session {
        payload["session_id"] = Value::String(session.to_owned());
    }
    serde_json::json!({
        "contract_version": "0.1",
        "request_id": request_id,
        "operation": "retrieve",
        "payload": payload
    })
    .to_string()
}

fn error_code(response: &Value) -> &str {
    assert_eq!(response["ok"], Value::Bool(false), "{response}");
    response["error"]["code"].as_str().expect("error code")
}

#[test]
fn valid_store_returns_memory_id() {
    let db = database_path("store");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let response = response(&invoke(
        &db,
        &store_request(
            "req_001",
            &item,
            "session-001",
            "The migration was completed.",
        ),
    ));

    assert_eq!(response["contract_version"], "0.1");
    assert_eq!(response["request_id"], "req_001");
    assert_eq!(response["ok"], Value::Bool(true));
    let memory_id = response["result"]["memory_id"].as_str().expect("memory_id");
    assert!(
        Uuid::parse_str(memory_id).is_ok(),
        "memory_id is a UUID: {memory_id}"
    );
}

#[test]
fn valid_retrieve_returns_stored_entry_with_provenance() {
    let db = database_path("retrieve");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    invoke(
        &db,
        &store_request(
            "req_001",
            &item,
            "session-001",
            "The migration was completed.",
        ),
    );

    let response = response(&invoke(&db, &retrieve_request("req_002", &item, None)));

    assert_eq!(response["ok"], Value::Bool(true));
    let entries = response["result"]["entries"]
        .as_array()
        .expect("entries array");
    assert_eq!(
        entries.len(),
        1,
        "cross-session retrieval inside one WorkItem: {response}"
    );
    let entry = &entries[0];
    assert_eq!(entry["work_item_id"], item.as_str());
    assert_eq!(entry["session_id"], "session-001");
    assert_eq!(entry["content"], "The migration was completed.");
    assert_eq!(entry["provenance"]["source_type"], "execution_result");
    assert_eq!(entry["provenance"]["source_id"], "session-001");
    assert_eq!(entry["provenance"]["captured_at"], "2026-11-21T10:00:00Z");
    assert!(entry["created_at"]
        .as_str()
        .is_some_and(|v| v.ends_with('Z')));
    assert!(entry["score"].as_f64().is_some_and(|score| score > 0.0));
    assert_eq!(response["result"]["entries_included"], 1);
    assert_eq!(response["result"]["entries_omitted"], 0);
}

#[test]
fn unknown_contract_version_is_rejected() {
    let db = database_path("version");
    let _cleanup = Removal(db.clone());

    let request = serde_json::json!({
        "contract_version": "9.9",
        "request_id": "req_003",
        "operation": "retrieve",
        "payload": {"work_item_id": work_item(), "query": "x"}
    });
    let response = response(&invoke(&db, &request.to_string()));

    assert_eq!(error_code(&response), "UNSUPPORTED_VERSION");
    assert_eq!(response["request_id"], "req_003");
}

#[test]
fn invalid_operation_is_rejected() {
    let db = database_path("operation");
    let _cleanup = Removal(db.clone());

    let request = serde_json::json!({
        "contract_version": "0.1",
        "request_id": "req_004",
        "operation": "delete",
        "payload": {"work_item_id": work_item()}
    });
    let response = response(&invoke(&db, &request.to_string()));

    assert_eq!(error_code(&response), "INVALID_OPERATION");
}

#[test]
fn invalid_identifier_is_rejected() {
    let db = database_path("id");
    let _cleanup = Removal(db.clone());

    for bad in [
        "wi_not-a-uuid",
        "550e8400-e29b-41d4-a716-446655440000",
        "wi_",
    ] {
        let response = response(&invoke(
            &db,
            &store_request("req_005", bad, "session-001", "content"),
        ));
        assert_eq!(error_code(&response), "INVALID_ID", "identifier {bad}");
    }
}

// Contract §3 fixes a single canonical form, `wi_<UUIDv4>` in lowercase
// hyphenated notation. `Uuid::parse_str` alone also accepts all four of these.
#[test]
fn non_canonical_work_item_id_forms_are_rejected() {
    let db = database_path("id-canonical");
    let _cleanup = Removal(db.clone());

    for bad in [
        "wi_550e8400e29b41d4a716446655440000",
        "wi_550E8400-E29B-41D4-A716-446655440000",
        "wi_{550e8400-e29b-41d4-a716-446655440000}",
        "wi_urn:uuid:550e8400-e29b-41d4-a716-446655440000",
    ] {
        let response = response(&invoke(
            &db,
            &store_request("req_005a", bad, "session-001", "content"),
        ));
        assert_eq!(
            error_code(&response),
            "INVALID_ID",
            "non-canonical form must be rejected: {bad}"
        );
    }
}

#[test]
fn canonical_work_item_id_is_accepted() {
    let db = database_path("id-accepted");
    let _cleanup = Removal(db.clone());

    let response = response(&invoke(
        &db,
        &store_request(
            "req_005b",
            "wi_550e8400-e29b-41d4-a716-446655440000",
            "session-001",
            "content",
        ),
    ));

    assert_eq!(response["ok"], Value::Bool(true), "response: {response}");
}

#[test]
fn missing_work_item_scope_is_rejected() {
    let db = database_path("scope");
    let _cleanup = Removal(db.clone());

    let request = serde_json::json!({
        "contract_version": "0.1",
        "request_id": "req_006",
        "operation": "retrieve",
        "payload": {"session_id": "session-001", "query": "migration"}
    });
    let response = response(&invoke(&db, &request.to_string()));

    assert_eq!(error_code(&response), "INVALID_SCOPE");
}

#[test]
fn invalid_provenance_is_rejected() {
    let db = database_path("provenance");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let cases = [
        serde_json::json!({}),
        serde_json::json!({"source_type": "guess", "source_id": "s", "captured_at": "2026-11-21T10:00:00Z"}),
        serde_json::json!({"source_type": "conversation", "source_id": "", "captured_at": "2026-11-21T10:00:00Z"}),
        serde_json::json!({"source_type": "conversation", "source_id": "s", "captured_at": "yesterday"}),
    ];

    for provenance in cases {
        let request = serde_json::json!({
            "contract_version": "0.1",
            "request_id": "req_007",
            "operation": "store",
            "payload": {
                "work_item_id": item,
                "session_id": "session-001",
                "content": "content",
                "provenance": provenance
            }
        });
        let response = response(&invoke(&db, &request.to_string()));
        assert_eq!(error_code(&response), "INVALID_PROVENANCE", "{provenance}");
    }
}

#[test]
fn session_filter_narrows_retrieval_within_one_work_item() {
    let db = database_path("session-filter");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    invoke(
        &db,
        &store_request("req_a", &item, "session-a", "migration completed alpha"),
    );
    invoke(
        &db,
        &store_request("req_b", &item, "session-b", "migration completed beta"),
    );

    // Absent session_id: both Sessions inside the WorkItem are visible.
    let all = response(&invoke(&db, &retrieve_request("req_all", &item, None)));
    assert_eq!(all["result"]["entries"].as_array().map(Vec::len), Some(2));

    // Present session_id: narrowed to that Session only.
    let filtered = response(&invoke(
        &db,
        &retrieve_request("req_one", &item, Some("session-b")),
    ));
    let entries = filtered["result"]["entries"]
        .as_array()
        .expect("entries array");
    assert_eq!(entries.len(), 1, "session filter must narrow: {filtered}");
    assert_eq!(entries[0]["session_id"], "session-b");
    assert_eq!(entries[0]["content"], "migration completed beta");
    assert_eq!(entries[0]["work_item_id"], item.as_str());
}

#[test]
fn cross_work_item_entries_are_never_returned() {
    let db = database_path("isolation");
    let _cleanup = Removal(db.clone());
    let (first, second) = (work_item(), work_item());

    invoke(
        &db,
        &store_request("req_a", &first, "session-a", "migration completed alpha"),
    );
    invoke(
        &db,
        &store_request("req_b", &second, "session-b", "migration completed beta"),
    );

    let response = response(&invoke(&db, &retrieve_request("req_c", &first, None)));

    assert_eq!(response["ok"], Value::Bool(true));
    let entries = response["result"]["entries"]
        .as_array()
        .expect("entries array");
    assert_eq!(
        entries.len(),
        1,
        "only the first WorkItem may match: {response}"
    );
    assert_eq!(entries[0]["work_item_id"], first.as_str());
    assert_eq!(entries[0]["content"], "migration completed alpha");
}

#[test]
fn entries_survive_process_restart() {
    let db = database_path("restart");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let stored = response(&invoke(
        &db,
        &store_request(
            "req_001",
            &item,
            "session-001",
            "migration completed durably",
        ),
    ));
    let memory_id = stored["result"]["memory_id"]
        .as_str()
        .expect("memory_id")
        .to_owned();

    // Second process, same database file: proves the commit reached disk.
    let response = response(&invoke(&db, &retrieve_request("req_002", &item, None)));

    let entries = response["result"]["entries"]
        .as_array()
        .expect("entries array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["memory_id"], memory_id.as_str());
    assert_eq!(entries[0]["content"], "migration completed durably");
}

#[test]
fn stdout_and_stderr_stay_separated() {
    let db = database_path("streams");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let outcome = invoke(
        &db,
        &store_request("req_001", &item, "session-001", "migration completed"),
    );

    let parsed = response(&outcome);
    assert_eq!(parsed["ok"], Value::Bool(true));
    assert!(
        !outcome.stdout.contains("memory-stdio:"),
        "diagnostics must not reach stdout: {:?}",
        outcome.stdout
    );
    assert!(
        !outcome.stderr.contains("contract_version"),
        "protocol data must not reach stderr: {:?}",
        outcome.stderr
    );
    assert!(
        outcome.stdout.lines().count() == 1,
        "stdout must contain exactly one response line"
    );
}

#[test]
fn malformed_json_is_rejected_without_panicking() {
    let db = database_path("malformed");
    let _cleanup = Removal(db.clone());

    for bad in [
        "{not json",
        "[]",
        "\"a string\"",
        "{\"contract_version\":\"0.1\"}",
    ] {
        let outcome = invoke(&db, bad);
        let parsed = response(&outcome);
        assert_eq!(error_code(&parsed), "INVALID_REQUEST", "input {bad}");
        assert_eq!(outcome.code, 0);
    }
}

#[test]
fn storage_failure_is_reported_as_storage_error() {
    let root = std::env::temp_dir().join(format!("ma-stdio-missing-{}", Uuid::new_v4()));
    let db = root.join("absent").join("memory.sqlite3");
    let _cleanup = Removal(root);

    let response = response(&invoke(
        &db,
        &store_request(
            "req_001",
            &work_item(),
            "session-001",
            "migration completed",
        ),
    ));

    assert_eq!(error_code(&response), "STORAGE_ERROR");
    assert_eq!(response["error"]["retryable"], Value::Bool(false));
}

/// Spawns the binary, feeds stdin, and tolerates the process exiting before the
/// write lands (EPIPE): startup-failure cases legitimately exit before reading.
fn invoke_raw(db_arg: Option<&str>, input: &str) -> Outcome {
    let mut command = Command::new(binary());
    if let Some(db) = db_arg {
        command.arg("--db").arg(db);
    }
    command
        .env_remove("MA_SQLITE_PATH")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().expect("binary must start");
    let _ = child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(input.as_bytes());
    let output = child.wait_with_output().expect("binary must exit");

    Outcome {
        stdout: String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        stderr: String::from_utf8(output.stderr).expect("stderr is UTF-8"),
        code: output.status.code().expect("exit code"),
    }
}

#[test]
fn missing_database_path_aborts_without_protocol_output() {
    let outcome = invoke_raw(None, "{\"contract_version\":\"0.1\"}\n");

    assert_eq!(outcome.code, 2, "startup failure exit code");
    assert!(
        outcome.stdout.is_empty(),
        "no protocol output without a database"
    );
    assert!(
        outcome.stderr.contains("--db"),
        "stderr must explain the missing configuration"
    );
}

#[test]
fn relative_database_path_is_rejected() {
    let outcome = invoke_raw(Some("relative/memory.sqlite3"), "{}\n");

    assert_eq!(outcome.code, 2);
    assert!(outcome.stdout.is_empty());
    assert!(
        outcome.stderr.contains("absolute"),
        "stderr must state the absolute-path requirement"
    );
}

#[test]
fn empty_retrieval_is_a_successful_result() {
    let db = database_path("empty");
    let _cleanup = Removal(db.clone());

    let response = response(&invoke(
        &db,
        &retrieve_request("req_001", &work_item(), None),
    ));

    assert_eq!(response["ok"], Value::Bool(true));
    assert_eq!(
        response["result"]["entries"].as_array().map(Vec::len),
        Some(0)
    );
    assert_eq!(response["result"]["entries_included"], 0);
}

#[test]
fn max_chars_bounds_the_returned_entries() {
    let db = database_path("max-chars");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    invoke(
        &db,
        &store_request("req_a", &item, "session-001", "migration completed alpha"),
    );
    invoke(
        &db,
        &store_request("req_b", &item, "session-001", "migration completed beta"),
    );

    // Absent max_chars keeps the old behaviour: nothing is omitted.
    let unbounded = response(&invoke(&db, &retrieve_request("req_c", &item, None)));
    assert_eq!(
        unbounded["result"]["entries"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(unbounded["result"]["entries_omitted"], 0);

    // "migration completed alpha" is 25 characters, "... beta" is 24.
    let mut request: Value = serde_json::from_str(&retrieve_request("req_d", &item, None))
        .expect("retrieve request is JSON");
    request["payload"]["max_chars"] = Value::from(24);
    let bounded = response(&invoke(&db, &request.to_string()));
    assert_eq!(bounded["result"]["entries_included"], 1);
    assert_eq!(bounded["result"]["entries_omitted"], 1);
}

/// Contract §9: max_chars counts Unicode scalar values, not UTF-8 bytes.
/// Entries are either included whole or omitted whole; content is never truncated.
#[test]
fn retrieve_max_chars_counts_unicode_scalars() {
    let db = database_path("unicode-scalars");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let content = "中🙂";

    assert_eq!(content.chars().count(), 2);
    assert_eq!(content.len(), 7);

    invoke(
        &db,
        &store_request("req_unicode_store", &item, "session-unicode", content),
    );

    let mut exact_request: Value =
        serde_json::from_str(&retrieve_request("req_unicode_exact", &item, None))
            .expect("retrieve request is JSON");

    exact_request["payload"]["max_chars"] = Value::from(2);

    let exact = response(&invoke(&db, &exact_request.to_string()));

    let exact_entries = exact["result"]["entries"]
        .as_array()
        .expect("entries must be an array");

    assert_eq!(
        exact_entries.len(),
        1,
        "two Unicode scalar values must fit budget 2"
    );
    assert_eq!(
        exact_entries[0]["content"], content,
        "accepted content must remain completely unchanged"
    );
    assert_eq!(exact["result"]["entries_included"], 1);
    assert_eq!(exact["result"]["entries_omitted"], 0);

    let mut insufficient_request: Value =
        serde_json::from_str(&retrieve_request("req_unicode_short", &item, None))
            .expect("retrieve request is JSON");

    insufficient_request["payload"]["max_chars"] = Value::from(1);

    let insufficient = response(&invoke(&db, &insufficient_request.to_string()));

    let insufficient_entries = insufficient["result"]["entries"]
        .as_array()
        .expect("entries must be an array");

    assert!(
        insufficient_entries.is_empty(),
        "a two-scalar entry must not be partially returned under budget 1"
    );
    assert_eq!(insufficient["result"]["entries_included"], 0);
    assert_eq!(insufficient["result"]["entries_omitted"], 1);
}

/// Contract §9: max_chars is an aggregate budget over complete entries.
/// The budget counts Unicode scalar values, accepts exact boundaries,
/// and never truncates content.
#[test]
fn retrieve_max_chars_obeys_entry_budget_boundaries() {
    // ASCII exact boundary: "abc" fits budget 3.
    {
        let db = database_path("budget-ascii-exact");
        let _cleanup = Removal(db.clone());
        let item = work_item();

        invoke(&db, &store_request("req_a1", &item, "session-a", "abc"));

        let mut request: Value = serde_json::from_str(&retrieve_request("req_a2", &item, None))
            .expect("retrieve request is JSON");
        request["payload"]["max_chars"] = Value::from(3);

        let result = response(&invoke(&db, &request.to_string()));

        assert_eq!(
            result["result"]["entries"].as_array().map(Vec::len),
            Some(1),
            "ASCII content of length 3 must fit budget 3"
        );
        assert_eq!(result["result"]["entries"][0]["content"], "abc");
        assert_eq!(result["result"]["entries_included"], 1);
        assert_eq!(result["result"]["entries_omitted"], 0);
    }

    // ASCII over budget: "abcd" does not fit budget 3.
    {
        let db = database_path("budget-ascii-over");
        let _cleanup = Removal(db.clone());
        let item = work_item();

        invoke(&db, &store_request("req_b1", &item, "session-b", "abcd"));

        let mut request: Value = serde_json::from_str(&retrieve_request("req_b2", &item, None))
            .expect("retrieve request is JSON");
        request["payload"]["max_chars"] = Value::from(3);

        let result = response(&invoke(&db, &request.to_string()));

        assert!(
            result["result"]["entries"].as_array().unwrap().is_empty(),
            "ASCII content of length 4 must be omitted under budget 3"
        );
        assert_eq!(result["result"]["entries_included"], 0);
        assert_eq!(result["result"]["entries_omitted"], 1);
    }

    // Exact cumulative boundary: "ab" + "cd" consumes exactly 4.
    {
        let db = database_path("budget-cumulative-exact");
        let _cleanup = Removal(db.clone());
        let item = work_item();

        invoke(&db, &store_request("req_d1", &item, "session-d", "ab"));
        invoke(&db, &store_request("req_d2", &item, "session-d", "cd"));

        let mut request: Value = serde_json::from_str(&retrieve_request("req_d3", &item, None))
            .expect("retrieve request is JSON");
        request["payload"]["max_chars"] = Value::from(4);

        let result = response(&invoke(&db, &request.to_string()));

        assert_eq!(
            result["result"]["entries"].as_array().map(Vec::len),
            Some(2),
            "two entries totaling exactly 4 characters must both fit"
        );
        assert_eq!(result["result"]["entries_included"], 2);
        assert_eq!(result["result"]["entries_omitted"], 0);
    }

    // Cumulative over budget: "ab" fits, but adding "cde" exceeds 4.
    // "cde" is stored first so that "ab" is returned first deterministically.
    {
        let db = database_path("budget-cumulative-over");
        let _cleanup = Removal(db.clone());
        let item = work_item();

        invoke(&db, &store_request("req_e1", &item, "session-e", "cde"));
        invoke(&db, &store_request("req_e2", &item, "session-e", "ab"));

        let mut request: Value = serde_json::from_str(&retrieve_request("req_e3", &item, None))
            .expect("retrieve request is JSON");
        request["payload"]["max_chars"] = Value::from(4);

        let result = response(&invoke(&db, &request.to_string()));

        let entries = result["result"]["entries"]
            .as_array()
            .expect("entries must be an array");

        assert_eq!(
            entries.len(),
            1,
            "only the first two-character entry must fit before the second exceeds budget"
        );
        assert_eq!(
            entries[0]["content"], "ab",
            "the first entry must be retained whole"
        );
        assert_eq!(result["result"]["entries_included"], 1);
        assert_eq!(result["result"]["entries_omitted"], 1);
    }
}

/// Contract §9: max_chars must be a strictly positive integer.
/// Floats, booleans, strings, zero, and negative values are invalid.
#[test]
fn non_positive_or_non_integer_max_chars_is_rejected() {
    let db = database_path("max-chars-invalid");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let invalid_values = [
        ("zero", Value::from(0)),
        ("negative", Value::from(-1)),
        ("fractional", serde_json::json!(1.5)),
        ("boolean", Value::Bool(true)),
        ("string", Value::String("2".to_owned())),
    ];

    for (label, bad) in invalid_values {
        let mut request: Value =
            serde_json::from_str(&retrieve_request("req_invalid", &item, None))
                .expect("retrieve request is JSON");

        request["payload"]["max_chars"] = bad.clone();

        let parsed = response(&invoke(&db, &request.to_string()));

        assert_eq!(
            error_code(&parsed),
            "INVALID_REQUEST",
            "max_chars {label} ({bad}) must be rejected"
        );
    }
}

/// Contract §10: provenance is always an object.
/// Missing, null, and non-object metadata normalize to an empty object.
/// A valid object is preserved without modification.
/// Every returned entry has exactly seven fields.
#[test]
fn provenance_is_always_an_object_and_preserves_valid_objects() {
    let db = database_path("provenance-matrix");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let valid_provenance = serde_json::json!({
        "source_type": "execution_result",
        "source_id": "session-valid",
        "captured_at": "2026-11-21T10:00:00Z",
        "custom": {
            "origin": "integration-test"
        }
    });

    let cases = [
        ("missing", None),
        ("null", Some(Value::Null)),
        ("string", Some(Value::String("invalid".to_owned()))),
        ("array", Some(serde_json::json!(["invalid"]))),
        ("number", Some(Value::from(42))),
        ("object", Some(valid_provenance.clone())),
    ];

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime must build");

    runtime.block_on(async {
        let storage = SqliteStorage::open(&db).await.expect("storage must open");

        for (label, provenance) in &cases {
            let mut metadata = serde_json::json!({
                "owner": "so"
            });

            if let Some(provenance) = provenance {
                metadata["_so_contract"] = serde_json::json!({
                    "provenance": provenance
                });
            }

            let now = chrono::Utc::now();

            storage
                .store(ai_memory_adapter::adapter::MemoryEntry {
                    id: Uuid::new_v4(),
                    session_id: format!("session-{label}"),
                    work_item_id: Some(item.clone()),
                    content: format!("migration completed provenance-{label}"),
                    embedding: None,
                    metadata,
                    created_at: now,
                    access_count: 0,
                    last_accessed: now,
                })
                .await
                .expect("entry must store");
        }
    });

    let parsed = response(&invoke(
        &db,
        &retrieve_request("req_provenance_matrix", &item, None),
    ));

    let entries = parsed["result"]["entries"]
        .as_array()
        .expect("entries must be an array");

    assert_eq!(
        entries.len(),
        cases.len(),
        "all six provenance cases must be retrieved"
    );

    for (label, expected) in cases {
        let content = format!("migration completed provenance-{label}");

        let entry = entries
            .iter()
            .find(|entry| entry["content"] == content)
            .unwrap_or_else(|| panic!("missing provenance case: {label}"));

        assert_eq!(
            entry.as_object().map(serde_json::Map::len),
            Some(7),
            "entry {label} must contain exactly seven fields"
        );

        assert!(
            entry.get("memory_id").is_some(),
            "entry {label} must contain memory_id"
        );
        assert!(
            entry.get("work_item_id").is_some(),
            "entry {label} must contain work_item_id"
        );
        assert!(
            entry.get("session_id").is_some(),
            "entry {label} must contain session_id"
        );
        assert!(
            entry.get("content").is_some(),
            "entry {label} must contain content"
        );
        assert!(
            entry.get("provenance").is_some(),
            "entry {label} must contain provenance"
        );
        assert!(
            entry.get("created_at").is_some(),
            "entry {label} must contain created_at"
        );
        assert!(
            entry.get("score").is_some(),
            "entry {label} must contain score"
        );

        match expected {
            Some(value) if value.is_object() => {
                assert_eq!(
                    entry["provenance"], value,
                    "valid provenance object must be preserved unchanged"
                );
            }
            _ => {
                assert_eq!(
                    entry["provenance"],
                    serde_json::json!({}),
                    "missing, null, and non-object provenance must normalize to {{}}"
                );
            }
        }
    }
}

#[test]
fn multiple_request_lines_are_rejected_before_any_operation_runs() {
    let db = database_path("single-request");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let input = format!(
        "{}\n{}\n",
        store_request("req_001", &item, "session-001", "migration completed"),
        retrieve_request("req_002", &item, None)
    );
    let rejected = response(&invoke(&db, &input));

    assert_eq!(error_code(&rejected), "INVALID_REQUEST");
    assert_eq!(rejected["request_id"], "req_001");

    // The first Store must not have run before the extra line was rejected.
    let retrieved = response(&invoke(&db, &retrieve_request("req_003", &item, None)));
    assert_eq!(retrieved["request_id"], "req_003");
    assert_eq!(
        retrieved["result"]["entries"].as_array().map(Vec::len),
        Some(0)
    );
}

#[test]
fn empty_stdin_returns_one_invalid_request_response() {
    let db = database_path("empty-stdin");
    let _cleanup = Removal(db.clone());

    let parsed = response(&invoke(&db, ""));
    assert_eq!(error_code(&parsed), "INVALID_REQUEST");
}

/// Contract §5 is one request per invocation, but a client that keeps stdin
/// open must still get its answer: the read must not wait for EOF.
#[test]
fn request_is_answered_without_closing_stdin() {
    let db = database_path("open-stdin");
    let _cleanup = Removal(db.clone());
    let item = work_item();

    let mut child = Command::new(binary())
        .arg("--db")
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("binary must start");
    let mut stdin = child.stdin.take().expect("stdin piped");
    stdin
        // read_request() frames on the newline, so the request must be terminated
        // even though this stdin is deliberately never closed.
        .write_all(format!("{}\n", retrieve_request("req_open", &item, None)).as_bytes())
        .expect("request must be writable");
    stdin.flush().expect("request must be flushed");

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    let output = rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("the process must answer without waiting for stdin EOF")
        .expect("binary must exit");
    // stdin stays open until here, so the answer above cannot have needed EOF.
    drop(stdin);

    let parsed = response(&Outcome {
        stdout: String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        stderr: String::from_utf8(output.stderr).expect("stderr is UTF-8"),
        code: output.status.code().expect("exit code"),
    });
    assert_eq!(parsed["request_id"], "req_open");
}
