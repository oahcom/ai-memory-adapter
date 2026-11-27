//! JSON-over-stdio entrypoint for the MA contract v0.1 (T154-18).
//!
//! Invocation: `memory-stdio --db <path>`; `MA_SQLITE_PATH` is the fallback.
//!
//! Framing: the first complete JSON request line is the only request processed
//! by this invocation. A newline terminates the request; EOF also terminates a
//! final non-empty request line. The process responds without waiting for EOF.
//! Subsequent input is never processed. The client does not have to close stdin.
//! Exactly one JSON response line is written to stdout; diagnostics go to stderr.

use std::io::{BufRead, Write};

use ai_memory_adapter::{
    adapter::{AdapterError, MemoryAdapter, MemoryEntry, Query},
    context::CharBudget,
    storage::{
        sqlite::{lexical_score, tokenize},
        SqliteStorage,
    },
};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;

const CONTRACT_VERSION: &str = "0.1";
/// Reserved metadata key that carries SO provenance (contract §7).
const PROVENANCE_KEY: &str = "_so_contract";
const SOURCE_TYPES: [&str; 4] = [
    "conversation",
    "user_instruction",
    "execution_result",
    "external_reference",
];
const DEFAULT_LIMIT: usize = 10;
const DEFAULT_THRESHOLD: f32 = 0.34;

/// A protocol-level failure, rendered as the contract error envelope.
struct Failure {
    code: &'static str,
    message: String,
}

impl Failure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let db_path = match database_path() {
        Ok(path) => path,
        Err(message) => {
            eprintln!("memory-stdio: {message}");
            std::process::exit(2);
        }
    };

    let mut stdout = std::io::stdout().lock();
    let mut storage = None;

    // Contract v0.1 is deliberately one request per process invocation: only the
    // first request frame is read, and a second request is never executed.
    let line = match read_request() {
        Ok(line) => line,
        Err(error) => {
            eprintln!("memory-stdio: stdin read failed: {error}");
            std::process::exit(1);
        }
    };

    let response = if line.trim().is_empty() {
        error_response(
            "",
            &Failure::new("INVALID_REQUEST", "one request line is required"),
        )
    } else {
        match serde_json::from_str::<Value>(&line) {
            Ok(request) => handle(request, &db_path, &mut storage).await,
            Err(error) => error_response(
                "",
                &Failure::new(
                    "INVALID_REQUEST",
                    format!("request is not valid JSON: {error}"),
                ),
            ),
        }
    };

    let rendered = serde_json::to_string(&response).unwrap_or_else(|error| {
        eprintln!("memory-stdio: response serialization failed: {error}");
        String::from(
            r#"{"contract_version":"0.1","request_id":"","ok":false,"error":{"code":"SERIALIZATION_ERROR","message":"response serialization failed","retryable":false}}"#,
        )
    });

    if writeln!(stdout, "{rendered}")
        .and_then(|()| stdout.flush())
        .is_err()
    {
        eprintln!("memory-stdio: stdout write failed");
        std::process::exit(2);
    }
}

/// Reads exactly one request frame.
///
/// A newline terminates the request. EOF also terminates a
/// final non-empty line. This function deliberately does not
/// wait for EOF after a complete newline-terminated request.
fn read_request() -> std::io::Result<String> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();

    reader.read_line(&mut line)?;

    Ok(line)
}

/// Resolves the SQLite path: `--db <path>` wins, `MA_SQLITE_PATH` is the fallback.
///
/// The contract requires an absolute path so the working directory can never
/// silently decide where memory lands.
fn database_path() -> Result<String, String> {
    let mut args = std::env::args().skip(1);
    let mut from_flag = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                from_flag = Some(
                    args.next()
                        .ok_or_else(|| "--db requires a path argument".to_owned())?,
                );
            }
            other if other.starts_with("--db=") => {
                from_flag = Some(other["--db=".len()..].to_owned());
            }
            other => return Err(format!("unrecognized argument: {other}")),
        }
    }

    let path = match from_flag {
        Some(path) => path,
        None => std::env::var("MA_SQLITE_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "--db <path> is required (or set MA_SQLITE_PATH)".to_owned())?,
    };

    if !std::path::Path::new(&path).is_absolute() {
        return Err(format!("database path must be absolute: {path}"));
    }
    Ok(path)
}

fn ok_response(request_id: &str, result: Value) -> Value {
    json!({
        "contract_version": CONTRACT_VERSION,
        "request_id": request_id,
        "ok": true,
        "result": result,
    })
}

fn error_response(request_id: &str, failure: &Failure) -> Value {
    json!({
        "contract_version": CONTRACT_VERSION,
        "request_id": request_id,
        "ok": false,
        "error": {
            "code": failure.code,
            "message": failure.message,
            "retryable": is_retryable(failure.code),
        },
    })
}

/// Contract §11: retryability follows the actual error condition, not a blanket
/// rule. Only transient transport failures are retryable; a storage error is not
/// automatically retryable.
fn is_retryable(code: &str) -> bool {
    matches!(code, "ADAPTER_UNAVAILABLE" | "ADAPTER_TIMEOUT")
}

async fn handle(request: Value, db_path: &str, storage: &mut Option<SqliteStorage>) -> Value {
    let Some(object) = request.as_object() else {
        return error_response(
            "",
            &Failure::new("INVALID_REQUEST", "request must be a JSON object"),
        );
    };
    let request_id = object
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let Some(version) = object.get("contract_version").and_then(Value::as_str) else {
        return error_response(
            request_id,
            &Failure::new(
                "INVALID_REQUEST",
                "contract_version is required and must be a string",
            ),
        );
    };
    if version != CONTRACT_VERSION {
        return error_response(
            request_id,
            &Failure::new(
                "UNSUPPORTED_VERSION",
                format!("unsupported contract_version: {version}"),
            ),
        );
    }
    if request_id.trim().is_empty() {
        return error_response(
            "",
            &Failure::new(
                "INVALID_REQUEST",
                "request_id is required and must be a non-empty string",
            ),
        );
    }

    let Some(operation) = object.get("operation").and_then(Value::as_str) else {
        return error_response(
            request_id,
            &Failure::new(
                "INVALID_REQUEST",
                "operation is required and must be a string",
            ),
        );
    };
    if operation != "store" && operation != "retrieve" {
        return error_response(
            request_id,
            &Failure::new(
                "INVALID_OPERATION",
                format!("unsupported operation: {operation}"),
            ),
        );
    }

    let Some(payload) = object.get("payload").and_then(Value::as_object) else {
        return error_response(
            request_id,
            &Failure::new(
                "INVALID_REQUEST",
                "payload is required and must be a JSON object",
            ),
        );
    };

    let result = if operation == "store" {
        store(payload, db_path, storage).await
    } else {
        retrieve(payload, db_path, storage).await
    };

    match result {
        Ok(result) => ok_response(request_id, result),
        Err(failure) => error_response(request_id, &failure),
    }
}

async fn store(
    payload: &Map<String, Value>,
    db_path: &str,
    slot: &mut Option<SqliteStorage>,
) -> Result<Value, Failure> {
    let work_item_id = work_item_scope(payload)?;
    let session_id = required_str(payload, "session_id")?;
    let content = match payload.get("content") {
        Some(Value::String(text)) if !text.trim().is_empty() => text.clone(),
        Some(Value::String(_)) => {
            return Err(Failure::new("INVALID_CONTENT", "content must not be empty"))
        }
        None => return Err(Failure::new("INVALID_CONTENT", "content is required")),
        Some(_) => return Err(Failure::new("INVALID_CONTENT", "content must be a string")),
    };
    let provenance = provenance(payload)?;

    let mut metadata = match payload.get("metadata") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(Failure::new(
                "INVALID_REQUEST",
                "metadata must be a JSON object",
            ))
        }
    };
    if metadata.contains_key(PROVENANCE_KEY) {
        return Err(Failure::new(
            "INVALID_PROVENANCE",
            format!("metadata key {PROVENANCE_KEY} is reserved by the contract"),
        ));
    }
    metadata.insert(
        PROVENANCE_KEY.to_owned(),
        json!({ "provenance": provenance }),
    );

    let now = Utc::now();
    let entry = MemoryEntry {
        id: Uuid::new_v4(),
        session_id,
        work_item_id: Some(work_item_id),
        content,
        embedding: None,
        metadata: Value::Object(metadata),
        created_at: now,
        access_count: 0,
        last_accessed: now,
    };

    let storage = open_storage(db_path, slot).await?;
    let memory_id = storage.store(entry).await.map_err(storage_failure)?;

    Ok(json!({ "memory_id": memory_id.to_string() }))
}

async fn retrieve(
    payload: &Map<String, Value>,
    db_path: &str,
    slot: &mut Option<SqliteStorage>,
) -> Result<Value, Failure> {
    let work_item_id = work_item_scope(payload)?;
    let text = match payload.get("query") {
        Some(Value::String(text)) if !text.trim().is_empty() => text.clone(),
        Some(Value::String(_)) => {
            return Err(Failure::new("INVALID_REQUEST", "query must not be empty"))
        }
        None => return Err(Failure::new("INVALID_REQUEST", "query is required")),
        Some(_) => return Err(Failure::new("INVALID_REQUEST", "query must be a string")),
    };
    // Contract §9: session_id is optional, and an absent session_id means
    // cross-Session retrieval inside the WorkItem. A present session_id therefore
    // narrows retrieval to that Session. (The §10 example is internally
    // inconsistent on this point; §9 prose, the SO client template and the
    // required "Session filter" test all agree on narrowing.)
    let session_id = match payload.get("session_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) if !id.trim().is_empty() => Some(id.clone()),
        Some(_) => {
            return Err(Failure::new(
                "INVALID_REQUEST",
                "session_id must be a non-empty string when provided",
            ))
        }
    };
    let limit = match payload.get("limit") {
        None | Some(Value::Null) => DEFAULT_LIMIT,
        Some(value) => match value
            .as_u64()
            .and_then(|limit| usize::try_from(limit).ok())
            .filter(|limit| *limit > 0)
        {
            Some(limit) => limit,
            None => {
                return Err(Failure::new(
                    "INVALID_REQUEST",
                    "limit must be a positive integer",
                ))
            }
        },
    };
    let threshold = match payload.get("threshold") {
        None | Some(Value::Null) => DEFAULT_THRESHOLD,
        Some(value) => match value.as_f64() {
            Some(threshold) if threshold.is_finite() && (0.0..=1.0).contains(&threshold) => {
                threshold as f32
            }
            _ => {
                return Err(Failure::new(
                    "INVALID_REQUEST",
                    "threshold must be a finite number between 0 and 1",
                ))
            }
        },
    };

    // Optional output budget. Absent means "no truncation", which is exactly the
    // pre-budget behaviour, so existing callers keep their results. Charging goes
    // through the same `CharBudget` as context assembly so both agree on what fits.
    let max_chars = match payload.get("max_chars") {
        None | Some(Value::Null) => usize::MAX,
        Some(value) => match value
            .as_u64()
            .and_then(|max| usize::try_from(max).ok())
            .filter(|max| *max > 0)
        {
            Some(max) => max,
            None => {
                return Err(Failure::new(
                    "INVALID_REQUEST",
                    "max_chars must be a positive integer",
                ))
            }
        },
    };
    let mut budget = CharBudget::new(max_chars);

    let query = Query {
        text: text.clone(),
        session_id,
        work_item_id: Some(work_item_id.clone()),
        limit,
        threshold,
    };

    let storage = open_storage(db_path, slot).await?;
    let entries = storage.query(&query).await.map_err(storage_failure)?;

    let query_tokens = tokenize(&text);
    let mut rendered = Vec::with_capacity(entries.len());
    let mut entries_omitted = 0;
    for entry in entries {
        // Fail closed: never return an entry that escaped the requested scope.
        if entry.work_item_id.as_deref() != Some(work_item_id.as_str()) {
            return Err(Failure::new(
                "SCOPE_VIOLATION",
                format!(
                    "entry {} is outside work_item_id {}",
                    entry.id, work_item_id
                ),
            ));
        }
        let score = lexical_score(&query_tokens, &entry.content);
        let MemoryEntry {
            id,
            session_id,
            content,
            metadata,
            created_at,
            ..
        } = entry;
        // Budget is measured on returned content characters, not on JSON bytes.
        if !budget.admit(content.chars().count()) {
            entries_omitted += 1;
            continue;
        }
        rendered.push(json!({
            "memory_id": id.to_string(),
            "work_item_id": work_item_id,
            "session_id": session_id,
            "content": content,
            "provenance": metadata
                .pointer("/_so_contract/provenance")
                .filter(|value| value.is_object())
                .cloned()
                // Contract §10: `provenance` is an object. A record written
                // outside this binary carries no SO provenance; `{}` keeps the
                // field's shape instead of leaking a `null`.
                .unwrap_or_else(|| json!({})),
            "created_at": created_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            "score": score,
        }));
    }

    let entries_included = rendered.len();
    Ok(json!({
        "entries": rendered,
        "entries_included": entries_included,
        "entries_omitted": entries_omitted,
    }))
}

fn work_item_scope(payload: &Map<String, Value>) -> Result<String, Failure> {
    match payload.get("work_item_id") {
        None | Some(Value::Null) => Err(Failure::new("INVALID_SCOPE", "work_item_id is required")),
        Some(Value::String(id)) if id.trim().is_empty() => Err(Failure::new(
            "INVALID_SCOPE",
            "work_item_id must not be empty",
        )),
        Some(Value::String(id)) if is_work_item_id(id) => Ok(id.clone()),
        Some(Value::String(id)) => Err(Failure::new(
            "INVALID_ID",
            format!("work_item_id must match wi_<uuidv4>: {id}"),
        )),
        Some(_) => Err(Failure::new("INVALID_ID", "work_item_id must be a string")),
    }
}

/// Contract §3 fixes one canonical form: `wi_<UUIDv4>` in lowercase hyphenated
/// 8-4-4-4-12 notation. `Uuid::parse_str` alone is too permissive — it also
/// accepts braced, `urn:uuid:`-prefixed, uppercase and unhyphenated input.
fn is_work_item_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("wi_") else {
        return false;
    };
    let bytes = rest.as_bytes();
    bytes.len() == 36
        && bytes[8] == b'-'
        && bytes[13] == b'-'
        && bytes[18] == b'-'
        && bytes[23] == b'-'
        && rest
            .chars()
            .all(|c| c.is_ascii_digit() || c == '-' || c.is_ascii_lowercase())
        && Uuid::parse_str(rest).is_ok_and(|id| id.get_version_num() == 4)
}

fn required_str(payload: &Map<String, Value>, field: &str) -> Result<String, Failure> {
    match payload.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(Failure::new(
            "INVALID_REQUEST",
            format!("{field} is required and must be a non-empty string"),
        )),
    }
}

fn provenance(payload: &Map<String, Value>) -> Result<Value, Failure> {
    let Some(Value::Object(provenance)) = payload.get("provenance") else {
        return Err(Failure::new(
            "INVALID_PROVENANCE",
            "provenance is required and must be a JSON object",
        ));
    };

    let source_type = match provenance.get("source_type").and_then(Value::as_str) {
        Some(value) if SOURCE_TYPES.contains(&value) => value,
        Some(value) => {
            return Err(Failure::new(
                "INVALID_PROVENANCE",
                format!("unsupported source_type: {value}"),
            ))
        }
        None => {
            return Err(Failure::new(
                "INVALID_PROVENANCE",
                "provenance.source_type is required",
            ))
        }
    };
    let source_id = match provenance.get("source_id").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value,
        _ => {
            return Err(Failure::new(
                "INVALID_PROVENANCE",
                "provenance.source_id is required",
            ))
        }
    };
    let captured_at = match provenance.get("captured_at").and_then(Value::as_str) {
        Some(value) if chrono::DateTime::parse_from_rfc3339(value).is_ok() => value,
        _ => {
            return Err(Failure::new(
                "INVALID_PROVENANCE",
                "provenance.captured_at must be an RFC3339 timestamp",
            ))
        }
    };

    Ok(json!({
        "source_type": source_type,
        "source_id": source_id,
        "captured_at": captured_at,
    }))
}

async fn open_storage<'a>(
    db_path: &str,
    slot: &'a mut Option<SqliteStorage>,
) -> Result<&'a SqliteStorage, Failure> {
    if slot.is_none() {
        let opened = SqliteStorage::open(db_path)
            .await
            .map_err(storage_failure)?;
        *slot = Some(opened);
    }
    slot.as_ref()
        .ok_or_else(|| Failure::new("INTERNAL_ERROR", "storage handle unavailable"))
}

fn storage_failure(error: AdapterError) -> Failure {
    Failure::new("STORAGE_ERROR", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_is_decided_per_error_code() {
        assert!(is_retryable("ADAPTER_UNAVAILABLE"));
        assert!(is_retryable("ADAPTER_TIMEOUT"));
        for code in [
            "ADAPTER_CRASHED",
            "STORAGE_ERROR",
            "INVALID_REQUEST",
            "INVALID_SCOPE",
            "SCOPE_VIOLATION",
            "INTERNAL_ERROR",
        ] {
            assert!(!is_retryable(code), "{code} must not be retryable");
        }
    }

    #[test]
    fn error_envelope_carries_condition_derived_retryable() {
        let transient = error_response("r1", &Failure::new("ADAPTER_TIMEOUT", "slow"));
        assert_eq!(transient["error"]["retryable"], Value::Bool(true));
        let permanent = error_response("r1", &Failure::new("STORAGE_ERROR", "disk"));
        assert_eq!(permanent["error"]["retryable"], Value::Bool(false));
    }
}
