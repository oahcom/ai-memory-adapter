# AI Memory Adapter

🦀 **Rust-based** memory layer for AI Brain Architecture project.

## Problem

ChatGPT sessions have no persistent memory → context fragmentation → frequent restarts.

## Solution

```
Memory Adapter (Interface)
├── Storage (SQLite + Vector DB)
└── Context Injection (Smart context loading)
```

## Architecture

```rust
pub trait MemoryAdapter {
    async fn store(&self, entry: MemoryEntry) -> Result<EntryId>;
    async fn query(&self, query: &Query) -> Result<Vec<MemoryEntry>>;
    async fn inject_context(&self, session_id: &str) -> Result<Context>;
}
```

## Development

Developed by ChatGPT on remote server `43.138.133.238` following AI-STD-001 spec.

## Tech Stack

- **Language**: Rust 2021 edition
- **Database**: SQLx + SQLite
- **Vector DB**: none by default — SQLite is the only default backend; `qdrant` and `faiss-backend` are explicit opt-in Cargo features (`default = []`)
- **Async Runtime**: Tokio

## Quick Start

```bash
cargo build --release --bin memory-stdio
cargo test
./target/release/memory-stdio --db /absolute/path/memory.sqlite3
```

The v0.1 contract is one request per process invocation: one JSON request line
on stdin, exactly one JSON response line on stdout, diagnostics on stderr only.
The client does not have to close stdin; extra non-empty input is rejected with
`INVALID_REQUEST` before any operation runs.

## Work Item scoped retrieval

A memory may optionally carry a stable `work_item_id` in addition to its
session identifier. Use the Work Item scope when related work continues across
multiple ChatGPT sessions:

```rust
let results = adapter.query(&Query {
    text: "migration design".into(),
    session_id: None,
    work_item_id: Some("WI-123".into()),
    limit: 10,
    threshold: 0.5,
}).await?;

let context = injector.inject_work_item("WI-123", "migration design").await?;
```

When both `work_item_id` and `session_id` are supplied, both filters apply.
Existing databases are migrated with a nullable column; legacy records remain
unscoped and are not returned by a named Work Item query. An omitted Work Item
scope preserves the pre-existing session/global query behavior.

**Rust source compatibility:** this crate is currently pre-1.0 (`0.x`). Adding public fields to `MemoryEntry` and `Query` is source-breaking for downstream code that constructs these structs with exhaustive struct literals; those callers must add `work_item_id`. Serde defaults preserve compatibility with older serialized data, not Rust source code.
