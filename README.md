# AI Memory Adapter

🦀 **Rust-based** memory layer for AI Brain Architecture project.

## Problem

ChatGPT sessions have no persistent memory → context fragmentation → frequent restarts.

## Solution

```
Memory Adapter (Interface)
├── Storage (SQLite + Vector DB)
├── Context Injection (Smart context loading)
└── Pruning (Ebbinghaus forgetting curve)
```

## Architecture

```rust
pub trait MemoryAdapter {
    async fn store(&self, entry: MemoryEntry) -> Result<EntryId>;
    async fn query(&self, query: &Query) -> Result<Vec<MemoryEntry>>;
    async fn inject_context(&self, session_id: &str) -> Result<Context>;
    async fn prune(&self, retention_policy: &RetentionPolicy) -> Result<PruneReport>;
}
```

## Development

Developed by ChatGPT on remote server `43.138.133.238` following AI-STD-001 spec.

## Tech Stack

- **Language**: Rust 2021 edition
- **Database**: SQLx + SQLite
- **Vector DB**: Qdrant (default) / FAISS (optional)
- **Async Runtime**: Tokio

## Quick Start

```bash
cargo build --release
cargo test
./target/release/memory-adapter
```

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
