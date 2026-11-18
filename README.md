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
