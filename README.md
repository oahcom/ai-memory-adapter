# AI Memory Adapter

Rust memory persistence and WorkItem-scoped retrieval for the AI Brain
Architecture project. The adapter is a local process dependency of Session Ops,
not a WorkItem lifecycle manager and not a network service.

## Responsibilities

- Persist memory records in SQLite.
- Retrieve records inside an explicitly supplied WorkItem scope.
- Preserve Session Ops provenance across persistence and retrieval.
- Render bounded context without allowing stored content to forge record
  boundaries or metadata headers.
- Keep Adapter Core independent of transport and optional vector backends.
- Expose the direct Session Ops integration through the local `memory-stdio`
  process boundary.

Session Ops remains authoritative for WorkItem identity, lifecycle, and
session-to-WorkItem association. See
[`docs/integration/session-ops-contract-v0.1.md`](docs/integration/session-ops-contract-v0.1.md)
for the frozen SO–MA v0.1 protocol.

## Runtime requirements

- Rust stable toolchain (edition 2021).
- A writable absolute path for the SQLite database.
- No Qdrant service or external network connection is required by the default
  build.

## Build and test

```bash
cargo fmt --all -- --check
cargo build --release --bin memory-stdio
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

## Run the adapter

The v0.1 contract is **one request per process invocation**. The process reads
one UTF-8 JSON request line from stdin, writes exactly one JSON response line
to stdout, and sends diagnostics only to stderr. `--db` takes precedence over
`MA_SQLITE_PATH`; the selected database path must be absolute.

```bash
cargo build --release --bin memory-stdio
printf '%s\n' '{"contract_version":"0.1","request_id":"req_001","operation":"retrieve","payload":{"work_item_id":"wi_550e8400-e29b-41d4-a716-446655440000","query":"migration design","limit":10,"threshold":0.34}}' \
  | ./target/release/memory-stdio --db /var/lib/ai-memory-adapter/memory.sqlite3
```

Create the database directory before launching the adapter and grant the
service account exclusive write access to it. Do not place the database in a
world-writable directory. Back up the database using a SQLite-consistent backup
procedure; copying only the main `.sqlite3` file while WAL mode is active is
not a safe backup strategy.

## Production-readiness boundary

The v0.1 stdio path is an integration interface, **not an authenticated remote
multi-user service and not production activation**. It filters by WorkItem ID,
but a WorkItem ID is not an authorization credential and the local process
boundary does not establish a remote caller's identity or grants. Do not expose
this binary as a public or shared remote memory endpoint.

The separate first-class Adapter MCP surface and its principal → tenant →
WorkItem authorization model are tracked in
[issue #25](https://github.com/oahcom/ai-memory-adapter/issues/25). That work
must define and enforce verified identity, per-WorkItem grants, bounded
payloads/results, and read/write authorization before remote private-memory
access is considered ready. A separately deployed RMC OAuth endpoint is not,
by itself, evidence that Adapter memory calls have those grants.

Production data access, deployment, and activation remain separate approval
gates; a successful local build or stdio test does not satisfy them.

## WorkItem-scoped retrieval

Every direct Session Ops store and retrieve request must carry a canonical
`wi_<lowercase UUIDv4>` WorkItem ID. A WorkItem may span multiple Sessions;
when `session_id` is also supplied to retrieval, it narrows the result within
that WorkItem. The adapter never infers WorkItem identity from a Session ID.

Legacy records without a WorkItem ID remain unscoped and are not returned by a
WorkItem-scoped request. They are not automatically assigned to a WorkItem.

## Storage and retrieval characteristics

- SQLite is the only default storage backend (`default = []`).
- Qdrant and FAISS dependency feature flags remain in the manifest for future
  work, but no Qdrant/FAISS storage implementation is present in this baseline;
  do not enable or treat them as supported production backends.
- Retrieval uses bounded SQLite candidate selection and lexical relevance
  scoring; it does not promise embedding-based semantic retrieval.
- `request_id` is a correlation identifier, not an idempotency key. A caller
  must not blindly retry a Store whose outcome is uncertain after a timeout.
- The v0.1 stdio protocol does not expose WorkItem lifecycle operations, global
  search, delete/update-by-id, or administrative pruning.
- The Core `Query` API rejects fully unscoped queries; session-only and
  WorkItem-only queries remain supported for their respective legacy and
  cross-session use cases. Session-only filtering is not a substitute for
  verified WorkItem authorization when Session IDs can be reused.

## Compatibility

This crate is pre-1.0. Public struct fields may change between 0.x releases.
Adding `work_item_id` to `MemoryEntry` and `Query` is source-breaking for
callers using exhaustive Rust struct literals. Serde defaults preserve older
serialized data only; they do not preserve Rust source compatibility.
