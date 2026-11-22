//! Test-only support for fault injection and synchronization.
//! Only compiled during `cargo test` or with the `test-support` feature.
#![cfg_attr(all(feature = "test-support", not(test)), allow(dead_code))]

use std::sync::{Arc, Mutex};
use uuid::Uuid;

type SchemaMigrationHook = Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>;
static SCHEMA_MIGRATION_TEST_HOOK: Mutex<Option<SchemaMigrationHook>> = Mutex::new(None);
type SchemaMigrationIndexHook = Arc<dyn Fn(&str, bool) -> Result<(), String> + Send + Sync>;
static SCHEMA_MIGRATION_INDEX_TEST_HOOK: Mutex<Option<SchemaMigrationIndexHook>> = Mutex::new(None);

/// Get the schema migration hook that observes in-transaction index visibility.
pub(crate) fn schema_migration_index_hook() -> Option<SchemaMigrationIndexHook> {
    SCHEMA_MIGRATION_INDEX_TEST_HOOK.lock().unwrap().clone()
}

/// Set the schema migration index hook. Returns a guard that clears it on drop.
pub(crate) fn set_schema_migration_index_hook(
    hook: SchemaMigrationIndexHook,
) -> SchemaMigrationIndexHookGuard {
    *SCHEMA_MIGRATION_INDEX_TEST_HOOK.lock().unwrap() = Some(hook);
    SchemaMigrationIndexHookGuard
}

pub(crate) struct SchemaMigrationIndexHookGuard;

impl Drop for SchemaMigrationIndexHookGuard {
    fn drop(&mut self) {
        *SCHEMA_MIGRATION_INDEX_TEST_HOOK.lock().unwrap() = None;
    }
}

/// Get the current schema migration hook.
pub(crate) fn schema_migration_hook() -> Option<SchemaMigrationHook> {
    SCHEMA_MIGRATION_TEST_HOOK.lock().unwrap().clone()
}

/// Set the schema migration hook. Returns a guard that clears it on drop.
pub(crate) fn set_schema_migration_hook(hook: SchemaMigrationHook) -> SchemaMigrationHookGuard {
    *SCHEMA_MIGRATION_TEST_HOOK.lock().unwrap() = Some(hook);
    SchemaMigrationHookGuard
}

pub(crate) struct SchemaMigrationHookGuard;

impl Drop for SchemaMigrationHookGuard {
    fn drop(&mut self) {
        *SCHEMA_MIGRATION_TEST_HOOK.lock().unwrap() = None;
    }
}

/// Test-only observer for query() candidate fetch count.
pub(crate) static LAST_QUERY_CANDIDATE_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

static LAST_QUERY_CANDIDATE_IDS: Mutex<Vec<Uuid>> = Mutex::new(Vec::new());

/// Record candidate ids selected by query().
pub(crate) fn record_query_candidate_ids(ids: Vec<Uuid>) {
    *LAST_QUERY_CANDIDATE_IDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = ids;
}

/// Read back the ids recorded by the most recent query().
#[cfg(feature = "test-support")]
pub(crate) fn last_query_candidate_ids() -> Vec<Uuid> {
    LAST_QUERY_CANDIDATE_IDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}
