//! Test-only support for fault injection and synchronization.
//! Only compiled during `cargo test` or with the `test-support` feature.
#![cfg_attr(
    all(feature = "test-support", not(test)),
    allow(dead_code)
)]


use std::sync::{Arc, Mutex};
use uuid::Uuid;

type PruneHook = Arc<dyn Fn(usize) -> Result<(), String> + Send + Sync>;

static PRUNE_TEST_HOOK: Mutex<Option<PruneHook>> = Mutex::new(None);

/// Get the current prune hook, if set.
pub(crate) fn prune_hook() -> Option<PruneHook> {
    PRUNE_TEST_HOOK.lock().unwrap().clone()
}

/// Set the prune hook. Returns a guard that clears the hook on drop.
pub(crate) fn set_prune_hook(hook: PruneHook) -> HookGuard {
    *PRUNE_TEST_HOOK.lock().unwrap() = Some(hook);
    HookGuard
}

/// Guard that clears the hook when dropped (including on panic).
pub(crate) struct HookGuard;

impl Drop for HookGuard {
    fn drop(&mut self) {
        *PRUNE_TEST_HOOK.lock().unwrap() = None;
    }
}

/// One-shot synchronization barrier for concurrency tests.
/// The hook signals when it fires; the test waits for that signal,
/// does its work, then allows the hook to proceed.
pub(crate) struct SyncBarrier {
    fired: Arc<Mutex<bool>>,
    proceed: Arc<Mutex<bool>>,
}

impl SyncBarrier {
    pub fn new() -> Self {
        SyncBarrier {
            fired: Arc::new(Mutex::new(false)),
            proceed: Arc::new(Mutex::new(false)),
        }
    }

    /// Returns a hook that signals `fired` and then waits for `proceed`.
    pub fn hook(&self) -> PruneHook {
        let fired = self.fired.clone();
        let proceed = self.proceed.clone();
        Arc::new(move |_batch: usize| {
            *fired.lock().unwrap() = true;
            // Busy-wait for proceed signal (test thread will set it)
            loop {
                if *proceed.lock().unwrap() {
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })
    }

    /// Wait until the hook has fired (with timeout).
    pub fn wait_fired(&self, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if *self.fired.lock().unwrap() {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Signal the hook to proceed.
    pub fn proceed(&self) {
        *self.proceed.lock().unwrap() = true;
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
