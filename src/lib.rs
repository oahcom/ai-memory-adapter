//! AI Memory Adapter
//!
//! Core memory abstraction for the AI Brain project.

pub mod adapter;
pub mod context;
pub mod storage;

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support_api;
