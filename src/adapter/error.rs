//! Adapter error definitions.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Vector backend error: {0}")]
    Vector(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type AdapterResult<T> = std::result::Result<T, AdapterError>;
