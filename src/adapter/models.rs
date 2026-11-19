//! Memory data models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::error::{AdapterError, AdapterResult};

/// A persistent memory entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryEntry {
    pub id: Uuid,
    pub session_id: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub access_count: i32,
    pub last_accessed: DateTime<Utc>,
}

impl MemoryEntry {
    /// Validate before persistence.
    pub fn validate(&self) -> AdapterResult<()> {
        if self.session_id.trim().is_empty() {
            return Err(AdapterError::Validation(
                "session_id must not be empty".into(),
            ));
        }
        if self.content.trim().is_empty() {
            return Err(AdapterError::Validation("content must not be empty".into()));
        }
        if self.access_count < 0 {
            return Err(AdapterError::Validation(
                "access_count must not be negative".into(),
            ));
        }
        if let Some(embedding) = &self.embedding {
            if embedding.is_empty() {
                return Err(AdapterError::Validation(
                    "embedding must not be empty when provided".into(),
                ));
            }
            if embedding.iter().any(|value| !value.is_finite()) {
                return Err(AdapterError::Validation(
                    "embedding must contain only finite values".into(),
                ));
            }
        }
        if self.last_accessed < self.created_at {
            return Err(AdapterError::Validation(
                "last_accessed must not precede created_at".into(),
            ));
        }
        Ok(())
    }
}

/// Memory retrieval query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Query {
    pub text: String,
    pub session_id: Option<String>,
    pub limit: usize,
    pub threshold: f32,
}

impl Query {
    /// Validate retrieval parameters.
    pub fn validate(&self) -> AdapterResult<()> {
        if self.text.trim().is_empty() {
            return Err(AdapterError::Validation(
                "query text must not be empty".into(),
            ));
        }
        if self
            .session_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
        {
            return Err(AdapterError::Validation(
                "session_id must not be empty when provided".into(),
            ));
        }
        if self.limit == 0 {
            return Err(AdapterError::Validation(
                "query limit must be greater than zero".into(),
            ));
        }
        if !self.threshold.is_finite() || !(0.0..=1.0).contains(&self.threshold) {
            return Err(AdapterError::Validation(
                "threshold must be finite and between 0.0 and 1.0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_entry() -> MemoryEntry {
        let now = Utc::now();
        MemoryEntry {
            id: Uuid::new_v4(),
            session_id: "session-001".into(),
            content: "Rust memory adapter".into(),
            embedding: Some(vec![0.1, 0.2, 0.3]),
            metadata: json!({"source": "test"}),
            created_at: now,
            access_count: 0,
            last_accessed: now,
        }
    }

    #[test]
    fn valid_memory_entry_passes() {
        assert!(sample_entry().validate().is_ok());
    }

    #[test]
    fn empty_session_id_fails() {
        let mut entry = sample_entry();
        entry.session_id = " ".into();
        assert!(matches!(entry.validate(), Err(AdapterError::Validation(_))));
    }

    #[test]
    fn negative_access_count_fails() {
        let mut entry = sample_entry();
        entry.access_count = -1;
        assert!(matches!(entry.validate(), Err(AdapterError::Validation(_))));
    }

    #[test]
    fn non_finite_embedding_fails() {
        let mut entry = sample_entry();
        entry.embedding = Some(vec![0.1, f32::NAN]);
        assert!(matches!(entry.validate(), Err(AdapterError::Validation(_))));
    }

    #[test]
    fn last_access_before_creation_fails() {
        let mut entry = sample_entry();
        entry.last_accessed = entry.created_at - chrono::Duration::seconds(1);
        assert!(matches!(entry.validate(), Err(AdapterError::Validation(_))));
    }

    #[test]
    fn valid_query_passes() {
        let query = Query {
            text: "Rust".into(),
            session_id: Some("session-001".into()),
            limit: 10,
            threshold: 0.75,
        };
        assert!(query.validate().is_ok());
    }

    #[test]
    fn zero_limit_fails() {
        let query = Query {
            text: "Rust".into(),
            session_id: None,
            limit: 0,
            threshold: 0.5,
        };
        assert!(query.validate().is_err());
    }

    #[test]
    fn invalid_threshold_fails() {
        let query = Query {
            text: "Rust".into(),
            session_id: None,
            limit: 10,
            threshold: f32::NAN,
        };
        assert!(query.validate().is_err());
    }
}
