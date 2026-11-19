//! Ebbinghaus forgetting-curve retention policy.

use chrono::{DateTime, Utc};

use crate::adapter::{AdapterError, AdapterResult, MemoryEntry};

const SECONDS_PER_DAY: f64 = 86_400.0;
const LN_2: f64 = std::f64::consts::LN_2;

/// Configurable Ebbinghaus retention policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EbbinghausPolicy {
    /// Base half-life in days.
    pub half_life_days: f64,
    /// Logarithmic weight applied to access frequency.
    pub access_weight: f64,
    /// Entries below this retention score are eligible for deletion.
    pub prune_threshold: f64,
}

impl Default for EbbinghausPolicy {
    fn default() -> Self {
        Self {
            half_life_days: 30.0,
            access_weight: 0.5,
            prune_threshold: 0.1,
        }
    }
}

impl EbbinghausPolicy {
    /// Validate policy parameters.
    pub fn validate(&self) -> AdapterResult<()> {
        if !self.half_life_days.is_finite() || self.half_life_days <= 0.0 {
            return Err(AdapterError::Validation(
                "half_life_days must be finite and greater than zero".into(),
            ));
        }
        if !self.access_weight.is_finite() || self.access_weight < 0.0 {
            return Err(AdapterError::Validation(
                "access_weight must be finite and non-negative".into(),
            ));
        }
        if !self.prune_threshold.is_finite() || !(0.0..=1.0).contains(&self.prune_threshold) {
            return Err(AdapterError::Validation(
                "prune_threshold must be finite and between 0.0 and 1.0".into(),
            ));
        }
        Ok(())
    }

    /// Calculate the retention score for one memory at a given time.
    /// Future last_accessed timestamps are treated as zero elapsed time.
    pub fn retention_score(
        &self,
        entry: &MemoryEntry,
        evaluated_at: DateTime<Utc>,
    ) -> AdapterResult<f64> {
        self.validate()?;
        if entry.access_count < 0 {
            return Err(AdapterError::Validation(
                "access_count must not be negative".into(),
            ));
        }

        let elapsed_seconds = evaluated_at
            .signed_duration_since(entry.last_accessed)
            .num_seconds()
            .max(0) as f64;
        let elapsed_days = elapsed_seconds / SECONDS_PER_DAY;
        let access_factor = 1.0 + self.access_weight * (1.0 + f64::from(entry.access_count)).ln();
        let effective_half_life = self.half_life_days * access_factor;
        let score = (-LN_2 * elapsed_days / effective_half_life).exp();
        Ok(score.clamp(0.0, 1.0))
    }

    /// Return whether a memory is below the configured retention threshold.
    pub fn should_prune(
        &self,
        entry: &MemoryEntry,
        evaluated_at: DateTime<Utc>,
    ) -> AdapterResult<bool> {
        Ok(self.retention_score(entry, evaluated_at)? < self.prune_threshold)
    }
}

/// Summary of one pruning execution.
#[derive(Debug, Clone, PartialEq)]
pub struct PruneReport {
    pub entries_examined: usize,
    pub entries_deleted: usize,
    pub entries_retained: usize,
    pub prune_threshold: f64,
    pub evaluated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;
    use uuid::Uuid;

    fn memory(access_count: i32, last_accessed: DateTime<Utc>) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            session_id: "test-session".into(),
            work_item_id: None,
            content: "test memory".into(),
            embedding: None,
            metadata: json!({}),
            created_at: last_accessed,
            access_count,
            last_accessed,
        }
    }

    #[test]
    fn score_is_half_after_one_base_half_life() {
        let now = Utc::now();
        let entry = memory(0, now - Duration::days(30));
        let score = EbbinghausPolicy::default()
            .retention_score(&entry, now)
            .unwrap();
        assert!((score - 0.5).abs() < 1e-9);
    }

    #[test]
    fn more_accesses_increase_retention() {
        let now = Utc::now();
        let last_accessed = now - Duration::days(60);
        let low_score = EbbinghausPolicy::default()
            .retention_score(&memory(0, last_accessed), now)
            .unwrap();
        let high_score = EbbinghausPolicy::default()
            .retention_score(&memory(20, last_accessed), now)
            .unwrap();
        assert!(high_score > low_score);
    }

    #[test]
    fn future_last_accessed_has_full_retention() {
        let now = Utc::now();
        let entry = memory(0, now + Duration::days(2));
        let score = EbbinghausPolicy::default()
            .retention_score(&entry, now)
            .unwrap();
        assert_eq!(score, 1.0);
    }

    #[test]
    fn invalid_policy_is_rejected() {
        assert!(EbbinghausPolicy {
            half_life_days: 0.0,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(EbbinghausPolicy {
            access_weight: f64::NAN,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(EbbinghausPolicy {
            prune_threshold: 1.5,
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn negative_access_count_is_rejected() {
        let now = Utc::now();
        assert!(EbbinghausPolicy::default()
            .retention_score(&memory(-1, now), now)
            .is_err());
    }
}
