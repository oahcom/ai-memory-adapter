//! Query relevant memories and assemble a bounded context.

use crate::adapter::{AdapterError, AdapterResult, MemoryAdapter, Query};

/// Configuration for context retrieval and assembly.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextConfig {
    /// Maximum number of candidate memories requested from the adapter.
    pub max_entries: usize,
    /// Minimum retrieval score, in the range [0.0, 1.0].
    pub min_score: f32,
    /// Maximum output size measured in Unicode characters, not tokens.
    pub max_chars: usize,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            max_entries: 10,
            min_score: 0.25,
            max_chars: 8_000,
        }
    }
}

impl ContextConfig {
    fn validate(&self) -> AdapterResult<()> {
        if self.max_entries == 0 {
            return Err(AdapterError::Validation(
                "max_entries must be greater than zero".into(),
            ));
        }
        if !self.min_score.is_finite() || !(0.0..=1.0).contains(&self.min_score) {
            return Err(AdapterError::Validation(
                "min_score must be finite and between 0.0 and 1.0".into(),
            ));
        }
        if self.max_chars == 0 {
            return Err(AdapterError::Validation(
                "max_chars must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Result of context assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextResult {
    /// Formatted context ready for injection.
    pub text: String,
    /// Number of memories included in the output.
    pub entries_included: usize,
    /// Number of retrieved memories omitted because of the character budget.
    pub entries_omitted: usize,
    /// Actual output size measured in Unicode characters.
    pub chars_used: usize,
}

/// Builds a bounded context from an existing memory adapter.
pub struct ContextInjector<'a> {
    adapter: &'a dyn MemoryAdapter,
    config: ContextConfig,
}

impl<'a> ContextInjector<'a> {
    /// Create an injector after validating its configuration.
    pub fn new(adapter: &'a dyn MemoryAdapter, config: ContextConfig) -> AdapterResult<Self> {
        config.validate()?;
        Ok(Self { adapter, config })
    }

    /// Retrieve relevant memories for one session and assemble context.
    pub async fn inject(&self, session_id: &str, query_text: &str) -> AdapterResult<ContextResult> {
        if session_id.trim().is_empty() {
            return Err(AdapterError::Validation(
                "session_id must not be empty".into(),
            ));
        }
        if query_text.trim().is_empty() {
            return Err(AdapterError::Validation(
                "query_text must not be empty".into(),
            ));
        }

        let query = Query {
            text: query_text.to_owned(),
            session_id: Some(session_id.to_owned()),
            work_item_id: None,
            limit: self.config.max_entries,
            threshold: self.config.min_score,
        };

        let memories = self.adapter.query(&query).await?;

        let mut text = String::new();
        let mut entries_included = 0;
        let mut entries_omitted = 0;
        let mut chars_used = 0;

        for memory in memories {
            let line = format!("[{}] {}", memory.created_at.to_rfc3339(), memory.content);
            let line_chars = line.chars().count();
            let separator_chars = if text.is_empty() { 0 } else { 1 };
            let required_chars = line_chars.saturating_add(separator_chars);

            if required_chars > self.config.max_chars.saturating_sub(chars_used) {
                entries_omitted += 1;
                continue;
            }

            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&line);
            chars_used += required_chars;
            entries_included += 1;
        }

        Ok(ContextResult {
            text,
            entries_included,
            entries_omitted,
            chars_used,
        })
    }
    /// Retrieve relevant memories across sessions, restricted to one work item.
    pub async fn inject_work_item(
        &self,
        work_item_id: &str,
        query_text: &str,
    ) -> AdapterResult<ContextResult> {
        if work_item_id.trim().is_empty() || query_text.trim().is_empty() {
            return Err(AdapterError::Validation(
                "work_item_id and query_text must not be empty".into(),
            ));
        }
        let query = Query {
            text: query_text.to_owned(),
            session_id: None,
            work_item_id: Some(work_item_id.to_owned()),
            limit: self.config.max_entries,
            threshold: self.config.min_score,
        };
        let memories = self.adapter.query(&query).await?;
        let mut text = String::new();
        let mut entries_included = 0;
        let mut entries_omitted = 0;
        let mut chars_used = 0;
        for memory in memories {
            // Fail closed if an adapter violates the requested scope. Do not
            // label an out-of-scope memory with the caller's Work Item ID.
            if memory.work_item_id.as_deref() != Some(work_item_id) {
                return Err(AdapterError::Storage(format!(
                    "adapter returned memory {} outside requested Work Item scope {}",
                    memory.id, work_item_id
                )));
            }
            let line = format!(
                "[memory_id={} work_item_id={} session_id={} created_at={}] {}",
                memory.id,
                memory.work_item_id.as_deref().unwrap_or_default(),
                memory.session_id,
                memory.created_at.to_rfc3339(),
                memory.content
            );
            let separator_chars = if text.is_empty() { 0 } else { 1 };
            let required_chars = line.chars().count().saturating_add(separator_chars);
            if required_chars > self.config.max_chars.saturating_sub(chars_used) {
                entries_omitted += 1;
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&line);
            chars_used += required_chars;
            entries_included += 1;
        }
        Ok(ContextResult {
            text,
            entries_included,
            entries_omitted,
            chars_used,
        })
    }
}
