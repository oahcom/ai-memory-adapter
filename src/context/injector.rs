//! Query relevant memories and assemble a bounded context.

use crate::adapter::{AdapterError, AdapterResult, MemoryAdapter, Query};
use serde_json::Value;

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
            min_score: 0.34,
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

/// Character budget shared by context assembly and the stdio renderer.
///
/// An entry is admitted whole or omitted; content is never truncated. Measuring
/// in Unicode characters keeps the limit meaningful for non-ASCII content.
#[derive(Debug, Clone)]
pub struct CharBudget {
    max_chars: usize,
    used: usize,
}

impl CharBudget {
    pub fn new(max_chars: usize) -> Self {
        Self { max_chars, used: 0 }
    }

    /// Charges `required_chars` and reports whether the entry fits.
    pub fn admit(&mut self, required_chars: usize) -> bool {
        if required_chars > self.max_chars.saturating_sub(self.used) {
            return false;
        }
        self.used += required_chars;
        true
    }

    /// Characters charged so far.
    pub fn used(&self) -> usize {
        self.used
    }
}

/// Result of context assembly.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextResult {
    /// Formatted context ready for injection.
    pub text: String,
    /// Number of memories included in the output.
    pub entries_included: usize,
    /// Number of retrieved memories omitted because of the character budget.
    pub entries_omitted: usize,
    /// Actual output size measured in Unicode characters.
    pub chars_used: usize,
    /// SO provenance of each included entry, index-aligned with `text` records.
    /// `None` means the entry carries no `_so_contract.provenance`.
    pub provenance: Vec<Option<Value>>,
}

// Encode untrusted metadata fields so structural delimiters cannot be injected.
pub(crate) fn escape_context_field(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            escaped.push(ch);
        } else {
            escaped.push_str(&format!("\\u{{{:x}}}", ch as u32));
        }
    }
    escaped
}

// Escape control characters and backslashes so content cannot create another
// physical record or forge a literal escape sequence.
pub(crate) fn escape_context_content(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '[' | ']' => escaped.push_str(&format!("\\u{{{:x}}}", ch as u32)),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ if ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}') => {
                escaped.push_str(&format!("\\u{{{:x}}}", ch as u32));
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Renders the SO provenance of one memory for the context header.
///
/// A missing provenance renders as the literal `none`, never as an empty
/// string: an empty value would be indistinguishable from a missing one.
fn provenance_field(metadata: &Value) -> String {
    match metadata.pointer("/_so_contract/provenance") {
        Some(value) => escape_context_field(&value.to_string()),
        None => "none".to_owned(),
    }
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
        let mut budget = CharBudget::new(self.config.max_chars);
        let mut provenance = Vec::new();

        for memory in memories {
            let escaped_content = escape_context_content(&memory.content);
            let line = format!("[{}] {}", memory.created_at.to_rfc3339(), escaped_content);
            let line_chars = line.chars().count();
            let separator_chars = if text.is_empty() { 0 } else { 1 };
            let required_chars = line_chars.saturating_add(separator_chars);

            if !budget.admit(required_chars) {
                entries_omitted += 1;
                continue;
            }

            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&line);
            entries_included += 1;
            provenance.push(memory.metadata.pointer("/_so_contract/provenance").cloned());
        }

        Ok(ContextResult {
            text,
            entries_included,
            entries_omitted,
            chars_used: budget.used(),
            provenance,
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
        let mut budget = CharBudget::new(self.config.max_chars);
        let mut provenance = Vec::new();
        for memory in memories {
            // Fail closed if an adapter violates the requested scope. Do not
            // label an out-of-scope memory with the caller's Work Item ID.
            if memory.work_item_id.as_deref() != Some(work_item_id) {
                return Err(AdapterError::Storage(format!(
                    "adapter returned memory {} outside requested Work Item scope {}",
                    memory.id, work_item_id
                )));
            }
            let escaped_work_item_id = escape_context_field(work_item_id);
            let escaped_session_id = escape_context_field(&memory.session_id);
            let escaped_content = escape_context_content(&memory.content);
            let line = format!(
                "[memory_id={} work_item_id={} session_id={} created_at={} provenance={}] {}",
                memory.id,
                escaped_work_item_id,
                escaped_session_id,
                memory.created_at.to_rfc3339(),
                provenance_field(&memory.metadata),
                escaped_content
            );
            let separator_chars = if text.is_empty() { 0 } else { 1 };
            let required_chars = line.chars().count().saturating_add(separator_chars);
            if !budget.admit(required_chars) {
                entries_omitted += 1;
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&line);
            entries_included += 1;
            provenance.push(memory.metadata.pointer("/_so_contract/provenance").cloned());
        }
        Ok(ContextResult {
            text,
            entries_included,
            entries_omitted,
            chars_used: budget.used(),
            provenance,
        })
    }
}
