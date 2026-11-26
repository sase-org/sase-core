//! Wire values and stable errors for question copy metadata.

use std::fmt;

use serde::{Deserialize, Serialize};

pub const QUESTION_COPY_CODE_INVALID: &str = "invalid_question_copy";
pub const QUESTION_COPY_MAX_ENTRIES: usize = 20;
pub const QUESTION_COPY_MAX_TEXT_BYTES: usize = 4096;
pub const QUESTION_COPY_MAX_LABEL_CHARS: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionCopyItemWire {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionCopyDisplayWire {
    pub items: Vec<QuestionCopyItemWire>,
    pub unavailable: usize,
}

/// The conservative source category for an automatically detected copy value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionCopyCandidateKind {
    Code,
    Url,
    Ref,
}

/// One safe, deduplicated copy candidate found in question prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionCopyCandidateWire {
    pub text: String,
    pub kind: QuestionCopyCandidateKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuestionCopyError {
    pub code: String,
    pub entry_index: Option<usize>,
    pub field: String,
    pub message: String,
}

impl QuestionCopyError {
    pub fn new(
        entry_index: Option<usize>,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: QUESTION_COPY_CODE_INVALID.to_string(),
            entry_index,
            field: field.into(),
            message: message.into(),
        }
    }

    pub fn location(&self) -> String {
        match self.entry_index {
            Some(index) => format!("copy[{index}].{}", self.field),
            None if self.field == "copy" => "copy".to_string(),
            None => format!("copy.{}", self.field),
        }
    }
}

impl fmt::Display for QuestionCopyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}: {}",
            self.code,
            self.location(),
            self.message
        )
    }
}

impl std::error::Error for QuestionCopyError {}
