//! Versioned prompt command-substitution policy wires.
//!
//! Offsets in this contract are UTF-8 byte indexes into `text`, matching
//! Rust `&str` indexing. They are not Python `str` indexes when the prefix
//! contains non-ASCII characters.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PROMPT_COMMAND_SUBSTITUTION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Wire spelling for replacement and protected-range offsets.
pub const PROMPT_COMMAND_SUBSTITUTION_OFFSET_UNIT: &str = "utf8_bytes";

pub const PROMPT_COMMAND_SUBSTITUTION_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_prompt_command_substitution_schema";
pub const PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_REQUEST: &str =
    "invalid_prompt_command_substitution_request";
pub const PROMPT_COMMAND_SUBSTITUTION_CODE_INVALID_PROTECTED_RANGE: &str =
    "invalid_protected_range";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{code}: {message}")]
pub struct PromptCommandSubstitutionError {
    pub code: String,
    pub message: String,
}

impl PromptCommandSubstitutionError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

/// Provenance of the scanned text.
///
/// Generated covers already-rendered and other produced data: it is never
/// eligible, even in a human launch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCommandTextKindWire {
    HumanSource,
    AdmittedDefinition,
    Generated,
}

/// Launch-authorship stamp gathered by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptLaunchAuthorshipWire {
    Human,
    Agent,
}

/// Inclusive-start exclusive-end UTF-8 byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptUtf8ByteRangeWire {
    pub start: u64,
    pub end: u64,
}

/// Host-collected facts for one command-substitution planning request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptCommandSubstitutionRequestWire {
    pub schema_version: u32,
    pub text: String,
    pub kind: PromptCommandTextKindWire,
    pub launch_authorship: PromptLaunchAuthorshipWire,
    #[serde(default)]
    pub protected_ranges: Vec<PromptUtf8ByteRangeWire>,
}

/// One complete `$(command)` span in the original text.
///
/// `start` is the UTF-8 byte index of `$`. `end` is the UTF-8 byte index
/// after the matching `)`. `command` is the original inner command text,
/// not including the `$(` / `)` delimiters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptCommandReplacementWire {
    pub start: u64,
    pub end: u64,
    pub command: String,
}

/// Ordered replacement plan. Empty `replacements` when ineligible or when
/// the text has no complete unescaped commands outside protected ranges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptCommandSubstitutionPlanWire {
    pub schema_version: u32,
    pub eligible: bool,
    pub offset_unit: String,
    pub replacements: Vec<PromptCommandReplacementWire>,
}
