use serde::{Deserialize, Serialize};

pub const EDITOR_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EditorPosition {
    pub line: u32,
    pub character: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EditorRange {
    pub start: EditorPosition,
    pub end: EditorPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditorTextEdit {
    pub range: EditorRange,
    pub new_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenInfo {
    pub text: String,
    pub range: EditorRange,
    pub byte_start: usize,
    pub byte_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionContextKind {
    Xprompt,
    SlashSkill,
    FilePath,
    FileHistory,
    XpromptArgumentName,
    XpromptArgumentValue,
    XpromptArgumentPath,
    XpromptArgumentTypeHint,
    DirectiveName,
    DirectiveArgument,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionContext {
    pub kind: CompletionContextKind,
    pub token: Option<TokenInfo>,
    #[serde(default)]
    pub active_xprompt: Option<String>,
    #[serde(default)]
    pub active_input: Option<String>,
    #[serde(default)]
    pub directive_name: Option<String>,
    pub replacement_range: EditorRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionCandidate {
    pub display: String,
    pub insertion: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub documentation: Option<String>,
    #[serde(default)]
    pub is_dir: bool,
    pub name: String,
    #[serde(default)]
    pub replacement: Option<EditorTextEdit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionList {
    pub candidates: Vec<CompletionCandidate>,
    pub shared_extension: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptInputHint {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub required: bool,
    pub default_display: Option<String>,
    pub position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptAssistEntry {
    pub name: String,
    pub insertion: String,
    pub reference_prefix: String,
    pub kind: Option<String>,
    pub input_signature: Option<String>,
    pub inputs: Vec<XpromptInputHint>,
    pub content_preview: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_path_display: Option<String>,
    pub is_skill: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveMetadata {
    pub name: &'static str,
    pub alias: Option<&'static str>,
    pub description: &'static str,
    pub takes_argument: bool,
    pub allows_multiple: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Information,
    Hint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditorDiagnostic {
    pub range: EditorRange,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoverPayload {
    pub range: EditorRange,
    pub markdown: String,
}
