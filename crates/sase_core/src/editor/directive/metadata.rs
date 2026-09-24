use super::super::wire::{
    DirectiveKeywordSpec, DirectiveMetadata, DirectiveSuggestedValue,
    DirectiveSyntaxForm, DirectiveValueRole,
};

const AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "plan",
        documentation: "Plan-gate compatibility alias for normal approval",
    },
    DirectiveSuggestedValue {
        value: "tale",
        documentation: "Plan-gate compatibility alias for SDD tale approval",
    },
    DirectiveSuggestedValue {
        value: "epic",
        documentation: "Plan-gate compatibility alias for SDD epic approval",
    },
];

const EFFORT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "none",
        documentation: "No reasoning-effort override",
    },
    DirectiveSuggestedValue {
        value: "minimal",
        documentation: "Minimal reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "low",
        documentation: "Low reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "medium",
        documentation: "Medium reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "high",
        documentation: "High reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "xhigh",
        documentation: "Extra-high reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "max",
        documentation: "Maximum reasoning effort",
    },
];

const BOOL_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "false",
        documentation: "Disable xprompt expansion",
    },
    DirectiveSuggestedValue {
        value: "true",
        documentation: "Enable xprompt expansion",
    },
];

const REPEAT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "2",
        documentation: "Run the prompt twice",
    },
    DirectiveSuggestedValue {
        value: "3",
        documentation: "Run the prompt three times",
    },
];

const FINAL_SUGGESTIONS: &[DirectiveSuggestedValue] =
    &[DirectiveSuggestedValue {
        value: "none",
        documentation:
            "Clear the configured finalizer selection for this launch",
    }];

const HOLD_SELECTOR_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "pending",
        documentation: "Freeze currently waiting or queued targets at arm time",
    },
    DirectiveSuggestedValue {
        value: "future",
        documentation: "Fence matching launches created after arm time",
    },
];

const HOLD_SCOPE_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "project",
        documentation: "Apply the hold within the selected project",
    },
    DirectiveSuggestedValue {
        value: "host",
        documentation: "Apply the hold across this host",
    },
];

const WAIT_TIME_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "5m",
        documentation: "Duration: start after five minutes",
    },
    DirectiveSuggestedValue {
        value: "1430",
        documentation: "Wall clock: start at 14:30 today (or tomorrow if past)",
    },
];

const WAIT_CAPACITY_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "0",
        documentation:
            "Drain barrier: start after occupied weighted load is zero",
    },
    DirectiveSuggestedValue {
        value: "1",
        documentation: "Start when occupied weighted load is at most 1",
    },
];

const WAIT_CAPACITY_BUDGET_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "1",
        documentation:
            "Capacity budget of 1: run alone when this launch's effective weight is 1",
    },
    DirectiveSuggestedValue {
        value: "100",
        documentation:
            "Capacity budget of 100: replace max_running_agents for this launch",
    },
];

const WAIT_PRIORITY_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "10",
        documentation: "Default runner-queue priority",
    },
    DirectiveSuggestedValue {
        value: "1",
        documentation: "Join the runner queue ahead of larger priorities",
    },
];

const QUEUE_WEIGHT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "0.25",
        documentation: "Quarter capacity unit",
    },
    DirectiveSuggestedValue {
        value: "1.0",
        documentation: "Default capacity unit",
    },
    DirectiveSuggestedValue {
        value: "2.0",
        documentation: "Two capacity units",
    },
];

const BARE_PLUS: &[DirectiveSyntaxForm] =
    &[DirectiveSyntaxForm::Bare, DirectiveSyntaxForm::Plus];
const COLON: &[DirectiveSyntaxForm] = &[DirectiveSyntaxForm::Colon];
const COLON_BARE_PLUS: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Bare,
    DirectiveSyntaxForm::Plus,
];
const COLON_PAREN: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
];
const COLON_PAREN_BARE: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
    DirectiveSyntaxForm::Bare,
];
const ALT_FORMS: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::BraceShorthand,
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
];
const PAREN: &[DirectiveSyntaxForm] = &[DirectiveSyntaxForm::Parenthesized];
const PAREN_DOUBLE_COLON: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Parenthesized,
    DirectiveSyntaxForm::DoubleColon,
];

const ID_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "bead",
        description: "Associate this launch with a bead",
        value_role: DirectiveValueRole::Bead,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "clan",
        description: "Derive the full ID and join this agent clan",
        value_role: DirectiveValueRole::Clan,
        repeatable: false,
        conflicts_with: &["family", "session", "tribe"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        // legacy agent-family spelling; flips in core-contract
        name: "family",
        description: "Attach this suffix to an existing agent session",
        value_role: DirectiveValueRole::Session,
        repeatable: false,
        conflicts_with: &["clan", "session", "tribe"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "session",
        description: "Attach this suffix to an existing agent session",
        value_role: DirectiveValueRole::Session,
        repeatable: false,
        conflicts_with: &["clan", "family", "tribe"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "tribe",
        description: "Assign this agent to a user-managed tribe",
        value_role: DirectiveValueRole::Tribe,
        repeatable: false,
        conflicts_with: &["clan", "family", "session"],
        suggested_values: &[],
    },
];

const CLAN_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "summary",
        description: "Attach a Rich-markup summary to this clan",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &["summary_script"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "summary_script",
        description: "Generate this clan's summary with an executable script",
        value_role: DirectiveValueRole::PathOrExecutable,
        repeatable: false,
        conflicts_with: &["summary"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "tribe",
        description: "Assign this clan to a user-managed tribe",
        value_role: DirectiveValueRole::Tribe,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
];

const BOOL_TRUE_FALSE: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "true",
        documentation: "Acquire an operational workspace lease",
    },
    DirectiveSuggestedValue {
        value: "false",
        documentation: "Skip the workspace lease and use an ordinary cwd",
    },
];

const DURATION_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "20m",
        documentation: "Twenty minutes",
    },
    DirectiveSuggestedValue {
        value: "1h",
        documentation: "One hour",
    },
];

const PROC_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "bash",
        description: "Bash script body",
        value_role: DirectiveValueRole::Code,
        repeatable: false,
        conflicts_with: &["python"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "python",
        description: "Python script body",
        value_role: DirectiveValueRole::Code,
        repeatable: false,
        conflicts_with: &["bash"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "timeout",
        description: "Total execution timeout from child start",
        value_role: DirectiveValueRole::Duration,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: DURATION_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "idle_timeout",
        description: "Idle-output timeout from child start",
        value_role: DirectiveValueRole::Duration,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: DURATION_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "cwd",
        description: "Working directory for the proc",
        value_role: DirectiveValueRole::PathOrExecutable,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "workspace",
        description: "Whether to acquire an operational workspace lease",
        value_role: DirectiveValueRole::Bool,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: BOOL_TRUE_FALSE,
    },
    DirectiveKeywordSpec {
        name: "label",
        description: "Descriptive label; never identity",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
];

const SHOULD_RUN_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "true",
        documentation: "Keep this prompt segment",
    },
    DirectiveSuggestedValue {
        value: "false",
        documentation: "Omit this prompt segment before launch planning",
    },
];

const IF_KEYWORDS: &[DirectiveKeywordSpec] = &[DirectiveKeywordSpec {
    name: "should_run",
    description: "Statically keep or omit this prompt segment",
    value_role: DirectiveValueRole::Bool,
    repeatable: false,
    conflicts_with: &[],
    suggested_values: SHOULD_RUN_SUGGESTIONS,
}];

pub(super) const IF_DIRECTIVE_OFF: DirectiveMetadata = DirectiveMetadata {
    name: "if",
    alias: None,
    description:
        "Statically omit a prompt segment, or gate launch with a typed predicate when enabled",
    argument_hint: "(should_run=true|false)",
    takes_argument: true,
    allows_multiple: false,
    syntax_forms: PAREN,
    positional_role: None,
    positional_suggestions: &[],
    keywords: IF_KEYWORDS,
    dynamic_keyword_role: None,
};

pub(super) const IF_DIRECTIVE_ON: DirectiveMetadata = DirectiveMetadata {
    syntax_forms: PAREN_DOUBLE_COLON,
    argument_hint: "(should_run=true|false) or :: plus one bash/python fence",
    ..IF_DIRECTIVE_OFF
};

const QUEUE_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "capacity",
        description:
            "Start when occupied weighted load is at most this threshold",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_CAPACITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "p",
        description: "Alias for priority=; lower values start first",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &["priority"],
        suggested_values: WAIT_PRIORITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "priority",
        description: "Lower values start first; the default is 10",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &["p"],
        suggested_values: WAIT_PRIORITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "w",
        description: "Alias for weight=; positive capacity units",
        value_role: DirectiveValueRole::PositiveFloat,
        repeatable: false,
        conflicts_with: &["weight"],
        suggested_values: QUEUE_WEIGHT_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "weight",
        description: "Positive capacity units claimed by this launch",
        value_role: DirectiveValueRole::PositiveFloat,
        repeatable: false,
        conflicts_with: &["w"],
        suggested_values: QUEUE_WEIGHT_SUGGESTIONS,
    },
];

pub(super) const QUEUE_DIRECTIVE_OFF: DirectiveMetadata = DirectiveMetadata {
    name: "queue",
    alias: Some("q"),
    description: "Set weighted-load capacity, priority, and capacity weight",
    argument_hint: ":N or (N, capacity=, priority=, p=, weight=, w=)",
    takes_argument: true,
    allows_multiple: true,
    syntax_forms: COLON_PAREN,
    positional_role: Some(DirectiveValueRole::NonNegativeInt),
    positional_suggestions: WAIT_CAPACITY_SUGGESTIONS,
    keywords: QUEUE_KEYWORDS,
    dynamic_keyword_role: None,
};

pub(super) const QUEUE_DIRECTIVE_ON: DirectiveMetadata = DirectiveMetadata {
    name: "queue",
    alias: Some("q"),
    description:
        "Set this launch's capacity budget, priority, and capacity weight",
    argument_hint: ":N or (N, capacity=, priority=, p=, weight=, w=)",
    takes_argument: true,
    allows_multiple: true,
    syntax_forms: COLON_PAREN,
    positional_role: Some(DirectiveValueRole::PositiveInt),
    positional_suggestions: WAIT_CAPACITY_BUDGET_SUGGESTIONS,
    keywords: QUEUE_BUDGET_KEYWORDS,
    dynamic_keyword_role: None,
};

const QUEUE_BUDGET_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "capacity",
        description:
            "This launch's capacity budget, replacing max_running_agents",
        value_role: DirectiveValueRole::PositiveInt,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_CAPACITY_BUDGET_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "p",
        description: "Alias for priority=; lower values start first",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &["priority"],
        suggested_values: WAIT_PRIORITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "priority",
        description: "Lower values start first; the default is 10",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &["p"],
        suggested_values: WAIT_PRIORITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "w",
        description: "Alias for weight=; positive capacity units",
        value_role: DirectiveValueRole::PositiveFloat,
        repeatable: false,
        conflicts_with: &["weight"],
        suggested_values: QUEUE_WEIGHT_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "weight",
        description: "Positive capacity units claimed by this launch",
        value_role: DirectiveValueRole::PositiveFloat,
        repeatable: false,
        conflicts_with: &["w"],
        suggested_values: QUEUE_WEIGHT_SUGGESTIONS,
    },
];

const HOLD_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "hood",
        description: "Hold launches in this agent hood",
        value_role: DirectiveValueRole::Hood,
        repeatable: true,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "scope",
        description: "Choose whether the hold applies to this project or host",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: HOLD_SCOPE_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "ttl",
        description: "Maximum hold duration",
        value_role: DirectiveValueRole::Duration,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: DURATION_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "tribe",
        description: "Hold launches assigned to this user-managed tribe",
        value_role: DirectiveValueRole::Tribe,
        repeatable: true,
        conflicts_with: &[],
        suggested_values: &[],
    },
];

const WAIT_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "agent",
        description: "Wait for an explicit agent identity",
        value_role: DirectiveValueRole::Agent,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "bead",
        description: "Wait until this bead is closed",
        value_role: DirectiveValueRole::Bead,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "hood",
        description: "Wait for current members of this agent hood",
        value_role: DirectiveValueRole::Hood,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "proc",
        description: "Wait for a proc ID or shell name",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "time",
        description: "Start after a duration or absolute wall-clock time",
        value_role: DirectiveValueRole::WaitTime,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_TIME_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "unit",
        description: "Wait for a logical launch unit ID",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
];

pub const BEAD_COMPLETION_LIMIT: usize = 100;

pub(super) const BEAD_STATUS_RANK: &[(&str, u8)] = &[
    ("in_progress", 0),
    ("claimed", 1),
    ("ready", 2),
    ("open", 3),
    ("snoozed", 4),
];

pub const DIRECTIVES: &[DirectiveMetadata] = &[
    DirectiveMetadata {
        name: "model",
        alias: Some("m"),
        description: "Override the LLM model for this prompt",
        argument_hint: ":model or (model, alias=model)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Model),
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: Some(DirectiveValueRole::ModelAliasKey),
    },
    DirectiveMetadata {
        // `%e` is the advertised `%effort` alias. It canonicalizes to `effort`
        // for completion, hover, diagnostics, and fan-out parsing. Mirrors the
        // Python xprompt parser's `_DIRECTIVE_ALIASES["e"] = "effort"`.
        name: "effort",
        alias: Some("e"),
        description: "Set the reasoning-effort level for this prompt",
        argument_hint: ":level",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: EFFORT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "final",
        alias: None,
        description: "Select configured finalizer instances for this launch",
        argument_hint: ":instance|!instance|none or (instance, ...)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::FinalizerInstance),
        positional_suggestions: FINAL_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "id",
        alias: Some("i"),
        description: "Assign an agent ID with optional bead, clan, session, or user-managed tribe",
        argument_hint:
            ":agent-id or :name.{@key}; ([id], bead=, clan=/session=/tribe=)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN_BARE,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: &[],
        keywords: ID_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "clan",
        alias: Some("c"),
        description: "Declare a new parallel agent clan",
        argument_hint:
            ":name or :name.{@key}, (name, tribe=/summary=/summary_script=), or :name:: summary",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Clan),
        positional_suggestions: &[],
        keywords: CLAN_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "wait",
        alias: Some("w"),
        description: "Wait for another agent/workflow and/or a time floor",
        argument_hint: ":agent or (agent, bead=, hood=, time=)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON_PAREN_BARE,
        positional_role: Some(DirectiveValueRole::Agent),
        positional_suggestions: &[],
        keywords: WAIT_KEYWORDS,
        dynamic_keyword_role: None,
    },
    QUEUE_DIRECTIVE_OFF,
    DirectiveMetadata {
        name: "hold",
        alias: None,
        description: "Hold selected pre-run agents until this launch settles",
        argument_hint:
            ":agent or (agent, pending, future, hood=, tribe=, ttl=, scope=)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Agent),
        positional_suggestions: HOLD_SELECTOR_SUGGESTIONS,
        keywords: HOLD_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "dispatch",
        alias: None,
        description: "Send this launch to an enrolled remote machine",
        argument_hint: ":machine or (machine)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Machine),
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: None,
    },
    IF_DIRECTIVE_OFF,
    DirectiveMetadata {
        name: "proc",
        alias: None,
        description:
            "Launch a stand-alone proc unit with a Bash or Python body",
        argument_hint:
            "(\"cmd\"), (bash=|python=, timeout=, idle_timeout=, cwd=, workspace=, label=), or :: fence",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: PAREN_DOUBLE_COLON,
        positional_role: Some(DirectiveValueRole::Code),
        positional_suggestions: &[],
        keywords: PROC_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "auto",
        alias: Some("a"),
        description:
            "Request automatic gate resolution; arguments are interpreted by the gate kind",
        argument_hint: ":argument (e.g. plan|tale|epic)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_BARE_PLUS,
        positional_role: Some(DirectiveValueRole::GateOwned),
        positional_suggestions: AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "hide",
        alias: Some("h"),
        description: "Hide the agent from the default Agents tab display",
        argument_hint: "flag",
        takes_argument: false,
        allows_multiple: false,
        syntax_forms: BARE_PLUS,
        positional_role: None,
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "repeat",
        alias: Some("r"),
        description: "Run the prompt multiple times",
        argument_hint: ":count",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::PositiveInt),
        positional_suggestions: REPEAT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        // The `%{A | B}` brace shorthand is the advertised alt spelling. The
        // legacy `%(...)` alias is kept parse-compatible via
        // `canonical_directive_name`, but is no longer surfaced here so editor
        // completion/hover stop advertising it.
        name: "alt",
        alias: None,
        description:
            "Split prompt into variants with different text; shorthand %{A | B}",
        argument_hint: "(variants)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: ALT_FORMS,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "xprompts_enabled",
        alias: None,
        description: "Enable or disable xprompt expansion for a region",
        argument_hint: ":false|true",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::Bool),
        positional_suggestions: BOOL_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
];

pub(super) const HIDDEN_COMPLETION_DIRECTIVES: &[&str] = &[];
