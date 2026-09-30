//! Static Jinja completion catalog.
//!
//! Every name sase injects into agent prompts (with its availability
//! rule), Jinja's own globals, and the identifier-named Jinja 3.1
//! filters, tests, and statement keywords the prompt environment
//! supports. The prompt environment enables no extensions, so `do`,
//! `break`, and `continue` are excluded.
//!
//! Wording follows `docs/xprompt.md` ("Template Context", "Repeat
//! Directive") and the provider skill variables in `docs/llms.md`.

use std::sync::LazyLock;

use super::wire::{
    JinjaAvailabilityRule, JinjaCatalogFilterWire, JinjaCatalogGlobalWire,
    JinjaCatalogStatementWire, JinjaCatalogTestWire, JinjaCatalogVariableWire,
    JinjaCatalogWire, JinjaCompletionSource, JinjaFilterTier,
    JinjaNamespaceMemberWire,
};

static CATALOG: LazyLock<JinjaCatalogWire> = LazyLock::new(build_catalog);

/// Return the static Jinja catalog.
pub fn jinja_catalog() -> &'static JinjaCatalogWire {
    &CATALOG
}

fn variable(
    name: &str,
    type_label: &str,
    group: JinjaCompletionSource,
    summary: &str,
    documentation: &str,
    availability_rule: JinjaAvailabilityRule,
) -> JinjaCatalogVariableWire {
    JinjaCatalogVariableWire {
        name: name.to_string(),
        type_label: type_label.to_string(),
        group,
        summary: summary.to_string(),
        documentation: documentation.to_string(),
        availability_rule,
        legacy_for: None,
        members: Vec::new(),
    }
}

fn member(
    name: &str,
    type_label: &str,
    summary: &str,
) -> JinjaNamespaceMemberWire {
    JinjaNamespaceMemberWire {
        name: name.to_string(),
        type_label: type_label.to_string(),
        summary: summary.to_string(),
    }
}

fn variables() -> Vec<JinjaCatalogVariableWire> {
    vec![
        variable(
            "root",
            "str",
            JinjaCompletionSource::Sase,
            "Absolute path to the primary workspace directory.",
            "Absolute path to the primary (#1) workspace directory \
             for the current project. Omitted when the project cannot \
             be resolved.\n\nExample: `{{ root }}`",
            JinjaAvailabilityRule::Always,
        ),
        JinjaCatalogVariableWire {
            members: vec![
                member(
                    "chats",
                    "list[str]",
                    "Chat-transcript paths for agents named in \
                     `%wait:<name>` directives, in the order they \
                     appear.",
                ),
                member(
                    "artifacts",
                    "list[dict]",
                    "Metadata dictionaries for non-chat artifacts \
                     produced by waited agents; no file contents.",
                ),
            ],
            ..variable(
                "wait",
                "namespace",
                JinjaCompletionSource::Sase,
                "Namespace of outputs produced by waited agents.",
                "Namespace populated while an agent run renders its \
                 executable prompt. `wait.chats` lists chat-transcript \
                 paths for agents named in `%wait:<name>` directives; \
                 `wait.artifacts` lazily lists non-chat artifact \
                 metadata dictionaries.\n\nExample: \
                 `{{ wait.chats }}`",
                JinjaAvailabilityRule::Run,
            )
        },
        variable(
            "patch_name",
            "str",
            JinjaCompletionSource::Sase,
            "Name of the patch the agent is working on.",
            "Name of the patch (change) the agent run works on, as \
             passed to the agent runner.\n\nExample: \
             `{{ patch_name }}`",
            JinjaAvailabilityRule::Run,
        ),
        variable(
            "workspace_num",
            "int",
            JinjaCompletionSource::Sase,
            "Number of the agent's workspace.",
            "1-based number of the workspace directory assigned to \
             this agent run.\n\nExample: `{{ workspace_num }}`",
            JinjaAvailabilityRule::Run,
        ),
        JinjaCatalogVariableWire {
            legacy_for: Some("patch_name".to_string()),
            ..variable(
                "cl_name",
                "str",
                JinjaCompletionSource::Sase,
                "Legacy alias of `patch_name`.",
                "Legacy alias of `patch_name` — prefer \
                 `{{ patch_name }}`.\n\nExample: `{{ patch_name }}`",
                JinjaAvailabilityRule::Run,
            )
        },
        variable(
            "n",
            "int",
            JinjaCompletionSource::Sase,
            "Current `%repeat` iteration (1-based).",
            "Current iteration of a `%repeat` run (1-based). Only \
             defined when the launching prompt uses `%repeat`.\n\n\
             Example: `Run test suite batch {{ n }} of {{ N }}.`",
            JinjaAvailabilityRule::RunNeedsRepeat,
        ),
        variable(
            "N",
            "int",
            JinjaCompletionSource::Sase,
            "Total `%repeat` iterations.",
            "Total iteration count — the `%repeat` argument. Only \
             defined when the launching prompt uses `%repeat`.\n\n\
             Example: `Run test suite batch {{ n }} of {{ N }}.`",
            JinjaAvailabilityRule::RunNeedsRepeat,
        ),
        variable(
            "agents",
            "mapping",
            JinjaCompletionSource::Sase,
            "Output variables of waited agents, keyed by agent name.",
            "Reserved mapping holding every producer's output \
             variables, keyed by agent name — for example \
             `{{ agents[\"build\"].path }}` after `%wait:build` when \
             that agent used `sase var set path=...`. Only defined \
             when the prompt uses `%wait`.\n\nExample: \
             `{{ agents[\"build\"].path }}`",
            JinjaAvailabilityRule::RunNeedsWait,
        ),
        JinjaCatalogVariableWire {
            legacy_for: Some("wait.chats".to_string()),
            ..variable(
                "wait_chats",
                "list[str]",
                JinjaCompletionSource::Sase,
                "Legacy alias of `wait.chats`.",
                "Legacy alias of `wait.chats` — prefer \
                 `{{ wait.chats }}`.\n\nExample: `{{ wait.chats }}`",
                JinjaAvailabilityRule::RunNeedsWait,
            )
        },
        variable(
            "_args",
            "list",
            JinjaCompletionSource::Positional,
            "List of all positional arguments.",
            "List of all positional arguments passed to the xprompt. \
             Individual positions render as `{{ _1 }}`, `{{ _2 }}`, \
             and so on up to the declared input count (at least \
             `{{ _1 }}`). Unavailable in `prompt` \
             scope.\n\nExample: `{{ _args }}`",
            JinjaAvailabilityRule::XpromptOnly,
        ),
        variable(
            "provider_name",
            "str",
            JinjaCompletionSource::Provider,
            "Name of the provider rendering a skill.",
            "Name of the LLM provider (for example `\"Grok\"`) \
             rendering a skill file. Only defined in `xprompt` scope \
             with truthy frontmatter `skill`.\n\nExample: \
             `{{ provider_name }}`",
            JinjaAvailabilityRule::XpromptSkillOnly,
        ),
        variable(
            "provider_tool_name",
            "str",
            JinjaCompletionSource::Provider,
            "Display name of the provider's tool.",
            "Display name of the provider tool rendering a skill \
             file (for example `\"Grok Build\"`). Only defined in \
             `xprompt` scope with truthy frontmatter \
             `skill`.\n\nExample: `{{ provider_tool_name }}`",
            JinjaAvailabilityRule::XpromptSkillOnly,
        ),
        variable(
            "provider_native_ask_tool",
            "str",
            JinjaCompletionSource::Provider,
            "Native question tool of the provider.",
            "Name of the provider's native ask-user-question tool \
             (for example `\"ask_user_question\"`). Only defined in \
             `xprompt` scope with truthy frontmatter \
             `skill`.\n\nExample: \
             `{{ provider_native_ask_tool }}`",
            JinjaAvailabilityRule::XpromptSkillOnly,
        ),
        JinjaCatalogVariableWire {
            members: loop_members(),
            ..variable(
                "loop",
                "for-loop",
                JinjaCompletionSource::Local,
                "The current `for`-loop object.",
                "Jinja's special loop object, available inside a \
                 `{% for %}` body. Its members describe the current \
                 iteration.\n\nExample: `{{ loop.index }}`",
                JinjaAvailabilityRule::Always,
            )
        },
    ]
}

fn loop_members() -> Vec<JinjaNamespaceMemberWire> {
    vec![
        member("index", "int", "1-based position of the current iteration."),
        member(
            "index0",
            "int",
            "0-based position of the current iteration.",
        ),
        member(
            "revindex",
            "int",
            "1-based iterations remaining, counting the current one.",
        ),
        member(
            "revindex0",
            "int",
            "0-based iterations remaining after the current one.",
        ),
        member("first", "bool", "True on the first iteration."),
        member("last", "bool", "True on the last iteration."),
        member("length", "int", "Number of items in the sequence."),
        member("depth", "int", "1-based nesting depth of a recursive loop."),
        member(
            "depth0",
            "int",
            "0-based nesting depth of a recursive loop.",
        ),
        member(
            "previtem",
            "unknown",
            "Item from the previous iteration (undefined on the \
             first).",
        ),
        member(
            "nextitem",
            "unknown",
            "Item from the next iteration (undefined on the last).",
        ),
        member(
            "cycle",
            "callable",
            "Call with values to alternate each iteration, for \
             example `{{ loop.cycle('odd', 'even') }}`.",
        ),
        member(
            "changed",
            "callable",
            "Call with a value; true when it differs from the \
             previous iteration.",
        ),
    ]
}

fn jinja_global(
    name: &str,
    signature: &str,
    summary: &str,
) -> JinjaCatalogGlobalWire {
    JinjaCatalogGlobalWire {
        name: name.to_string(),
        signature: signature.to_string(),
        summary: summary.to_string(),
    }
}

fn jinja_globals() -> Vec<JinjaCatalogGlobalWire> {
    vec![
        jinja_global(
            "range",
            "([start, ]stop[, step])",
            "Generate a sequence of numbers for `for` loops, like \
             Python's range.",
        ),
        jinja_global(
            "dict",
            "(**kwargs)",
            "Build a dictionary from keyword arguments.",
        ),
        jinja_global(
            "lipsum",
            "(n=5, html=true, min=20, max=100)",
            "Generate filler text for template previews.",
        ),
        jinja_global(
            "cycler",
            "(*items)",
            "Cycle through values across loop iterations.",
        ),
        jinja_global(
            "joiner",
            "(sep=', ')",
            "Join items with a separator omitted before the first \
             item.",
        ),
        jinja_global(
            "namespace",
            "(**kwargs)",
            "Create a mutable object for assignments inside loops.",
        ),
    ]
}

fn filter(
    name: &str,
    signature: &str,
    summary: &str,
    tier: JinjaFilterTier,
) -> JinjaCatalogFilterWire {
    JinjaCatalogFilterWire {
        name: name.to_string(),
        signature: signature.to_string(),
        summary: summary.to_string(),
        tier,
    }
}

fn filters() -> Vec<JinjaCatalogFilterWire> {
    use JinjaFilterTier::{Common, Other, Sase};
    vec![
        filter(
            "plan_ref_path",
            "()",
            "Return the `YYYYmm/<name>.md` portion of a plan path \
             or `plan:` reference; other values pass through.",
            Sase,
        ),
        filter(
            "provider_disabled",
            "(mode=\"any\")",
            "True when the named provider has an active \
             machine-wide disable.",
            Sase,
        ),
        filter(
            "provider_enabled",
            "(mode=\"any\")",
            "True when the named provider has no active \
             machine-wide disable.",
            Sase,
        ),
        filter("abs", "()", "Return the absolute value.", Other),
        filter(
            "attr",
            "(name)",
            "Return an object's attribute by name.",
            Other,
        ),
        filter(
            "batch",
            "(linecount, fill_with=None)",
            "Group items into rows of `linecount`.",
            Other,
        ),
        filter("capitalize", "()", "Capitalize the first character.", Other),
        filter(
            "center",
            "(width=80)",
            "Center the value in a field of `width`.",
            Other,
        ),
        filter("count", "()", "Count the items.", Other),
        filter(
            "d",
            "(default_value='', boolean=false)",
            "Alias of `default`.",
            Other,
        ),
        filter(
            "default",
            "(default_value='', boolean=false)",
            "Use a default when the value is undefined.",
            Common,
        ),
        filter(
            "dictsort",
            "(case_sensitive=false, by='key', reverse=false)",
            "Sort a dict by key or value.",
            Other,
        ),
        filter("e", "()", "Alias of `escape`.", Other),
        filter("escape", "()", "Escape HTML special characters.", Other),
        filter(
            "filesizeformat",
            "(binary=false)",
            "Format a byte count as a human-readable size.",
            Other,
        ),
        filter("first", "()", "Return the first item.", Common),
        filter("float", "(default=0.0)", "Convert to a float.", Other),
        filter(
            "forceescape",
            "()",
            "Enforce HTML escaping on an already-markup value.",
            Other,
        ),
        filter(
            "format",
            "(*args, **kwargs)",
            "Apply printf-style formatting with the given \
             arguments.",
            Other,
        ),
        filter(
            "groupby",
            "(attribute, default=None, case_sensitive=false)",
            "Group items by an attribute.",
            Other,
        ),
        filter(
            "indent",
            "(width=4, first=false, blank=false)",
            "Indent lines after the first.",
            Common,
        ),
        filter(
            "int",
            "(default=0, base=10)",
            "Convert to an integer.",
            Other,
        ),
        filter(
            "items",
            "()",
            "Yield `(key, value)` pairs of a dict.",
            Other,
        ),
        filter(
            "join",
            "(d='', attribute=None)",
            "Join items into a string.",
            Common,
        ),
        filter("last", "()", "Return the last item.", Common),
        filter("length", "()", "Return the number of items.", Common),
        filter("list", "()", "Convert to a list.", Other),
        filter("lower", "()", "Convert to lowercase.", Common),
        filter(
            "map",
            "(*args, **kwargs)",
            "Apply a filter or attribute to every item.",
            Other,
        ),
        filter(
            "max",
            "(case_sensitive=false, attribute=None)",
            "Return the largest item.",
            Other,
        ),
        filter(
            "min",
            "(case_sensitive=false, attribute=None)",
            "Return the smallest item.",
            Other,
        ),
        filter("pprint", "()", "Pretty-print for debugging.", Other),
        filter("random", "()", "Return a random item.", Other),
        filter(
            "reject",
            "(*args, **kwargs)",
            "Reject items matching a test.",
            Other,
        ),
        filter(
            "rejectattr",
            "(*args, **kwargs)",
            "Reject items whose attribute matches a test.",
            Other,
        ),
        filter(
            "replace",
            "(old, new, count=None)",
            "Replace substrings.",
            Common,
        ),
        filter("reverse", "()", "Reverse the sequence.", Other),
        filter(
            "round",
            "(precision=0, method='common')",
            "Round a number.",
            Other,
        ),
        filter(
            "safe",
            "()",
            "Mark the value as safe HTML (skip escaping).",
            Other,
        ),
        filter(
            "select",
            "(*args, **kwargs)",
            "Keep items matching a test.",
            Other,
        ),
        filter(
            "selectattr",
            "(*args, **kwargs)",
            "Keep items whose attribute matches a test.",
            Other,
        ),
        filter(
            "slice",
            "(slices, fill_with=None)",
            "Slice an iterator into columns.",
            Other,
        ),
        filter(
            "sort",
            "(reverse=false, case_sensitive=false, attribute=None)",
            "Sort the sequence.",
            Common,
        ),
        filter("string", "()", "Convert to a string.", Other),
        filter("striptags", "()", "Remove SGML/XML tags.", Other),
        filter("sum", "(attribute=None, start=0)", "Sum the items.", Other),
        filter("title", "()", "Title-case the value.", Other),
        filter("tojson", "(indent=None)", "Dump to JSON.", Common),
        filter("trim", "(chars=None)", "Strip whitespace.", Common),
        filter(
            "truncate",
            "(length=255, killwords=false, end='...', leeway=None)",
            "Truncate to `length` characters.",
            Other,
        ),
        filter(
            "unique",
            "(case_sensitive=false, attribute=None)",
            "Drop duplicate items.",
            Common,
        ),
        filter("upper", "()", "Convert to uppercase.", Common),
        filter("urlencode", "()", "URL-encode the value.", Other),
        filter(
            "urlize",
            "(trim_url_limit=None, nofollow=false, target=None, \
             rel=None)",
            "Turn URLs into links.",
            Other,
        ),
        filter("wordcount", "()", "Count the words.", Other),
        filter(
            "wordwrap",
            "(width=79, break_long_words=true, wrapstring=None, \
             break_on_hyphens=true)",
            "Wrap text to `width` columns.",
            Other,
        ),
        filter(
            "xmlattr",
            "(autospace=true)",
            "Render a dict as XML attributes.",
            Other,
        ),
    ]
}

fn test(name: &str, summary: &str) -> JinjaCatalogTestWire {
    JinjaCatalogTestWire {
        name: name.to_string(),
        summary: summary.to_string(),
    }
}

fn tests() -> Vec<JinjaCatalogTestWire> {
    vec![
        test("boolean", "True for boolean values."),
        test("callable", "True when the value can be called."),
        test("defined", "True when the value is defined."),
        test("divisibleby", "True when divisible by the argument."),
        test("eq", "True when equal to the argument."),
        test("equalto", "Alias of `eq`."),
        test("escaped", "True when the value is escaped markup."),
        test("even", "True for even numbers."),
        test("false", "True when the value is false."),
        test("filter", "True when the named filter exists."),
        test("float", "True for float values."),
        test("ge", "True when greater than or equal."),
        test("greaterthan", "Alias of `ge`."),
        test("gt", "Alias of `ge`."),
        test("in", "True when contained in the argument."),
        test("integer", "True for integer values."),
        test("iterable", "True when the value can be iterated."),
        test("le", "True when less than or equal."),
        test("lessthan", "Alias of `le`."),
        test("lower", "True when the string is lowercase."),
        test("lt", "Alias of `le`."),
        test("mapping", "True for mappings."),
        test("ne", "True when not equal."),
        test("none", "True when the value is none."),
        test("number", "True for numeric values."),
        test("odd", "True for odd numbers."),
        test("sameas", "True when identical to the argument."),
        test("sequence", "True for sequences."),
        test("string", "True for strings."),
        test("test", "True when the named test exists."),
        test("true", "True when the value is true."),
        test("undefined", "True when the value is undefined."),
        test("upper", "True when the string is uppercase."),
    ]
}

fn statement(
    name: &str,
    closer: Option<&str>,
    summary: &str,
) -> JinjaCatalogStatementWire {
    JinjaCatalogStatementWire {
        name: name.to_string(),
        closer: closer.map(str::to_string),
        summary: summary.to_string(),
    }
}

fn statements() -> Vec<JinjaCatalogStatementWire> {
    vec![
        statement("if", Some("endif"), "Open a conditional block."),
        statement(
            "elif",
            None,
            "Continue a conditional chain with a new condition.",
        ),
        statement("else", None, "Provide the fallback branch of a block."),
        statement("endif", None, "Close an `if` block."),
        statement("for", Some("endfor"), "Loop over a sequence."),
        statement("endfor", None, "Close a `for` block."),
        statement(
            "set",
            Some("endset"),
            "Assign a variable (or capture a block).",
        ),
        statement("endset", None, "Close a block `set`."),
        statement("macro", Some("endmacro"), "Define a reusable macro."),
        statement("endmacro", None, "Close a `macro` block."),
        statement("call", Some("endcall"), "Call a macro with a caller block."),
        statement("endcall", None, "Close a `call` block."),
        statement("filter", Some("endfilter"), "Apply a filter to a block."),
        statement("endfilter", None, "Close a `filter` block."),
        statement("with", Some("endwith"), "Scope temporary assignments."),
        statement("endwith", None, "Close a `with` block."),
        statement(
            "raw",
            Some("endraw"),
            "Render contents without Jinja processing.",
        ),
        statement("endraw", None, "Close a `raw` block."),
        statement(
            "block",
            Some("endblock"),
            "Define an overridable template block.",
        ),
        statement("endblock", None, "Close a `block`."),
        statement("extends", None, "Inherit from a parent template."),
        statement("include", None, "Include another template."),
        statement("import", None, "Import a template's macros under a name."),
        statement("from", None, "Import specific names from a template."),
    ]
}

fn build_catalog() -> JinjaCatalogWire {
    JinjaCatalogWire {
        variables: variables(),
        filters: filters(),
        tests: tests(),
        jinja_globals: jinja_globals(),
        statements: statements(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn names(items: &[String]) -> HashSet<&str> {
        items.iter().map(String::as_str).collect()
    }

    #[test]
    fn names_are_unique_within_each_table() {
        let catalog = jinja_catalog();
        for (label, entries) in [
            (
                "variables",
                catalog
                    .variables
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect::<Vec<_>>(),
            ),
            (
                "filters",
                catalog
                    .filters
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect::<Vec<_>>(),
            ),
            (
                "tests",
                catalog
                    .tests
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect::<Vec<_>>(),
            ),
            (
                "jinja_globals",
                catalog
                    .jinja_globals
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect::<Vec<_>>(),
            ),
            (
                "statements",
                catalog
                    .statements
                    .iter()
                    .map(|entry| entry.name.clone())
                    .collect::<Vec<_>>(),
            ),
        ] {
            let unique = names(&entries);
            assert_eq!(
                unique.len(),
                entries.len(),
                "duplicate names in {label}"
            );
        }
    }

    #[test]
    fn every_variable_has_summary_and_documentation() {
        for entry in &jinja_catalog().variables {
            assert!(!entry.summary.is_empty(), "{} has no summary", entry.name);
            assert!(
                !entry.documentation.is_empty(),
                "{} has no documentation",
                entry.name
            );
        }
    }

    #[test]
    fn every_legacy_alias_points_at_an_existing_name() {
        let catalog = jinja_catalog();
        let variables: HashSet<&str> = catalog
            .variables
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        for entry in &catalog.variables {
            let Some(target) = entry.legacy_for.as_deref() else {
                continue;
            };
            if let Some((namespace, member)) = target.split_once('.') {
                let holder = catalog
                    .variables
                    .iter()
                    .find(|candidate| candidate.name == namespace)
                    .unwrap_or_else(|| {
                        panic!(
                            "{} legacy target {target} is unknown",
                            entry.name
                        )
                    });
                assert!(
                    holder.members.iter().any(|row| row.name == member),
                    "{} legacy target {target} is unknown",
                    entry.name
                );
            } else {
                assert!(
                    variables.contains(target),
                    "{} legacy target {target} is unknown",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn every_opener_has_a_closer() {
        let catalog = jinja_catalog();
        let statements: HashSet<&str> = catalog
            .statements
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        for entry in &catalog.statements {
            if let Some(closer) = entry.closer.as_deref() {
                assert!(
                    statements.contains(closer),
                    "closer {closer} of {} is unknown",
                    entry.name
                );
            }
        }
        for (opener, closer) in [
            ("if", "endif"),
            ("for", "endfor"),
            ("set", "endset"),
            ("macro", "endmacro"),
            ("call", "endcall"),
            ("filter", "endfilter"),
            ("with", "endwith"),
            ("raw", "endraw"),
            ("block", "endblock"),
        ] {
            let entry = catalog
                .statements
                .iter()
                .find(|candidate| candidate.name == opener)
                .unwrap_or_else(|| panic!("opener {opener} is missing"));
            assert_eq!(
                entry.closer.as_deref(),
                Some(closer),
                "opener {opener} has the wrong closer"
            );
        }
    }
}
