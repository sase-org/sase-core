//! Deterministic expanded-prompt selection for agent artifact directories.
//!
//! Every frontend reading agent prompt artifacts must agree on which
//! `*_prompt.md` file is the agent's expanded prompt. Raw prompts have their
//! own display surface and must never win this selection, even when the raw
//! file is newer or the host enumerates directory entries in raw-first order.
//!
//! Tie rule: prefer the newest integer `mtime_ns`; when timestamps are equal,
//! choose the lexicographically greatest path so the result does not depend
//! on directory enumeration order and conventional numbered prompts prefer
//! the later name (for example `02_prompt.md` over `01_prompt.md`).

use serde::{Deserialize, Serialize};

/// Wire schema for the agent prompt selection request.
pub const AGENT_PROMPT_SELECTION_WIRE_SCHEMA_VERSION: u64 = 1;

/// Canonical raw prompt basename with its own display surface.
pub const RAW_PROMPT_BASENAME: &str = "raw_prompt.md";
/// Pre-rename raw prompt basename, still excluded from expanded selection.
pub const LEGACY_RAW_PROMPT_BASENAME: &str = "raw_xprompt.md";

const FINALIZER_PREFIX: &str = "commit_finalizer_pass_";
const FINALIZER_SUFFIX: &str = "_prompt.md";

/// One observed `*_prompt.md` candidate with its integer nanosecond mtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPromptSelectionCandidateWire {
    pub path: String,
    pub mtime_ns: i64,
}

/// Selection request over observed candidates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPromptSelectionRequestWire {
    #[serde(default)]
    pub candidates: Vec<AgentPromptSelectionCandidateWire>,
    #[serde(default)]
    pub is_workflow_child: bool,
    #[serde(default)]
    pub step_name: Option<String>,
}

/// Return the basename of a `/`- or `\`-separated path.
pub fn prompt_candidate_basename(path: &str) -> &str {
    path.rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or(path)
}

/// Return true for the exact canonical and legacy raw prompt basenames.
pub fn is_raw_prompt_basename(basename: &str) -> bool {
    basename == RAW_PROMPT_BASENAME || basename == LEGACY_RAW_PROMPT_BASENAME
}

/// Return true for historical `commit_finalizer_pass_<digits>_prompt.md` files.
pub fn is_commit_finalizer_prompt_basename(basename: &str) -> bool {
    let Some(middle) = basename
        .strip_prefix(FINALIZER_PREFIX)
        .and_then(|rest| rest.strip_suffix(FINALIZER_SUFFIX))
    else {
        return false;
    };
    !middle.is_empty() && middle.bytes().all(|byte| byte.is_ascii_digit())
}

/// Return true when a basename may compete as an expanded prompt.
pub fn is_eligible_prompt_basename(basename: &str) -> bool {
    basename.ends_with("_prompt.md")
        && !is_raw_prompt_basename(basename)
        && !is_commit_finalizer_prompt_basename(basename)
}

/// Select the expanded prompt path, or `None` when no candidate is eligible.
///
/// Workflow children prefer a `-<step_name>_prompt.md` match and fall back
/// to the other eligible prompts when no step matches.
pub fn select_agent_prompt(
    request: &AgentPromptSelectionRequestWire,
) -> Option<String> {
    let mut eligible: Vec<&AgentPromptSelectionCandidateWire> = request
        .candidates
        .iter()
        .filter(|candidate| {
            is_eligible_prompt_basename(prompt_candidate_basename(
                &candidate.path,
            ))
        })
        .collect();
    if eligible.is_empty() {
        return None;
    }
    if request.is_workflow_child {
        if let Some(step_name) = request.step_name.as_deref() {
            if !step_name.is_empty() {
                let suffix = format!("-{step_name}_prompt.md");
                let stepped: Vec<&AgentPromptSelectionCandidateWire> = eligible
                    .iter()
                    .filter(|candidate| {
                        candidate.path.ends_with(suffix.as_str())
                    })
                    .copied()
                    .collect();
                if !stepped.is_empty() {
                    eligible = stepped;
                }
            }
        }
    }
    eligible
        .into_iter()
        .max_by(|left, right| {
            (left.mtime_ns, &left.path).cmp(&(right.mtime_ns, &right.path))
        })
        .map(|candidate| candidate.path.clone())
}

/// Return the agent prompt selection wire schema version.
pub fn agent_prompt_selection_wire_schema_version() -> u64 {
    AGENT_PROMPT_SELECTION_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        path: &str,
        mtime_ns: i64,
    ) -> AgentPromptSelectionCandidateWire {
        AgentPromptSelectionCandidateWire {
            path: path.to_string(),
            mtime_ns,
        }
    }

    fn request(
        candidates: Vec<AgentPromptSelectionCandidateWire>,
    ) -> AgentPromptSelectionRequestWire {
        AgentPromptSelectionRequestWire {
            candidates,
            is_workflow_child: false,
            step_name: None,
        }
    }

    #[test]
    fn raw_is_excluded_when_newer_equal_and_older() {
        for (raw_mtime, expanded_mtime) in [(200, 100), (100, 100), (100, 200)]
        {
            let req = request(vec![
                candidate("/artifacts/raw_prompt.md", raw_mtime),
                candidate("/artifacts/01_prompt.md", expanded_mtime),
            ]);
            assert_eq!(
                select_agent_prompt(&req),
                Some("/artifacts/01_prompt.md".to_string()),
                "raw mtime {raw_mtime} expanded mtime {expanded_mtime}",
            );
        }
    }

    #[test]
    fn legacy_raw_is_excluded() {
        let req = request(vec![
            candidate("/artifacts/raw_xprompt.md", 300),
            candidate("/artifacts/01_prompt.md", 100),
        ]);
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/01_prompt.md".to_string())
        );
    }

    #[test]
    fn listing_order_does_not_change_tie_winner() {
        let forward = request(vec![
            candidate("/artifacts/raw_prompt.md", 100),
            candidate("/artifacts/01_prompt.md", 100),
        ]);
        let reversed = request(vec![
            candidate("/artifacts/01_prompt.md", 100),
            candidate("/artifacts/raw_prompt.md", 100),
        ]);
        assert_eq!(
            select_agent_prompt(&forward),
            Some("/artifacts/01_prompt.md".to_string())
        );
        assert_eq!(
            select_agent_prompt(&forward),
            select_agent_prompt(&reversed)
        );
    }

    #[test]
    fn ties_prefer_lexicographically_greatest_path() {
        let req = request(vec![
            candidate("/artifacts/01_prompt.md", 100),
            candidate("/artifacts/02_prompt.md", 100),
        ]);
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/02_prompt.md".to_string())
        );
        let reversed = request(vec![
            candidate("/artifacts/02_prompt.md", 100),
            candidate("/artifacts/01_prompt.md", 100),
        ]);
        assert_eq!(select_agent_prompt(&req), select_agent_prompt(&reversed));
    }

    #[test]
    fn newest_mtime_wins() {
        let req = request(vec![
            candidate("/artifacts/02_prompt.md", 100),
            candidate("/artifacts/01_prompt.md", 200),
        ]);
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/01_prompt.md".to_string())
        );
    }

    #[test]
    fn raw_only_and_empty_have_no_selection() {
        let raw_only =
            request(vec![candidate("/artifacts/raw_prompt.md", 500)]);
        assert_eq!(select_agent_prompt(&raw_only), None);
        let legacy_only =
            request(vec![candidate("/artifacts/raw_xprompt.md", 500)]);
        assert_eq!(select_agent_prompt(&legacy_only), None);
        let empty = request(vec![]);
        assert_eq!(select_agent_prompt(&empty), None);
    }

    #[test]
    fn finalizer_prompts_are_excluded() {
        let req = request(vec![
            candidate("/artifacts/sase_prompt.md", 100),
            candidate("/artifacts/commit_finalizer_pass_1_prompt.md", 500),
        ]);
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/sase_prompt.md".to_string())
        );
        let only_finalizer = request(vec![candidate(
            "/artifacts/commit_finalizer_pass_12_prompt.md",
            500,
        )]);
        assert_eq!(select_agent_prompt(&only_finalizer), None);
    }

    #[test]
    fn workflow_step_preference_with_fallback() {
        let req = AgentPromptSelectionRequestWire {
            candidates: vec![
                candidate("/artifacts/01-other_prompt.md", 500),
                candidate("/artifacts/02-coder_prompt.md", 100),
            ],
            is_workflow_child: true,
            step_name: Some("coder".to_string()),
        };
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/02-coder_prompt.md".to_string())
        );
        let fallback = AgentPromptSelectionRequestWire {
            candidates: vec![
                candidate("/artifacts/01-other_prompt.md", 300),
                candidate("/artifacts/02-else_prompt.md", 100),
            ],
            is_workflow_child: true,
            step_name: Some("coder".to_string()),
        };
        assert_eq!(
            select_agent_prompt(&fallback),
            Some("/artifacts/01-other_prompt.md".to_string())
        );
    }

    #[test]
    fn non_workflow_ignores_step_name() {
        let req = AgentPromptSelectionRequestWire {
            candidates: vec![
                candidate("/artifacts/01-other_prompt.md", 500),
                candidate("/artifacts/02-coder_prompt.md", 100),
            ],
            is_workflow_child: false,
            step_name: Some("coder".to_string()),
        };
        assert_eq!(
            select_agent_prompt(&req),
            Some("/artifacts/01-other_prompt.md".to_string())
        );
    }
}
