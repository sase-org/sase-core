//! SASE secret-file and zone tables for attachment audience decisions.
//!
//! All tables are stored relative to `home` and `sase_home` facts supplied
//! by Python. Comparisons are lexical (neither side canonicalizes
//! symlinks); callers pass resolved absolute paths.

/// Secret files under `sase_home` whose exact bytes must never go public.
///
/// Derived from the sase tree: the Telegram token file, fleet/gateway
/// credential stores, and the mobile-gateway state. Scanner reads their
/// trimmed contents as known values; the decision table refuses them.
pub const SASE_SECRET_FILES: &[&str] = &[
    "telegram_bot_token",
    "fleet/credentials.json",
    "gateway/credentials.json",
    "gateway/token",
    "mobile_gateway/credentials.json",
    "mobile_gateway/service_account.json",
];

/// Personal-content zones under `sase_home` (private provenance).
///
/// Covers telegram, notifications, mobile gateway, prompt/command history,
// chats, and projects. Publishable output lives under
/// [`SASE_PUBLISHABLE_DIRS`], never here.
pub const SASE_PERSONAL_DIRS: &[&str] = &[
    "telegram",
    "notifications",
    "mobile_gateway",
    "chats",
    "projects",
];

/// Personal-content files under `sase_home` (private provenance).
pub const SASE_PERSONAL_FILES: &[&str] = &[
    "prompt_history.json",
    "prompt_stash.jsonl",
    "command_history.json",
    "command_line_history.json",
    "hook_history.json",
    "file_reference_history.json",
    "query_history.json",
    "prompt_misspellings.json",
    "prompt_word_deletions.json",
    "prompt_placeholders.json",
    "vcs_xprompt_mru.json",
    "xprompt_save_state.json",
];

/// Publishable output zones under `sase_home` (public evidence).
///
/// Logs, tool-run logs, perf traces, and TUI screenshot outputs. Managed-tmp
/// screenshot roots arrive via `scratch_roots` facts instead.
pub const SASE_PUBLISHABLE_DIRS: &[&str] = &[
    "logs",
    "tools",
    "bead_push_logs",
    "perf",
    "traces",
    "screenshots",
];

/// Personal/config zones under `home` (private provenance).
///
/// Covers `~/.config`, `~/.local/share`, `~/Documents`, `~/Downloads`,
/// `~/Desktop`, mail, chat, and notes vaults.
pub const PERSONAL_ZONE_DIRS: &[&str] = &[
    ".config",
    ".local/share",
    "Documents",
    "Downloads",
    "Desktop",
    "Mail",
    "mail",
    ".mail",
    ".thunderbird",
    ".evolution",
    "chat",
    "chats",
    "Chat",
    "Chats",
    ".chat",
    "Notes",
    "notes",
    "Obsidian",
    "obsidian",
    ".obsidian",
    "vault",
    "Vault",
];

fn trim_slash(value: &str) -> &str {
    value.trim_end_matches('/')
}

fn join_home(base: &str, rel: &str) -> String {
    let base = trim_slash(base);
    if base.is_empty() {
        return rel.to_string();
    }
    format!("{base}/{rel}")
}

fn path_is_within(path: &str, dir: &str) -> bool {
    let path = trim_slash(path);
    let dir = trim_slash(dir);
    if dir.is_empty() {
        return false;
    }
    path == dir || path.starts_with(&format!("{dir}/"))
}

/// Absolute paths of the SASE secret files for `sase_home`.
pub fn sase_secret_file_paths(sase_home: &str) -> Vec<String> {
    let base = trim_slash(sase_home);
    if base.is_empty() {
        return Vec::new();
    }
    SASE_SECRET_FILES
        .iter()
        .map(|rel| join_home(base, rel))
        .collect()
}

/// True when `path` is exactly one of the SASE secret files.
pub fn is_sase_secret_file(path: &str, sase_home: &str) -> bool {
    let base = trim_slash(sase_home);
    if base.is_empty() {
        return false;
    }
    let path = trim_slash(path);
    SASE_SECRET_FILES
        .iter()
        .any(|rel| path == join_home(base, rel))
}

/// True when `path` sits in a SASE personal-content zone.
pub fn is_sase_personal_path(path: &str, sase_home: &str) -> bool {
    let base = trim_slash(sase_home);
    if base.is_empty() {
        return false;
    }
    for dir in SASE_PERSONAL_DIRS {
        if path_is_within(path, &join_home(base, dir)) {
            return true;
        }
    }
    let path = trim_slash(path);
    for file in SASE_PERSONAL_FILES {
        if path == join_home(base, file) {
            return true;
        }
    }
    false
}

/// True when `path` sits in a SASE publishable output zone.
pub fn is_sase_publishable_path(path: &str, sase_home: &str) -> bool {
    let base = trim_slash(sase_home);
    if base.is_empty() {
        return false;
    }
    for dir in SASE_PUBLISHABLE_DIRS {
        if path_is_within(path, &join_home(base, dir)) {
            return true;
        }
    }
    false
}

/// True when `path` sits in a personal/config zone under `home`.
pub fn is_personal_zone_path(path: &str, home: &str) -> bool {
    let base = trim_slash(home);
    if base.is_empty() {
        return false;
    }
    for dir in PERSONAL_ZONE_DIRS {
        if path_is_within(path, &join_home(base, dir)) {
            return true;
        }
    }
    false
}

/// True when `path` sits under `root` (workspace or scratch).
pub fn path_is_within_root(path: &str, root: &str) -> bool {
    path_is_within(path, root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_files_match_exactly() {
        assert!(is_sase_secret_file(
            "/home/bryan/.sase/telegram_bot_token",
            "/home/bryan/.sase"
        ));
        assert!(is_sase_secret_file(
            "/home/bryan/.sase/fleet/credentials.json",
            "/home/bryan/.sase"
        ));
        assert!(!is_sase_secret_file(
            "/home/bryan/.sase/telegram_bot_token.bak",
            "/home/bryan/.sase"
        ));
        assert!(!is_sase_secret_file(
            "/home/bryan/.sase/logs/build.log",
            "/home/bryan/.sase"
        ));
        assert!(
            sase_secret_file_paths("/home/bryan/.sase").len()
                == SASE_SECRET_FILES.len()
        );
    }

    #[test]
    fn personal_and_publishable_zones() {
        let sase_home = "/home/bryan/.sase";
        for path in [
            "/home/bryan/.sase/telegram/agent.json",
            "/home/bryan/.sase/notifications/notifications.jsonl",
            "/home/bryan/.sase/mobile_gateway/state.json",
            "/home/bryan/.sase/prompt_history.json",
            "/home/bryan/.sase/command_history.json",
            "/home/bryan/.sase/chats/abc.md",
        ] {
            assert!(is_sase_personal_path(path, sase_home), "{path}");
            assert!(!is_sase_publishable_path(path, sase_home), "{path}");
        }
        for path in [
            "/home/bryan/.sase/logs/build.log",
            "/home/bryan/.sase/tools/run-123.jsonl",
            "/home/bryan/.sase/perf/trace.json",
            "/home/bryan/.sase/screenshots/shot.png",
        ] {
            assert!(is_sase_publishable_path(path, sase_home), "{path}");
            assert!(!is_sase_personal_path(path, sase_home), "{path}");
        }
    }

    #[test]
    fn home_personal_zones() {
        let home = "/home/bryan";
        for path in [
            "/home/bryan/.config/gh/hosts.yml",
            "/home/bryan/.local/share/sase/projects/x",
            "/home/bryan/Documents/notes.md",
            "/home/bryan/Downloads/report.pdf",
            "/home/bryan/Desktop/shot.png",
            "/home/bryan/Mail/inbox",
            "/home/bryan/Notes/vault.md",
        ] {
            assert!(is_personal_zone_path(path, home), "{path}");
        }
        assert!(!is_personal_zone_path(
            "/home/bryan/work/repo/main.py",
            home
        ));
        assert!(!is_personal_zone_path("/tmp/build.log", home));
    }
}
