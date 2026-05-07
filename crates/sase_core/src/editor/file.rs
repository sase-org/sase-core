use std::fs;
use std::path::{Path, PathBuf};

use super::wire::{CompletionCandidate, CompletionList};

pub fn build_file_completion_candidates(token: &str) -> CompletionList {
    build_file_completion_candidates_with_base(token, None)
}

pub fn build_file_completion_candidates_with_base(
    token: &str,
    base_dir: Option<&Path>,
) -> CompletionList {
    let (at_prefix, token) = token
        .strip_prefix('@')
        .map(|rest| ("@", rest))
        .unwrap_or(("", token));
    let Some((raw_dir, expanded_dir, partial)) =
        split_completion_path(token, base_dir)
    else {
        return empty_list();
    };
    let show_dotfiles = partial.starts_with('.');

    let entries = match fs::read_dir(&expanded_dir) {
        Ok(entries) => entries,
        Err(_) => return empty_list(),
    };

    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && !show_dotfiles {
            continue;
        }
        if !name.to_lowercase().starts_with(&partial.to_lowercase()) {
            continue;
        }
        let is_dir = entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false)
            || entry.path().is_dir();
        let display = if is_dir {
            format!("{name}/")
        } else {
            name.clone()
        };
        candidates.push(CompletionCandidate {
            display: display.clone(),
            insertion: format!("{at_prefix}{raw_dir}{display}"),
            detail: None,
            documentation: None,
            is_dir,
            name,
            replacement: None,
        });
    }

    candidates.sort_by(|a, b| {
        (!a.is_dir, a.name.to_lowercase(), a.name.clone()).cmp(&(
            !b.is_dir,
            b.name.to_lowercase(),
            b.name.clone(),
        ))
    });

    let shared_extension = shared_name_extension(&candidates, &partial);
    CompletionList {
        candidates,
        shared_extension,
    }
}

pub fn build_file_history_completion_candidates<I, S>(
    paths: I,
) -> CompletionList
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    CompletionList {
        candidates: paths
            .into_iter()
            .map(|path| {
                let path = path.into();
                CompletionCandidate {
                    display: path.clone(),
                    insertion: path.clone(),
                    detail: None,
                    documentation: None,
                    is_dir: false,
                    name: path,
                    replacement: None,
                }
            })
            .collect(),
        shared_extension: String::new(),
    }
}

fn split_completion_path(
    token: &str,
    base_dir: Option<&Path>,
) -> Option<(String, PathBuf, String)> {
    let (raw_dir, partial) = if token.ends_with('/') {
        (token.to_string(), String::new())
    } else {
        let slash = token.rfind('/')?;
        (token[..=slash].to_string(), token[slash + 1..].to_string())
    };

    let expanded_dir = expand_dir(&raw_dir, base_dir);
    Some((raw_dir, expanded_dir, partial))
}

fn expand_dir(raw_dir: &str, base_dir: Option<&Path>) -> PathBuf {
    let expanded = if raw_dir == "~/" {
        home_dir().unwrap_or_else(|| PathBuf::from(raw_dir))
    } else if let Some(rest) = raw_dir.strip_prefix("~/") {
        home_dir()
            .map(|home| home.join(rest))
            .unwrap_or_else(|| PathBuf::from(raw_dir))
    } else {
        PathBuf::from(raw_dir)
    };
    if expanded.is_absolute() {
        expanded
    } else {
        base_dir.unwrap_or_else(|| Path::new(".")).join(expanded)
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn shared_name_extension(
    candidates: &[CompletionCandidate],
    partial: &str,
) -> String {
    if candidates.len() <= 1 {
        return String::new();
    }
    let mut prefix = candidates[0].name.clone();
    for candidate in &candidates[1..] {
        prefix = common_prefix(&prefix, &candidate.name);
        if prefix.len() <= partial.len() {
            return String::new();
        }
    }
    prefix.get(partial.len()..).unwrap_or_default().to_string()
}

fn common_prefix(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_ch), (_, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if left_ch != right_ch {
            break;
        }
        end = left_idx + left_ch.len_utf8();
    }
    left[..end].to_string()
}

fn empty_list() -> CompletionList {
    CompletionList {
        candidates: Vec::new(),
        shared_extension: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn orders_directories_first_and_filters_dotfiles() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("script.py"), "").unwrap();
        fs::write(dir.path().join(".secret"), "").unwrap();

        let list =
            build_file_completion_candidates_with_base("./s", Some(dir.path()));
        assert_eq!(
            list.candidates
                .iter()
                .map(|c| (c.display.as_str(), c.is_dir))
                .collect::<Vec<_>>(),
            vec![("src/", true), ("script.py", false)]
        );
        assert_eq!(list.shared_extension, "");

        let hidden =
            build_file_completion_candidates_with_base("./.", Some(dir.path()));
        assert_eq!(hidden.candidates[0].display, ".secret");
    }

    #[test]
    fn preserves_at_prefix_and_marks_symlinked_directories() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("target_dir")).unwrap();
        symlink(
            dir.path().join("target_dir"),
            dir.path().join("target_link"),
        )
        .unwrap();

        let list = build_file_completion_candidates_with_base(
            "@./target",
            Some(dir.path()),
        );
        assert_eq!(list.candidates.len(), 2);
        assert!(list
            .candidates
            .iter()
            .all(|c| c.insertion.starts_with("@./")));
        assert!(list
            .candidates
            .iter()
            .any(|c| c.name == "target_link" && c.is_dir));
        assert_eq!(list.shared_extension, "_");
    }
}
