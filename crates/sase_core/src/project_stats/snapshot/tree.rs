//! Committed-tree enumeration via `ls-tree -r -z -l`.

#![allow(dead_code)] // Reached through measure_code_snapshot.

use std::path::Path;

use super::issue;
use crate::project_stats::git::{self, ResolvedRepo};
use crate::project_stats::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub mode: u32,
    pub kind: TreeEntryKind,
    pub oid: String,
    pub size: u64,
    pub path: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TreeEntryKind {
    Blob,
    Commit,
    Tree,
}

impl TreeEntry {
    pub(crate) fn classify_mode(&self) -> u32 {
        match self.mode & 0o170000 {
            0o120000 => 0o120000,
            0o160000 => 0o160000,
            0o100000 => {
                if self.mode & 0o111 != 0 {
                    0o100755
                } else {
                    0o100644
                }
            }
            _ => self.mode,
        }
    }

    pub(crate) fn is_regular_blob(&self) -> bool {
        self.kind == TreeEntryKind::Blob && (self.mode & 0o170000) == 0o100000
    }

    pub(crate) fn is_symlink(&self) -> bool {
        self.kind == TreeEntryKind::Blob && (self.mode & 0o170000) == 0o120000
    }

    pub(crate) fn is_gitlink(&self) -> bool {
        self.kind == TreeEntryKind::Commit || (self.mode & 0o170000) == 0o160000
    }
}

pub(crate) fn read_tree_oid(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
) -> Result<String, ProjectStatsIssueWire> {
    let expression = format!("{}^{{tree}}", repo.tip);
    let result = git::run_checked(
        Path::new(&repo.canonical_path),
        &["rev-parse", "--verify", "--end-of-options", &expression],
        budget,
    )
    .map_err(|message| {
        issue(
            spec,
            "tree_unavailable",
            format!("cannot resolve the committed tree: {message}"),
            Some("Verify the selected commit is present in the local object store."),
        )
    })?;
    let oid = String::from_utf8_lossy(&result.stdout)
        .trim()
        .to_ascii_lowercase();
    if !git::valid_oid(&oid) {
        return Err(issue(
            spec,
            "tree_unavailable",
            "Git returned an invalid tree object id".to_owned(),
            None,
        ));
    }
    Ok(oid)
}

pub(crate) fn list_tree(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
) -> Result<Vec<TreeEntry>, ProjectStatsIssueWire> {
    let result = git::run_checked(
        Path::new(&repo.canonical_path),
        &["ls-tree", "-r", "-z", "-l", "--end-of-options", &repo.tip],
        budget,
    )
    .map_err(|message| {
        issue(
            spec,
            "tree_unavailable",
            format!("cannot enumerate the committed tree: {message}"),
            Some(
                "Increase the query output budget if the repository is large.",
            ),
        )
    })?;
    parse_ls_tree(&result.stdout).map_err(|message| {
        issue(
            spec,
            "tree_unavailable",
            format!("committed tree listing is malformed: {message}"),
            None,
        )
    })
}

pub(crate) fn parse_ls_tree(bytes: &[u8]) -> Result<Vec<TreeEntry>, String> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let Some(end) = rest.iter().position(|byte| *byte == 0) else {
            return Err("unterminated ls-tree record".to_owned());
        };
        let record = &rest[..end];
        rest = &rest[end + 1..];
        if record.is_empty() {
            continue;
        }
        entries.push(parse_ls_tree_record(record)?);
    }
    Ok(entries)
}

fn parse_ls_tree_record(record: &[u8]) -> Result<TreeEntry, String> {
    let (mode_bytes, rest) = split_space(record)
        .ok_or_else(|| "ls-tree record is missing a mode".to_owned())?;
    let (kind_bytes, rest) = split_space(rest)
        .ok_or_else(|| "ls-tree record is missing a type".to_owned())?;
    let (oid_bytes, rest) = split_space(rest)
        .ok_or_else(|| "ls-tree record is missing an object id".to_owned())?;
    let tab = rest
        .iter()
        .position(|byte| *byte == b'\t')
        .ok_or_else(|| "ls-tree record is missing a path".to_owned())?;
    let size_bytes = &rest[..tab];
    let path = rest[tab + 1..].to_vec();
    if path.is_empty() {
        return Err("ls-tree record has an empty path".to_owned());
    }
    let mode_text = std::str::from_utf8(mode_bytes)
        .map_err(|_| "ls-tree mode is not UTF-8".to_owned())?;
    let mode = u32::from_str_radix(mode_text, 8)
        .map_err(|_| format!("ls-tree mode is invalid: {mode_text}"))?;
    let kind = match kind_bytes {
        b"blob" => TreeEntryKind::Blob,
        b"commit" => TreeEntryKind::Commit,
        b"tree" => TreeEntryKind::Tree,
        other => {
            return Err(format!(
                "ls-tree type is invalid: {}",
                String::from_utf8_lossy(other)
            ));
        }
    };
    let oid = std::str::from_utf8(oid_bytes)
        .map_err(|_| "ls-tree object id is not UTF-8".to_owned())?
        .to_ascii_lowercase();
    if !git::valid_oid(&oid) {
        return Err(format!("ls-tree object id is invalid: {oid}"));
    }
    let size_text = std::str::from_utf8(size_bytes)
        .map_err(|_| "ls-tree size is not UTF-8".to_owned())?
        .trim();
    let size = if size_text == "-" {
        0
    } else {
        size_text
            .parse::<u64>()
            .map_err(|_| format!("ls-tree size is invalid: {size_text}"))?
    };
    Ok(TreeEntry {
        mode,
        kind,
        oid,
        size,
        path,
    })
}

fn split_space(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let index = bytes.iter().position(|byte| *byte == b' ')?;
    Some((&bytes[..index], &bytes[index + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nul_separated_paths_without_whitespace_splitting() {
        let mut bytes = b"100644 blob ".to_vec();
        bytes.extend_from_slice(&[b'a'; 40]);
        bytes.extend_from_slice(b"      12\tsrc/odd name.rs\0");
        bytes.extend_from_slice(b"160000 commit ");
        bytes.extend_from_slice(&[b'b'; 40]);
        bytes.extend_from_slice(b" -\tvendor/dep\0");
        bytes.extend_from_slice(b"120000 blob ");
        bytes.extend_from_slice(&[b'c'; 40]);
        bytes.extend_from_slice(b" 7\tlink\0");
        let entries = parse_ls_tree(&bytes).expect("parse");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, b"src/odd name.rs");
        assert_eq!(entries[0].size, 12);
        assert!(entries[0].is_regular_blob());
        assert_eq!(entries[0].classify_mode(), 0o100644);
        assert!(entries[1].is_gitlink());
        assert!(entries[2].is_symlink());
    }

    #[test]
    fn preserves_non_utf8_paths() {
        let mut bytes = b"100644 blob ".to_vec();
        bytes.extend_from_slice(&[b'd'; 40]);
        bytes.extend_from_slice(b" 1\t");
        bytes.extend_from_slice(b"src/\xff.rs");
        bytes.push(0);
        let entries = parse_ls_tree(&bytes).expect("parse");
        assert_eq!(entries[0].path, b"src/\xff.rs");
    }

    #[test]
    fn rejects_unterminated_and_malformed_records() {
        assert!(parse_ls_tree(b"100644 blob ").is_err());
        let mut bytes = b"bad blob ".to_vec();
        bytes.extend_from_slice(&[b'a'; 40]);
        bytes.extend_from_slice(b" 1\tfile\0");
        assert!(parse_ls_tree(&bytes).is_err());
    }
}
