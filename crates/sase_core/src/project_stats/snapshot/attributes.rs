//! Committed Linguist attributes via isolated `check-attr --source`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::issue;
use crate::project_stats::classify::{AttributeState, CommittedAttributes};
use crate::project_stats::git::{self, ResolvedRepo};
use crate::project_stats::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
};

const ATTR_GENERATED: &[u8] = b"linguist-generated";
const ATTR_VENDORED: &[u8] = b"linguist-vendored";
const ATTR_DOCUMENTATION: &[u8] = b"linguist-documentation";
const ISOLATED_INIT_ENV: &[(&str, &str)] = &[
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_CONFIG_SYSTEM", "/dev/null"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
];

fn isolated_init_args(object_format: &str) -> Vec<String> {
    vec![
        "init".to_owned(),
        "--bare".to_owned(),
        "--template=".to_owned(),
        format!("--object-format={object_format}"),
    ]
}

pub(crate) struct AttrSession {
    _temp: tempfile::TempDir,
    git_dir: PathBuf,
    empty_attributes: PathBuf,
}

impl AttrSession {
    pub(crate) fn open(
        repo: &ResolvedRepo,
        spec: &ProjectStatsRepoSpecWire,
        budget: &ProjectStatsBudgetWire,
    ) -> Result<Self, ProjectStatsIssueWire> {
        let repo_path = Path::new(&repo.canonical_path);
        let objects = git::run_checked(
            repo_path,
            &["rev-parse", "--git-path", "objects"],
            budget,
        )
        .and_then(|result| {
            let text = String::from_utf8(result.stdout).map_err(|error| {
                format!("Git returned a non-UTF-8 objects path: {error}")
            })?;
            let path = PathBuf::from(text.trim());
            let path = if path.is_absolute() {
                path
            } else {
                repo_path.join(path)
            };
            path.canonicalize().map_err(|error| {
                format!("cannot canonicalize Git objects directory: {error}")
            })
        })
        .map_err(|message| {
            issue(
                spec,
                "attr_unavailable",
                format!("cannot locate the repository object store: {message}"),
                None,
            )
        })?;

        let temp = tempfile::tempdir().map_err(|error| {
            issue(
                spec,
                "attr_unavailable",
                format!("cannot create an isolated Git directory: {error}"),
                None,
            )
        })?;
        let format_result = git::run_checked(
            repo_path,
            &["rev-parse", "--show-object-format"],
            budget,
        )
        .map_err(|message| {
            issue(
                spec,
                "attr_unavailable",
                format!("cannot read repository object format: {message}"),
                None,
            )
        })?;
        let object_format = String::from_utf8(format_result.stdout)
            .map_err(|error| {
                issue(
                    spec,
                    "attr_unavailable",
                    format!("Git returned an invalid object format: {error}"),
                    None,
                )
            })?
            .trim()
            .to_owned();
        if !matches!(object_format.as_str(), "sha1" | "sha256") {
            return Err(issue(
                spec,
                "attr_unavailable",
                format!("unsupported Git object format: {object_format}"),
                None,
            ));
        }
        let args = isolated_init_args(&object_format);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let initialized = git::run_unchecked_with_stdin_env(
            temp.path(),
            &args,
            b"",
            ISOLATED_INIT_ENV,
            budget,
        )
        .map_err(|message| {
            issue(
                spec,
                "attr_unavailable",
                format!(
                    "cannot initialize an isolated Git directory: {message}"
                ),
                None,
            )
        })?;
        if initialized.truncated || initialized.code != Some(0) {
            return Err(issue(
                spec,
                "attr_unavailable",
                "cannot initialize an isolated Git directory".to_owned(),
                None,
            ));
        }
        let alternates = temp.path().join("objects/info/alternates");
        if let Some(parent) = alternates.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                issue(
                    spec,
                    "attr_unavailable",
                    format!("cannot prepare object alternates: {error}"),
                    None,
                )
            })?;
        }
        fs::write(&alternates, format!("{}\n", objects.display())).map_err(
            |error| {
                issue(
                    spec,
                    "attr_unavailable",
                    format!("cannot write object alternates: {error}"),
                    None,
                )
            },
        )?;
        let empty_attributes = temp.path().join("empty-attributes");
        fs::write(&empty_attributes, b"").map_err(|error| {
            issue(
                spec,
                "attr_unavailable",
                format!("cannot write the empty attributes file: {error}"),
                None,
            )
        })?;
        let git_dir = temp.path().to_path_buf();
        Ok(Self {
            _temp: temp,
            git_dir,
            empty_attributes,
        })
    }

    pub(crate) fn lookup(
        &self,
        repo: &ResolvedRepo,
        spec: &ProjectStatsRepoSpecWire,
        budget: &ProjectStatsBudgetWire,
        paths: &[Vec<u8>],
    ) -> Result<HashMap<Vec<u8>, CommittedAttributes>, ProjectStatsIssueWire>
    {
        let mut attributes = HashMap::new();
        if paths.is_empty() {
            return Ok(attributes);
        }
        for batch in path_batches(paths, budget.max_output_bytes.max(1)) {
            let stdin = nul_join(batch);
            let git_dir = self.git_dir.to_string_lossy();
            let empty = self.empty_attributes.to_string_lossy();
            let attributes_flag = format!("core.attributesFile={empty}");
            let args = [
                "--git-dir",
                git_dir.as_ref(),
                "-c",
                attributes_flag.as_str(),
                "check-attr",
                "--source",
                &repo.tip,
                "-z",
                "--stdin",
                "linguist-generated",
                "linguist-vendored",
                "linguist-documentation",
            ];
            let result = git::run_unchecked_with_stdin_env(
                Path::new(&repo.canonical_path),
                &args,
                &stdin,
                &[
                    ("GIT_ATTR_NOSYSTEM", "1"),
                    ("GIT_CONFIG_NOSYSTEM", "1"),
                    ("GIT_CONFIG_GLOBAL", "/dev/null"),
                    ("GIT_CONFIG_SYSTEM", "/dev/null"),
                ],
                budget,
            )
            .map_err(|message| attr_issue(spec, message))?;
            if result.truncated {
                return Err(attr_issue(
                    spec,
                    "git check-attr exceeded its command budget".to_owned(),
                ));
            }
            if result.code != Some(0) {
                return Err(issue(
                    spec,
                    "attr_unavailable",
                    "committed-attribute lookup is unsupported or failed"
                        .to_owned(),
                    Some(
                        "Install Git 2.40+ so check-attr --source can read committed .gitattributes without consulting the worktree.",
                    ),
                ));
            }
            let parsed = parse_check_attr(&result.stdout).map_err(|message| {
                attr_issue(
                    spec,
                    format!("committed-attribute output is malformed: {message}"),
                )
            })?;
            for path in batch {
                let Some(value) = parsed.get(path) else {
                    return Err(attr_issue(
                        spec,
                        format!(
                            "git check-attr omitted path {}",
                            String::from_utf8_lossy(path)
                        ),
                    ));
                };
                attributes.insert((*path).to_vec(), *value);
            }
        }
        Ok(attributes)
    }
}

fn attr_issue(
    spec: &ProjectStatsRepoSpecWire,
    message: String,
) -> ProjectStatsIssueWire {
    issue(
        spec,
        "attr_unavailable",
        message,
        Some(
            "Committed Linguist attributes must come from the selected tree; the working tree is not consulted.",
        ),
    )
}

fn path_batches(paths: &[Vec<u8>], max_output_bytes: u64) -> Vec<&[Vec<u8>]> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut used = 0u64;
    for (index, path) in paths.iter().enumerate() {
        let cost = 3 * (path.len() as u64 + 48);
        if start < index && used.saturating_add(cost) > max_output_bytes {
            batches.push(&paths[start..index]);
            start = index;
            used = 0;
        }
        used = used.saturating_add(cost);
    }
    if start < paths.len() {
        batches.push(&paths[start..]);
    }
    batches
}

fn nul_join(paths: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for path in paths {
        out.extend_from_slice(path);
        out.push(0);
    }
    out
}

pub(crate) fn parse_check_attr(
    bytes: &[u8],
) -> Result<HashMap<Vec<u8>, CommittedAttributes>, String> {
    let mut fields = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let Some(end) = rest.iter().position(|byte| *byte == 0) else {
            return Err("unterminated check-attr field".to_owned());
        };
        fields.push(&rest[..end]);
        rest = &rest[end + 1..];
    }
    if fields.len() % 3 != 0 {
        return Err(
            "check-attr output is not a multiple of three fields".to_owned()
        );
    }
    let mut parsed: HashMap<Vec<u8>, CommittedAttributes> = HashMap::new();
    for triple in fields.as_chunks::<3>().0 {
        let path = triple[0].to_vec();
        let name = triple[1];
        let value = AttributeState::parse(triple[2]).map_err(|_| {
            format!(
                "unsupported Linguist attribute value {}",
                String::from_utf8_lossy(triple[2])
            )
        })?;
        let entry = parsed.entry(path).or_default();
        match name {
            ATTR_GENERATED => entry.generated = value,
            ATTR_VENDORED => entry.vendored = value,
            ATTR_DOCUMENTATION => entry.documentation = value,
            _ => {}
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn isolated_init_uses_object_format_and_disables_user_configuration() {
        assert_eq!(
            isolated_init_args("sha256"),
            ["init", "--bare", "--template=", "--object-format=sha256"]
        );
        assert_eq!(
            ISOLATED_INIT_ENV,
            [
                ("GIT_CONFIG_GLOBAL", "/dev/null"),
                ("GIT_CONFIG_SYSTEM", "/dev/null"),
                ("GIT_CONFIG_NOSYSTEM", "1"),
            ]
        );
    }

    #[test]
    fn empty_template_argument_disables_template_copying() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let template = temp.path().join("template");
        std::fs::create_dir_all(template.join("info"))
            .expect("template info directory");
        std::fs::write(
            template.join("info/attributes"),
            "*.rs linguist-generated\n",
        )
        .expect("template attributes");
        let config = temp.path().join("global.config");
        let configured = Command::new("git")
            .args(["config", "--file"])
            .arg(&config)
            .args(["init.templateDir"])
            .arg(&template)
            .status()
            .expect("write global template config");
        assert!(configured.success());

        let target = temp.path().join("isolated.git");
        let mut command = Command::new("git");
        command
            .args(isolated_init_args("sha1"))
            .arg(&target)
            .env("GIT_CONFIG_GLOBAL", &config)
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1");
        let initialized = command.status().expect("initialize isolated repo");
        assert!(initialized.success());
        assert!(!target.join("info/attributes").exists());
    }

    #[test]
    fn parses_nul_triplets_for_the_three_linguist_attributes() {
        let mut bytes = Vec::new();
        for (path, attr, value) in [
            (
                b"src/lib.rs".as_slice(),
                ATTR_GENERATED,
                b"unspecified".as_slice(),
            ),
            (b"src/lib.rs".as_slice(), ATTR_VENDORED, b"false".as_slice()),
            (
                b"src/lib.rs".as_slice(),
                ATTR_DOCUMENTATION,
                b"set".as_slice(),
            ),
        ] {
            bytes.extend_from_slice(path);
            bytes.push(0);
            bytes.extend_from_slice(attr);
            bytes.push(0);
            bytes.extend_from_slice(value);
            bytes.push(0);
        }
        let parsed = parse_check_attr(&bytes).expect("parse");
        let attrs = parsed.get(b"src/lib.rs".as_slice()).expect("path");
        assert_eq!(attrs.generated, AttributeState::Unspecified);
        assert_eq!(attrs.vendored, AttributeState::Disabled);
        assert_eq!(attrs.documentation, AttributeState::Enabled);
    }

    #[test]
    fn rejects_unknown_attribute_values() {
        let mut bytes = b"file.rs".to_vec();
        bytes.push(0);
        bytes.extend_from_slice(ATTR_GENERATED);
        bytes.push(0);
        bytes.extend_from_slice(b"maybe");
        bytes.push(0);
        assert!(parse_check_attr(&bytes).is_err());
    }
}
