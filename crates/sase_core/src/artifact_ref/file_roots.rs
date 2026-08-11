//! Resolution for `@file:<path>` payloads against explicit file roots.

use std::fs::{self, File, Metadata};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

use super::filter::ArtifactPathFilter;
use super::{
    artifact_path_payload, filtered_resolution, paths_to_strings, resolution,
    validate_file_path_payload, validate_locator_label, ArtifactRefContextWire,
    ArtifactRefError, ArtifactRefResolutionWire,
};

pub fn resolve_artifact_file_path(
    path: &str,
    context: &ArtifactRefContextWire,
    rendered: String,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    validate_file_path_payload(path)?;
    let candidate = match expand_file_path_payload(path, context) {
        Ok(path) => path,
        Err(diagnostic) => return Ok(denied(rendered, diagnostic)),
    };
    if context.file_roots.is_empty() {
        return Ok(filtered_without_path(
            rendered,
            "configure artifact_refs.file.roots before using @file references",
        ));
    }

    let candidate_canonical = canonicalize_existing_or_parent(&candidate)
        .map_err(|error| {
            ArtifactRefError::io(format!(
                "could not resolve @file candidate path: {error}"
            ))
        })?;
    let mut roots = Vec::new();
    let mut unavailable_roots = Vec::new();
    for root in &context.file_roots {
        validate_locator_label(&root.name, "file root")?;
        let filter = ArtifactPathFilter::compile(root.path_globs.as_deref())?;
        match fs::canonicalize(&root.path) {
            Ok(canonical) => roots.push(ResolvedFileRoot {
                name: root.name.clone(),
                canonical,
                filter,
            }),
            Err(_) => unavailable_roots.push(root.name.clone()),
        }
    }
    if roots.is_empty() {
        let names = unavailable_roots.join(", ");
        return Ok(filtered_without_path(
            rendered,
            &format!(
                "configured artifact_refs.file.roots are unavailable: {names}"
            ),
        ));
    }

    let matches = matching_roots(&candidate_canonical, &roots);
    if matches.is_empty() {
        if let Some(root_name) =
            symlink_escape_root(&candidate, &candidate_canonical, &roots)
        {
            return Ok(denied(
                rendered,
                format!(
                    "@file path is a symlink that escapes configured root {root_name}"
                ),
            ));
        }
        let names = roots
            .iter()
            .map(|root| root.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(filtered_without_path(
            rendered,
            &format!(
                "@file path is outside configured artifact_refs.file.roots ({names})"
            ),
        ));
    }
    if matches.len() > 1 {
        let mut resolved = resolution("ambiguous", rendered);
        resolved.candidates =
            matches.iter().map(|root| root.name.clone()).collect();
        return Ok(resolved);
    }

    let root = matches[0];
    let relative =
        candidate_canonical
            .strip_prefix(&root.canonical)
            .map_err(|_| {
                ArtifactRefError::validation("file root containment drifted")
            })?;
    if relative.as_os_str().is_empty() {
        return Ok(denied(
            rendered,
            "@file path resolves to a configured root directory",
        ));
    }
    let relative_payload = artifact_path_payload(relative)?;
    if !root.filter.allows(&relative_payload)? {
        return Ok(filtered_resolution(
            "file",
            &relative_payload,
            rendered,
            root.filter.summary(),
        ));
    }

    let symlink_metadata = match fs::symlink_metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ArtifactRefResolutionWire {
                schema_version:
                    super::ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
                status: "missing".to_string(),
                rendered,
                locator: None,
                resolved_path: None,
                candidates: paths_to_strings(vec![candidate]),
                diagnostic: None,
            });
        }
        Err(error) => {
            return Ok(denied(
                rendered,
                format!("@file path cannot be inspected: {error}"),
            ));
        }
    };
    if symlink_metadata.file_type().is_symlink()
        && !contains_path(&candidate_canonical, &root.canonical)
    {
        return Ok(denied(
            rendered,
            format!(
                "@file path is a symlink that escapes configured root {}",
                root.name
            ),
        ));
    }

    let metadata = match fs::metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(denied(
                rendered,
                format!("@file path cannot be read: {error}"),
            ));
        }
    };
    if let Some(kind) = denied_file_type(&metadata) {
        return Ok(denied(
            rendered,
            format!("@file path must be a regular file, not {kind}"),
        ));
    }
    if let Err(error) = File::open(&candidate) {
        return Ok(denied(
            rendered,
            format!("@file path cannot be opened for reading: {error}"),
        ));
    }
    if let Some(limit) = context.file_capture_max_bytes {
        let size = metadata.len();
        if size > limit {
            return Ok(denied(
                rendered,
                format!(
                    "@file path is too large to capture: {size} bytes > {limit} bytes"
                ),
            ));
        }
    }

    Ok(ArtifactRefResolutionWire {
        schema_version: super::ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: "exact".to_string(),
        rendered,
        locator: Some(format!("{}:{relative_payload}", root.name)),
        resolved_path: Some(candidate_canonical.to_string_lossy().into_owned()),
        candidates: Vec::new(),
        diagnostic: None,
    })
}

#[derive(Debug)]
struct ResolvedFileRoot {
    name: String,
    canonical: PathBuf,
    filter: ArtifactPathFilter,
}

fn expand_file_path_payload(
    path: &str,
    context: &ArtifactRefContextWire,
) -> Result<PathBuf, String> {
    if path == "~" || path.starts_with("~/") {
        let home = context
            .home_dir
            .as_deref()
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
            .ok_or_else(|| {
                "@file:~ needs artifact reference context home_dir".to_string()
            })?;
        let suffix = path.strip_prefix("~/").unwrap_or("");
        return Ok(if suffix.is_empty() {
            home
        } else {
            home.join(suffix)
        });
    }
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        return Ok(candidate);
    }
    Err("@file: needs an absolute or ~/ path".to_string())
}

fn canonicalize_existing_or_parent(path: &Path) -> std::io::Result<PathBuf> {
    if let Ok(canonical) = fs::canonicalize(path) {
        return Ok(canonical);
    }
    let mut ancestor = path;
    let mut suffix = Vec::new();
    loop {
        if let Some(name) = ancestor.file_name() {
            suffix.push(name.to_os_string());
        }
        let Some(parent) = ancestor.parent() else {
            return fs::canonicalize(path);
        };
        ancestor = parent;
        if ancestor.exists() {
            let mut canonical = fs::canonicalize(ancestor)?;
            for component in suffix.iter().rev() {
                canonical.push(component);
            }
            return Ok(canonical);
        }
    }
}

fn matching_roots<'a>(
    candidate: &Path,
    roots: &'a [ResolvedFileRoot],
) -> Vec<&'a ResolvedFileRoot> {
    roots
        .iter()
        .filter(|root| contains_path(candidate, &root.canonical))
        .collect()
}

fn contains_path(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn symlink_escape_root(
    candidate: &Path,
    candidate_canonical: &Path,
    roots: &[ResolvedFileRoot],
) -> Option<String> {
    let Ok(metadata) = fs::symlink_metadata(candidate) else {
        return None;
    };
    if !metadata.file_type().is_symlink() {
        return None;
    }
    let parent = candidate.parent()?;
    let Ok(parent_canonical) = canonicalize_existing_or_parent(parent) else {
        return None;
    };
    roots
        .iter()
        .find(|root| {
            contains_path(&parent_canonical, &root.canonical)
                && !contains_path(candidate_canonical, &root.canonical)
        })
        .map(|root| root.name.clone())
}

fn denied_file_type(metadata: &Metadata) -> Option<&'static str> {
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        return Some("a directory");
    }
    #[cfg(unix)]
    {
        if file_type.is_fifo() {
            return Some("a FIFO");
        }
        if file_type.is_socket() {
            return Some("a socket");
        }
        if file_type.is_block_device() {
            return Some("a block device");
        }
        if file_type.is_char_device() {
            return Some("a character device");
        }
    }
    (!file_type.is_file()).then_some("a special file")
}

fn denied(
    rendered: String,
    diagnostic: impl Into<String>,
) -> ArtifactRefResolutionWire {
    let mut resolved = resolution("denied", rendered);
    resolved.diagnostic = Some(diagnostic.into());
    resolved
}

fn filtered_without_path(
    rendered: String,
    diagnostic: &str,
) -> ArtifactRefResolutionWire {
    let mut resolved = resolution("filtered", rendered);
    resolved.diagnostic =
        Some(format!("artifact reference filtered: {diagnostic}"));
    resolved
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::ffi::CString;
    #[cfg(unix)]
    use std::os::unix::fs::{symlink, PermissionsExt};

    use tempfile::tempdir;

    use super::*;
    use crate::artifact_ref::{
        ArtifactRefFileRootWire, ArtifactRefPayloadWire,
    };

    fn context_for(
        root_name: &str,
        root: &Path,
        path_globs: Option<Vec<String>>,
    ) -> ArtifactRefContextWire {
        ArtifactRefContextWire {
            file_roots: vec![ArtifactRefFileRootWire {
                name: root_name.to_string(),
                path: root.to_string_lossy().into_owned(),
                path_globs,
            }],
            home_dir: root
                .parent()
                .map(|path| path.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    fn resolve(
        path: &Path,
        context: &ArtifactRefContextWire,
    ) -> ArtifactRefResolutionWire {
        resolve_artifact_file_path(
            &path.to_string_lossy(),
            context,
            format!("file:{}", path.to_string_lossy()),
        )
        .unwrap()
    }

    #[test]
    fn absolute_and_home_paths_resolve_to_one_logical_path() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let bob = home.join("bob");
        fs::create_dir_all(&bob).unwrap();
        fs::write(bob.join("gtd.md"), "tasks").unwrap();
        let context = ArtifactRefContextWire {
            file_roots: vec![ArtifactRefFileRootWire {
                name: "bob".to_string(),
                path: bob.to_string_lossy().into_owned(),
                path_globs: None,
            }],
            home_dir: Some(home.to_string_lossy().into_owned()),
            ..Default::default()
        };

        let absolute = resolve(&bob.join("gtd.md"), &context);
        let home_relative = resolve_artifact_file_path(
            "~/bob/gtd.md",
            &context,
            "file:~/bob/gtd.md".to_string(),
        )
        .unwrap();

        assert_eq!(absolute.status, "exact");
        assert_eq!(home_relative.status, "exact");
        assert_eq!(absolute.locator.as_deref(), Some("bob:gtd.md"));
        assert_eq!(home_relative.locator.as_deref(), Some("bob:gtd.md"));
        assert_eq!(absolute.resolved_path, home_relative.resolved_path);
    }

    #[test]
    fn relative_paths_and_zero_roots_are_rejected_without_guessing() {
        let denied = resolve_artifact_file_path(
            "notes.md",
            &ArtifactRefContextWire::default(),
            "file:notes.md".to_string(),
        )
        .unwrap();
        assert_eq!(denied.status, "denied");
        assert!(denied
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("absolute or ~/"));

        let temp = tempdir().unwrap();
        let path = temp.path().join("notes.md");
        fs::write(&path, "notes").unwrap();
        let filtered = resolve(&path, &ArtifactRefContextWire::default());
        assert_eq!(filtered.status, "filtered");
        assert!(filtered
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("artifact_refs.file.roots"));
    }

    #[test]
    fn overlapping_roots_are_ambiguous_and_glob_miss_is_filtered() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("root");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let file = nested.join("secret.txt");
        fs::write(&file, "secret").unwrap();
        let context = ArtifactRefContextWire {
            file_roots: vec![
                ArtifactRefFileRootWire {
                    name: "root".to_string(),
                    path: root.to_string_lossy().into_owned(),
                    path_globs: None,
                },
                ArtifactRefFileRootWire {
                    name: "nested".to_string(),
                    path: nested.to_string_lossy().into_owned(),
                    path_globs: None,
                },
            ],
            ..Default::default()
        };
        let ambiguous = resolve(&file, &context);
        assert_eq!(ambiguous.status, "ambiguous");
        assert_eq!(ambiguous.candidates, ["root", "nested"]);

        let context =
            context_for("root", &root, Some(vec!["**/*.md".to_string()]));
        let filtered = resolve(&file, &context);
        assert_eq!(filtered.status, "filtered");
        let diagnostic = filtered.diagnostic.unwrap();
        assert!(diagnostic.contains("nested/secret.txt"));
        assert!(!diagnostic.contains(temp.path().to_str().unwrap()));
    }

    #[test]
    fn missing_inside_root_is_missing_and_traversal_escape_is_filtered() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.md"), "secret").unwrap();
        let context = context_for("root", &root, None);

        let missing = resolve(&root.join("missing.md"), &context);
        assert_eq!(missing.status, "missing");
        assert_eq!(missing.candidates.len(), 1);

        let escaped = resolve(&root.join("../outside/secret.md"), &context);
        assert_eq!(escaped.status, "filtered");
        assert!(!escaped
            .diagnostic
            .as_deref()
            .unwrap()
            .contains(outside.to_str().unwrap()));
    }

    #[test]
    fn directories_and_size_overages_are_denied() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("dir")).unwrap();
        let mut context = context_for("root", &root, None);

        let directory = resolve(&root.join("dir"), &context);
        assert_eq!(directory.status, "denied");
        assert!(directory
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("directory"));

        let file = root.join("large.bin");
        fs::write(&file, "abcdef").unwrap();
        context.file_capture_max_bytes = Some(3);
        let too_large = resolve(&file, &context);
        assert_eq!(too_large.status, "denied");
        assert!(too_large
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("6 bytes > 3 bytes"));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_fifo_and_unreadable_files_are_denied() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.md"), "secret").unwrap();
        symlink(outside.join("secret.md"), root.join("escape.md")).unwrap();
        let context = context_for("root", &root, None);

        let escaped = resolve(&root.join("escape.md"), &context);
        assert_eq!(escaped.status, "denied");
        assert!(escaped.diagnostic.as_deref().unwrap().contains("symlink"));

        let fifo = root.join("pipe");
        let fifo_c = CString::new(fifo.to_string_lossy().as_bytes()).unwrap();
        let created = unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) };
        assert_eq!(created, 0, "mkfifo failed");
        let pipe = resolve(&fifo, &context);
        assert_eq!(pipe.status, "denied");
        assert!(pipe.diagnostic.as_deref().unwrap().contains("FIFO"));

        let unreadable = root.join("no-read.md");
        fs::write(&unreadable, "secret").unwrap();
        let original = fs::metadata(&unreadable).unwrap().permissions();
        let mut locked = original.clone();
        locked.set_mode(0o000);
        fs::set_permissions(&unreadable, locked).unwrap();
        let result = resolve(&unreadable, &context);
        fs::set_permissions(&unreadable, original).unwrap();
        assert_eq!(result.status, "denied");
    }

    #[test]
    fn file_path_payload_still_round_trips_as_a_file_path_payload() {
        let payload = crate::artifact_ref::parse_artifact_ref("file:/tmp/x.md")
            .unwrap()
            .payload;
        assert!(matches!(payload, ArtifactRefPayloadWire::FilePath { .. }));
    }
}
