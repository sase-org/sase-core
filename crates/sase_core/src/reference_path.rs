//! Crate-private helpers for safe logical paths and ordered-root resolution.

use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelativePayloadError {
    Empty,
    Absolute,
    Backslash,
    EmptySegment,
    ParentSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathPayloadError {
    InvalidUtf8,
    EscapesRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriftPolicy {
    SixDigitShard,
    AnyChildBasename,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderedRootStatus {
    Exact,
    Drifted,
    Ambiguous,
    Missing,
}

impl OrderedRootStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Drifted => "drifted",
            Self::Ambiguous => "ambiguous",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderedRootResolution {
    pub(crate) status: OrderedRootStatus,
    pub(crate) resolved_path: Option<PathBuf>,
    pub(crate) candidates: Vec<PathBuf>,
}

pub(crate) fn validate_relative_payload(
    path: &str,
) -> Result<(), RelativePayloadError> {
    if path.is_empty() {
        return Err(RelativePayloadError::Empty);
    }
    if path.starts_with('/') {
        return Err(RelativePayloadError::Absolute);
    }
    if path.contains('\\') {
        return Err(RelativePayloadError::Backslash);
    }
    if path.split('/').any(str::is_empty) {
        return Err(RelativePayloadError::EmptySegment);
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(RelativePayloadError::ParentSegment);
    }
    Ok(())
}

pub(crate) fn path_to_relative_payload(
    path: &Path,
) -> Result<String, PathPayloadError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part =
                    part.to_str().ok_or(PathPayloadError::InvalidUtf8)?;
                parts.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(PathPayloadError::EscapesRoot);
            }
        }
    }
    Ok(parts.join("/"))
}

pub(crate) fn resolve_ordered_root_file(
    payload: &Path,
    roots: &[PathBuf],
    initial_candidates: Vec<PathBuf>,
    drift_policy: DriftPolicy,
) -> OrderedRootResolution {
    let mut candidates = initial_candidates;
    for candidate in &candidates {
        if candidate.is_file() {
            return OrderedRootResolution {
                status: OrderedRootStatus::Exact,
                resolved_path: Some(candidate.clone()),
                candidates,
            };
        }
    }

    for root in roots {
        let candidate = root.join(payload);
        candidates.push(candidate.clone());
        if candidate.is_file() {
            return OrderedRootResolution {
                status: OrderedRootStatus::Exact,
                resolved_path: Some(candidate),
                candidates,
            };
        }
    }

    if let Some(filename) = drift_filename(payload, drift_policy) {
        let mut drift_matches = Vec::new();
        for root in roots {
            let Ok(children) = std::fs::read_dir(root) else {
                continue;
            };
            let mut child_dirs = children
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect::<Vec<_>>();
            child_dirs.sort();
            for child_dir in child_dirs {
                let candidate = child_dir.join(filename);
                if candidate.is_file() {
                    drift_matches.push(candidate);
                }
            }
        }
        if drift_matches.len() == 1 {
            return OrderedRootResolution {
                status: OrderedRootStatus::Drifted,
                resolved_path: drift_matches.first().cloned(),
                candidates: drift_matches,
            };
        }
        if drift_matches.len() > 1 {
            return OrderedRootResolution {
                status: OrderedRootStatus::Ambiguous,
                resolved_path: None,
                candidates: drift_matches,
            };
        }
    }

    OrderedRootResolution {
        status: OrderedRootStatus::Missing,
        resolved_path: None,
        candidates,
    }
}

fn drift_filename(path: &Path, policy: DriftPolicy) -> Option<&Path> {
    let components = path.components().collect::<Vec<_>>();
    if policy == DriftPolicy::SixDigitShard {
        if components.len() != 2 {
            return None;
        }
        let shard = match components[0] {
            Component::Normal(shard) => shard.to_str()?,
            _ => return None,
        };
        if shard.len() != 6 || !shard.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
    }
    match *components.last()? {
        Component::Normal(filename) => Some(Path::new(filename)),
        _ => None,
    }
}
