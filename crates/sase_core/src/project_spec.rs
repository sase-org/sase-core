//! Project spec path helpers.
//!
//! Canonical project spec files use the `.sase` extension. Legacy `.gp`
//! files are still recognized during migration.

use std::path::{Path, PathBuf};

pub const PROJECT_SPEC_EXTENSION: &str = ".sase";
pub const LEGACY_PROJECT_SPEC_EXTENSION: &str = ".gp";
pub const PROJECT_SPEC_ARCHIVE_SUFFIX: &str = "-archive";

pub fn active_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{PROJECT_SPEC_EXTENSION}")
}

pub fn archive_project_spec_filename(project_name: &str) -> String {
    format!(
        "{project_name}{PROJECT_SPEC_ARCHIVE_SUFFIX}{PROJECT_SPEC_EXTENSION}"
    )
}

pub fn legacy_active_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{LEGACY_PROJECT_SPEC_EXTENSION}")
}

pub fn legacy_archive_project_spec_filename(project_name: &str) -> String {
    format!("{project_name}{PROJECT_SPEC_ARCHIVE_SUFFIX}{LEGACY_PROJECT_SPEC_EXTENSION}")
}

pub fn project_spec_basename(file_path: &str) -> String {
    let base = file_path.rsplit(['/', '\\']).next().unwrap_or(file_path);
    let stem = match base.rfind('.') {
        Some(i) => &base[..i],
        None => base,
    };
    if let Some(prefix) = stem.strip_suffix(PROJECT_SPEC_ARCHIVE_SUFFIX) {
        prefix.to_string()
    } else {
        stem.to_string()
    }
}

pub fn is_archive_project_spec(file_path: &str) -> bool {
    let path = Path::new(file_path);
    let Some(extension) = path.extension().and_then(|value| value.to_str())
    else {
        return false;
    };
    if extension != &PROJECT_SPEC_EXTENSION[1..]
        && extension != &LEGACY_PROJECT_SPEC_EXTENSION[1..]
    {
        return false;
    }
    path.file_stem()
        .and_then(|value| value.to_str())
        .is_some_and(|stem| stem.ends_with(PROJECT_SPEC_ARCHIVE_SUFFIX))
}

pub fn to_archive_project_spec_path(project_file: &Path) -> PathBuf {
    let basename = project_spec_basename(&project_file.to_string_lossy());
    project_file.with_file_name(archive_project_spec_filename(&basename))
}

pub fn to_active_project_spec_path(project_file: &Path) -> PathBuf {
    let basename = project_spec_basename(&project_file.to_string_lossy());
    project_file.with_file_name(active_project_spec_filename(&basename))
}

pub fn preferred_project_spec_path(
    project_dir: &Path,
    project_name: &str,
    archive: bool,
) -> PathBuf {
    let canonical_name = if archive {
        archive_project_spec_filename(project_name)
    } else {
        active_project_spec_filename(project_name)
    };
    let legacy_name = if archive {
        legacy_archive_project_spec_filename(project_name)
    } else {
        legacy_active_project_spec_filename(project_name)
    };
    let canonical_path = project_dir.join(canonical_name);
    if canonical_path.exists() {
        return canonical_path;
    }
    let legacy_path = project_dir.join(legacy_name);
    if legacy_path.exists() {
        return legacy_path;
    }
    canonical_path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_spec_filenames_are_canonical_sase() {
        assert_eq!(PROJECT_SPEC_EXTENSION, ".sase");
        assert_eq!(LEGACY_PROJECT_SPEC_EXTENSION, ".gp");
        assert_eq!(active_project_spec_filename("proj"), "proj.sase");
        assert_eq!(archive_project_spec_filename("proj"), "proj-archive.sase");
    }

    #[test]
    fn project_spec_basename_accepts_active_and_archive_extensions() {
        assert_eq!(project_spec_basename("/tmp/myproj.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.sase"), "myproj");
        assert_eq!(project_spec_basename("/tmp/myproj-archive.gp"), "myproj");
        assert_eq!(project_spec_basename("/tmp/no_ext"), "no_ext");
        assert_eq!(project_spec_basename("foo.bar.sase"), "foo.bar");
    }

    #[test]
    fn project_spec_path_conversions_emit_canonical_extension() {
        assert_eq!(
            to_archive_project_spec_path(Path::new("/tmp/proj.sase")),
            PathBuf::from("/tmp/proj-archive.sase")
        );
        assert_eq!(
            to_archive_project_spec_path(Path::new("/tmp/proj.gp")),
            PathBuf::from("/tmp/proj-archive.sase")
        );
        assert_eq!(
            to_active_project_spec_path(Path::new("/tmp/proj-archive.sase")),
            PathBuf::from("/tmp/proj.sase")
        );
        assert_eq!(
            to_active_project_spec_path(Path::new("/tmp/proj-archive.gp")),
            PathBuf::from("/tmp/proj.sase")
        );
    }

    #[test]
    fn archive_detection_accepts_canonical_and_legacy_extensions() {
        assert!(is_archive_project_spec("/tmp/proj-archive.sase"));
        assert!(is_archive_project_spec("/tmp/proj-archive.gp"));
        assert!(!is_archive_project_spec("/tmp/proj.sase"));
        assert!(!is_archive_project_spec("/tmp/proj-archive.txt"));
    }
}
