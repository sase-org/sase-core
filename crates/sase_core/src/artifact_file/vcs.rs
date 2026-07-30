//! Content-verified materialization of byte-free artifact-file records.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const GIT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Request for content-addressed materialization of one VCS-backed artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileVcsMaterializationRequestWire {
    pub cache_root: String,
    #[serde(default)]
    pub checkout_paths: Vec<String>,
    pub vcs_sha: String,
    pub vcs_relpath: String,
    pub sha256: String,
    #[serde(default)]
    pub suffix: String,
    #[serde(default)]
    pub max_history_scan: usize,
}

/// Result of attempting to materialize one VCS-backed artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileVcsMaterializationWire {
    pub status: String,
    pub path: Option<String>,
    pub checkout: Option<String>,
    pub sha: Option<String>,
    pub checkouts_tried: Vec<String>,
}

/// Materialize exact artifact bytes from one of the supplied git checkouts.
///
/// Git failures, timeouts, malformed requests, and cache I/O failures all
/// degrade to a `missing` result. Content is never returned or cached until
/// its SHA-256 digest matches the request.
pub fn materialize_vcs_artifact_file(
    request: &ArtifactFileVcsMaterializationRequestWire,
) -> ArtifactFileVcsMaterializationWire {
    let Some(cache_path) = cache_path(request) else {
        return missing(Vec::new());
    };
    if cache_entry_matches(&cache_path, &request.sha256) {
        return ArtifactFileVcsMaterializationWire {
            status: "cached".to_string(),
            path: Some(path_string(&cache_path)),
            checkout: None,
            sha: None,
            checkouts_tried: Vec::new(),
        };
    }
    discard_invalid_cache_entry(&cache_path);

    let mut checkouts_tried = Vec::new();
    for checkout in &request.checkout_paths {
        checkouts_tried.push(checkout.clone());
        if let Some(bytes) = read_blob(
            Path::new(checkout),
            &request.vcs_sha,
            &request.vcs_relpath,
        ) {
            if digest_matches(&bytes, &request.sha256)
                && write_cache_entry(&cache_path, &bytes)
            {
                return materialized(
                    &cache_path,
                    checkout,
                    &request.vcs_sha,
                    checkouts_tried,
                );
            }
        }
    }

    if request.max_history_scan > 0 {
        for checkout in &request.checkout_paths {
            let checkout_path = Path::new(checkout);
            for sha in history_for_path(
                checkout_path,
                &request.vcs_relpath,
                request.max_history_scan,
            ) {
                let Some(bytes) =
                    read_blob(checkout_path, &sha, &request.vcs_relpath)
                else {
                    continue;
                };
                if digest_matches(&bytes, &request.sha256)
                    && write_cache_entry(&cache_path, &bytes)
                {
                    return materialized(
                        &cache_path,
                        checkout,
                        &sha,
                        checkouts_tried,
                    );
                }
            }
        }
    }

    missing(checkouts_tried)
}

fn materialized(
    cache_path: &Path,
    checkout: &str,
    sha: &str,
    checkouts_tried: Vec<String>,
) -> ArtifactFileVcsMaterializationWire {
    ArtifactFileVcsMaterializationWire {
        status: "materialized".to_string(),
        path: Some(path_string(cache_path)),
        checkout: Some(checkout.to_string()),
        sha: Some(sha.to_string()),
        checkouts_tried,
    }
}

fn missing(checkouts_tried: Vec<String>) -> ArtifactFileVcsMaterializationWire {
    ArtifactFileVcsMaterializationWire {
        status: "missing".to_string(),
        path: None,
        checkout: None,
        sha: None,
        checkouts_tried,
    }
}

fn cache_path(
    request: &ArtifactFileVcsMaterializationRequestWire,
) -> Option<PathBuf> {
    if !valid_sha256(&request.sha256) || !valid_suffix(&request.suffix) {
        return None;
    }
    let requested_root = Path::new(&request.cache_root);
    fs::create_dir_all(requested_root).ok()?;
    let cache_root = requested_root.canonicalize().ok()?;
    let prefix_dir = cache_root.join(&request.sha256[..2]);
    if prefix_dir.exists()
        && fs::symlink_metadata(&prefix_dir)
            .ok()?
            .file_type()
            .is_symlink()
    {
        return None;
    }
    fs::create_dir_all(&prefix_dir).ok()?;
    let prefix_dir = prefix_dir.canonicalize().ok()?;
    if !prefix_dir.starts_with(&cache_root) {
        return None;
    }
    Some(prefix_dir.join(format!("{}{}", request.sha256, request.suffix)))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_suffix(value: &str) -> bool {
    value.is_empty()
        || (value.len() <= 64
            && value.starts_with('.')
            && value.len() > 1
            && !value.contains("..")
            && value[1..].bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'-' | b'_')
            }))
}

fn cache_entry_matches(path: &Path, expected_sha256: &str) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.file_type().is_file() {
        return false;
    }
    fs::read(path)
        .ok()
        .is_some_and(|bytes| digest_matches(&bytes, expected_sha256))
}

fn discard_invalid_cache_entry(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        let _ = fs::remove_file(path);
    }
}

fn write_cache_entry(path: &Path, bytes: &[u8]) -> bool {
    let Some(parent) = path.parent() else {
        return false;
    };
    let Ok(mut temporary) = NamedTempFile::new_in(parent) else {
        return false;
    };
    if temporary.write_all(bytes).is_err()
        || temporary.as_file().sync_all().is_err()
    {
        return false;
    }
    match temporary.persist(path) {
        Ok(_) => {
            let _ =
                File::open(parent).and_then(|directory| directory.sync_all());
            true
        }
        Err(_) => cache_entry_matches(path, &digest_bytes(bytes)),
    }
}

fn digest_matches(bytes: &[u8], expected_sha256: &str) -> bool {
    digest_bytes(bytes) == expected_sha256
}

fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn read_blob(checkout: &Path, sha: &str, relpath: &str) -> Option<Vec<u8>> {
    let spec = format!("{sha}:{relpath}");
    run_git(checkout, &["cat-file", "blob", &spec])
}

fn history_for_path(
    checkout: &Path,
    relpath: &str,
    max_history_scan: usize,
) -> Vec<String> {
    let max_count = format!("--max-count={max_history_scan}");
    let Some(output) = run_git(
        checkout,
        &["rev-list", &max_count, "--remotes=origin", "--", relpath],
    ) else {
        return Vec::new();
    };
    let Ok(output) = String::from_utf8(output) else {
        return Vec::new();
    };
    output
        .lines()
        .map(str::trim)
        .filter(|sha| !sha.is_empty())
        .map(str::to_string)
        .collect()
}

fn run_git(checkout: &Path, args: &[&str]) -> Option<Vec<u8>> {
    let mut stdout = tempfile::tempfile().ok()?;
    let stdout_writer = stdout.try_clone().ok()?;
    let mut child = Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < GIT_COMMAND_TIMEOUT => {
                thread::sleep(GIT_WAIT_POLL_INTERVAL);
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !status.success() {
        return None;
    }
    stdout.seek(SeekFrom::Start(0)).ok()?;
    let mut output = Vec::new();
    stdout.read_to_end(&mut output).ok()?;
    Some(output)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct GitFixture {
        _temp: tempfile::TempDir,
        repo: PathBuf,
        sha: String,
        relpath: String,
        bytes: Vec<u8>,
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn init_repo() -> GitFixture {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--initial-branch=master"]);
        git(&repo, &["config", "user.name", "SASE Test"]);
        git(&repo, &["config", "user.email", "sase@example.com"]);
        let relpath = "media/example.txt".to_string();
        let bytes = b"versioned artifact\n".to_vec();
        fs::create_dir_all(repo.join("media")).unwrap();
        fs::write(repo.join(&relpath), &bytes).unwrap();
        git(&repo, &["add", &relpath]);
        git(&repo, &["commit", "-m", "artifact"]);
        let sha = git(&repo, &["rev-parse", "HEAD"]);
        git(&repo, &["update-ref", "refs/remotes/origin/master", &sha]);
        GitFixture {
            _temp: temp,
            repo,
            sha,
            relpath,
            bytes,
        }
    }

    fn request(
        cache_root: &Path,
        fixture: &GitFixture,
    ) -> ArtifactFileVcsMaterializationRequestWire {
        ArtifactFileVcsMaterializationRequestWire {
            cache_root: path_string(cache_root),
            checkout_paths: vec![path_string(&fixture.repo)],
            vcs_sha: fixture.sha.clone(),
            vcs_relpath: fixture.relpath.clone(),
            sha256: digest_bytes(&fixture.bytes),
            suffix: ".txt".to_string(),
            max_history_scan: 20,
        }
    }

    #[test]
    fn materializes_direct_blob_then_uses_verified_cache() {
        let fixture = init_repo();
        let cache = fixture._temp.path().join("cache");
        let request = request(&cache, &fixture);

        let first = materialize_vcs_artifact_file(&request);
        assert_eq!(first.status, "materialized");
        assert_eq!(
            first.checkout.as_deref(),
            Some(path_string(&fixture.repo).as_str())
        );
        assert_eq!(first.sha.as_deref(), Some(fixture.sha.as_str()));
        assert_eq!(
            fs::read(first.path.as_ref().unwrap()).unwrap(),
            fixture.bytes
        );

        let mut cached_request = request;
        cached_request.checkout_paths = vec!["/missing/checkout".to_string()];
        let cached = materialize_vcs_artifact_file(&cached_request);
        assert_eq!(cached.status, "cached");
        assert!(cached.checkout.is_none());
        assert!(cached.checkouts_tried.is_empty());
    }

    #[test]
    fn replaces_wrong_cache_content_after_verifying_git_blob() {
        let fixture = init_repo();
        let cache = fixture._temp.path().join("cache");
        let request = request(&cache, &fixture);
        let path = cache
            .join(&request.sha256[..2])
            .join(format!("{}.txt", request.sha256));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"wrong").unwrap();

        let result = materialize_vcs_artifact_file(&request);
        assert_eq!(result.status, "materialized");
        assert_eq!(fs::read(path).unwrap(), fixture.bytes);
    }

    #[test]
    fn searches_bounded_remote_history_when_recorded_sha_is_gone() {
        let fixture = init_repo();
        let cache = fixture._temp.path().join("cache");
        let mut request = request(&cache, &fixture);
        request.vcs_sha =
            "0000000000000000000000000000000000000000".to_string();

        let result = materialize_vcs_artifact_file(&request);
        assert_eq!(result.status, "materialized");
        assert_eq!(result.sha.as_deref(), Some(fixture.sha.as_str()));
        assert_eq!(fs::read(result.path.unwrap()).unwrap(), fixture.bytes);
    }

    #[test]
    fn reports_missing_for_unknown_content_or_a_zero_history_bound() {
        let fixture = init_repo();
        let cache = fixture._temp.path().join("cache");
        let mut request = request(&cache, &fixture);
        request.vcs_sha =
            "0000000000000000000000000000000000000000".to_string();
        request.sha256 = digest_bytes(b"content absent from history");
        assert_eq!(materialize_vcs_artifact_file(&request).status, "missing");

        request.sha256 = digest_bytes(&fixture.bytes);
        request.max_history_scan = 0;
        assert_eq!(materialize_vcs_artifact_file(&request).status, "missing");
    }

    #[test]
    fn tries_later_checkouts_when_the_first_lacks_the_object() {
        let fixture = init_repo();
        let empty = tempfile::tempdir().unwrap();
        git(empty.path(), &["init", "--initial-branch=master"]);
        let cache = fixture._temp.path().join("cache");
        let mut request = request(&cache, &fixture);
        request.checkout_paths.insert(0, path_string(empty.path()));

        let result = materialize_vcs_artifact_file(&request);
        assert_eq!(result.status, "materialized");
        assert_eq!(
            result.checkout.as_deref(),
            Some(path_string(&fixture.repo).as_str())
        );
        assert_eq!(result.checkouts_tried, request.checkout_paths);
    }

    #[test]
    fn rejects_cache_path_escape_inputs() {
        let fixture = init_repo();
        let cache = fixture._temp.path().join("cache");
        let mut request = request(&cache, &fixture);
        request.suffix = "/../../outside".to_string();
        assert_eq!(materialize_vcs_artifact_file(&request).status, "missing");
        assert!(!fixture._temp.path().join("outside").exists());
    }
}
