//! Scanner tests with synthetic secrets assembled at runtime.
//!
//! No committed file contains a literal the scanner, gitleaks, or push
//! protection would flag. Tests inject env maps and use local temp files.

use std::collections::HashMap;
use std::io::Write;

use crate::note_attachment::{
    attachment_scan_file, attachment_scanner_rules_version,
    AttachmentScanHitKindWire, AttachmentScanOutcomeWire,
    ATTACHMENT_SCANNER_RULES_VERSION,
};

fn temp_file(content: &[u8]) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("candidate.log");
    std::fs::write(&path, content).unwrap();
    let path_str = path.to_str().unwrap().to_string();
    // Leak the dir anchor by returning it; caller holds it.
    (dir, path_str)
}

fn write_bytes(path: &str, content: &[u8]) {
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(content).unwrap();
}

#[test]
fn rules_version_is_one() {
    assert_eq!(ATTACHMENT_SCANNER_RULES_VERSION, 1);
    assert_eq!(attachment_scanner_rules_version(), 1);
}

#[test]
fn clean_text_is_clean() {
    let (_dir, path) = temp_file(b"hello world\nbuild ok\n");
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Clean);
    assert!(scan.hit.is_none());
    assert_eq!(scan.rules_version, 1);
    assert!(scan.bytes_scanned > 0);
}

#[test]
fn known_env_value_hits() {
    // Assemble token-like values at runtime; never commit a literal.
    let secret = format!("{}{}", "test-value-", "abcdefghij1234");
    let (_dir, path) = temp_file(b"prefix\n");
    let content = format!("log line\nleaked {}\nend\n", secret);
    write_bytes(&path, content.as_bytes());
    let mut env = HashMap::new();
    env.insert("MY_SERVICE_TOKEN".to_string(), secret.clone());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &env,
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
    let hit = scan.hit.as_ref().unwrap();
    assert_eq!(hit.kind, AttachmentScanHitKindWire::KnownValue);
    assert_eq!(hit.line, 2);
    // No matched text leaks into the output.
    let serialized = serde_json::to_string(&scan).unwrap();
    assert!(!serialized.contains(&secret));
}

#[test]
fn known_value_name_filtering() {
    let secret = format!("{}{}", "short-ok-value-", "xyz123456789");
    let (_dir, path) = temp_file(format!("has {}\n", secret).as_bytes());
    // Non-secret names never match.
    let mut env = HashMap::new();
    env.insert("USERNAME".to_string(), secret.clone());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &env,
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Clean);
    // Numeric, boolean, and path values never match.
    for (name, value) in [
        ("MY_TOKEN", "123456789012345"),
        ("MY_SECRET", "true"),
        ("MY_PASSWORD", "/home/bryan/.sase/token"),
    ] {
        let mut env = HashMap::new();
        env.insert(name.to_string(), value.to_string());
        let scan = attachment_scan_file(
            &path,
            1024 * 1024,
            &env,
            "/home/bryan",
            "/home/bryan/.sase",
        );
        assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Clean, "{name}");
    }
}

#[test]
fn credential_patterns_hit() {
    // GitHub-style token assembled at runtime.
    let prefix = ["gh", "p_"].concat();
    let token = format!("{}{}", prefix, "A".repeat(30));
    let (_dir, path) = temp_file(format!("leak {}\n", token).as_bytes());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
    assert_eq!(
        scan.hit.unwrap().kind,
        AttachmentScanHitKindWire::CredentialPattern
    );

    // PEM header.
    let (_dir2, path2) = temp_file(b"-----BEGIN PRIVATE KEY-----\n");
    let scan = attachment_scan_file(
        &path2,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);

    // High-entropy assignment.
    let value = "aB3dE5gH7jK9mN2pQ4sT6vX8".to_string();
    let line = format!("api_token = {}\n", value);
    let (_dir3, path3) = temp_file(line.as_bytes());
    let scan = attachment_scan_file(
        &path3,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
}

#[test]
fn env_dump_windows_hit() {
    let mut lines = Vec::new();
    for index in 0..10 {
        lines.push(format!("VAR{index}=value{index}"));
    }
    let content = lines.join("\n") + "\n";
    let (_dir, path) = temp_file(content.as_bytes());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
    assert_eq!(scan.hit.unwrap().kind, AttachmentScanHitKindWire::EnvDump);

    // Structured object with well-known keys.
    let object = r#"{"PATH": "/usr/bin", "HOME": "/home/bryan", "USER": "bryan", "SHELL": "/bin/bash", "PWD": "/work", "LANG": "en_US.UTF-8"}"#;
    let (_dir2, path2) = temp_file(object.as_bytes());
    let scan = attachment_scan_file(
        &path2,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
}

#[test]
fn chunk_straddling_value_is_found() {
    // A known value placed so it straddles small read chunks still hits.
    // The scanner reads the full buffer, so any split is covered.
    let secret = format!("{}{}", "straddle-", "0123456789abcdef");
    let mut content = vec![b'x'; 70000];
    content.extend_from_slice(secret.as_bytes());
    content.extend_from_slice(b"\nend\n");
    let (_dir, path) = temp_file(&content);
    let mut env = HashMap::new();
    env.insert("STRADDLE_TOKEN".to_string(), secret.clone());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &env,
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
    assert_eq!(
        scan.hit.unwrap().kind,
        AttachmentScanHitKindWire::KnownValue
    );
}

#[test]
fn long_lines_are_bounded() {
    // A 200KB single line with a late credential still resolves without
    // unbounded per-line allocation failures.
    let mut line = "x".repeat(200 * 1024);
    let prefix = ["gh", "p_"].concat();
    line.push_str(&format!(" {}{}", prefix, "B".repeat(30)));
    let (_dir, path) = temp_file(line.as_bytes());
    let scan = attachment_scan_file(
        &path,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
}

#[test]
fn over_cap_and_binary_skip() {
    let (_dir, path) = temp_file(b"small\n");
    let scan = attachment_scan_file(
        &path,
        2,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Skipped);

    let (_dir2, path2) = temp_file(b"ok\x00binary\n");
    let scan = attachment_scan_file(
        &path2,
        1024 * 1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Skipped);

    let scan = attachment_scan_file(
        "/nonexistent/path/candidate.log",
        1024,
        &HashMap::new(),
        "/home/bryan",
        "/home/bryan/.sase",
    );
    assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Skipped);
}
