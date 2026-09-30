//! Tests for attachment manifests, tombstones, and policy.

use crate::note_attachment::{
    attachment_placement, attachment_sensitive_path_reason,
    attachment_should_auto_fetch, validate_note_attachment_manifest,
    AttachmentImageDimsWire, AttachmentPlacementWire, AttachmentStoreTierWire,
    AttachmentTombstoneWire, BeadNoteAttachmentWire,
    ATTACHMENT_TOMBSTONE_WIRE_SCHEMA_VERSION,
};

const DIGEST_A: &str =
    "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";
const DIGEST_B: &str =
    "41aa07c3e9b1d2f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e";

fn descriptor(name: &str, sha256: &str) -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: name.to_string(),
        sha256: sha256.to_string(),
        size_bytes: 188416,
        mime_type: "image/png".to_string(),
        image: Some(AttachmentImageDimsWire {
            width: 1280,
            height: 720,
        }),
        origin: Some("athena".to_string()),
        visibility: None,
    }
}

fn text_descriptor(name: &str, sha256: &str) -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: name.to_string(),
        sha256: sha256.to_string(),
        size_bytes: 2202009,
        mime_type: "text/plain".to_string(),
        image: None,
        origin: None,
        visibility: None,
    }
}

#[test]
fn manifest_validates_matching_tokens() {
    let manifest = vec![
        descriptor("login.png", DIGEST_A),
        text_descriptor("crash.log", DIGEST_B),
    ];
    validate_note_attachment_manifest(
        &manifest,
        "Crash @attachment:login.png — log: @attachment:crash.log",
    )
    .unwrap();
}

#[test]
fn manifest_validates_repeated_token_with_one_descriptor() {
    let manifest = vec![descriptor("shot.png", DIGEST_A)];
    validate_note_attachment_manifest(
        &manifest,
        "shot @attachment:shot.png again @attachment:shot.png",
    )
    .unwrap();
    validate_note_attachment_manifest(
        &manifest,
        "@attachment:shot.png @attachment:shot.png",
    )
    .unwrap();
}

#[test]
fn manifest_rejects_missing_token() {
    let manifest = vec![descriptor("login.png", DIGEST_A)];
    let error = validate_note_attachment_manifest(&manifest, "no tokens here")
        .unwrap_err();
    assert!(
        error.to_string().contains("must match one-to-one"),
        "{error}"
    );
}

#[test]
fn manifest_rejects_orphan_descriptor() {
    let manifest = vec![
        descriptor("login.png", DIGEST_A),
        text_descriptor("crash.log", DIGEST_B),
    ];
    let error = validate_note_attachment_manifest(
        &manifest,
        "only @attachment:login.png is referenced",
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("must match one-to-one"),
        "{error}"
    );
}

#[test]
fn manifest_rejects_bad_sha() {
    let mut bad = descriptor("login.png", DIGEST_A);
    bad.sha256 = "not-a-digest".to_string();
    let error =
        validate_note_attachment_manifest(&[bad], "@attachment:login.png")
            .unwrap_err();
    assert!(error.to_string().contains("SHA-256"), "{error}");
    let mut upper = descriptor("login.png", DIGEST_A);
    upper.sha256 = DIGEST_A.to_uppercase();
    validate_note_attachment_manifest(&[upper], "@attachment:login.png")
        .unwrap_err();
}

#[test]
fn manifest_rejects_duplicate_names() {
    let manifest = vec![
        descriptor("login.png", DIGEST_A),
        text_descriptor("login.png", DIGEST_B),
    ];
    let error = validate_note_attachment_manifest(
        &manifest,
        "@attachment:login.png @attachment:login.png",
    )
    .unwrap_err();
    assert!(error.to_string().contains("duplicate name"), "{error}");
}

#[test]
fn manifest_rejects_unsanitized_name_and_bad_mime() {
    let mut bad = descriptor("login.png", DIGEST_A);
    bad.name = "../evil.png".to_string();
    validate_note_attachment_manifest(&[bad], "@attachment:evil.png")
        .unwrap_err();
    let mut mime = text_descriptor("crash.log", DIGEST_B);
    mime.mime_type = "not a mime".to_string();
    validate_note_attachment_manifest(&[mime], "@attachment:crash.log")
        .unwrap_err();
    let mut dims = descriptor("login.png", DIGEST_A);
    dims.image = Some(AttachmentImageDimsWire {
        width: 0,
        height: 720,
    });
    validate_note_attachment_manifest(&[dims], "@attachment:login.png")
        .unwrap_err();
}

#[test]
fn tombstone_round_trips_and_validates() {
    let tombstone = AttachmentTombstoneWire {
        schema_version: ATTACHMENT_TOMBSTONE_WIRE_SCHEMA_VERSION,
        sha256: DIGEST_A.to_string(),
        purged_at: "2026-09-29T12:00:00Z".to_string(),
        actor: "bryan".to_string(),
        reason: "contains a credential".to_string(),
    };
    tombstone.validate().unwrap();
    let json = serde_json::to_value(&tombstone).unwrap();
    let decoded: AttachmentTombstoneWire =
        serde_json::from_value(json).unwrap();
    assert_eq!(decoded, tombstone);
    let mut bad_version = tombstone.clone();
    bad_version.schema_version = 999;
    bad_version.validate().unwrap_err();
    let mut bad_sha = tombstone.clone();
    bad_sha.sha256 = "xyz".to_string();
    bad_sha.validate().unwrap_err();
    let mut blank = tombstone.clone();
    blank.reason = "  ".to_string();
    blank.validate().unwrap_err();
}

#[test]
fn placement_picks_first_accepting_tier() {
    let tiers = vec![
        AttachmentStoreTierWire {
            name: "git".to_string(),
            max_bytes: Some(50 * 1024 * 1024),
        },
        AttachmentStoreTierWire {
            name: "large".to_string(),
            max_bytes: Some(2 * 1024 * 1024 * 1024),
        },
    ];
    assert_eq!(
        attachment_placement(1024, &tiers, false).unwrap(),
        AttachmentPlacementWire::Store {
            store: "git".to_string()
        }
    );
    assert_eq!(
        attachment_placement(100 * 1024 * 1024, &tiers, false).unwrap(),
        AttachmentPlacementWire::Store {
            store: "large".to_string()
        }
    );
    assert_eq!(
        attachment_placement(1024, &tiers, true).unwrap(),
        AttachmentPlacementWire::LocalOnly
    );
    let error = attachment_placement(4 * 1024 * 1024 * 1024, &tiers, false)
        .unwrap_err();
    assert!(error.to_string().contains("no configured attachment store"));
}

#[test]
fn auto_fetch_cap_is_inclusive() {
    assert!(attachment_should_auto_fetch(
        25 * 1024 * 1024,
        25 * 1024 * 1024
    ));
    assert!(!attachment_should_auto_fetch(
        25 * 1024 * 1024 + 1,
        25 * 1024 * 1024
    ));
}

#[test]
fn sensitive_paths_are_refused() {
    let home = "/home/bryan";
    for path in [
        "/home/bryan/.ssh/id_ed25519",
        "/home/bryan/.ssh/known_hosts",
        "/home/bryan/.gnupg/pubring.kbx",
        "/home/bryan/work/.env",
        "/home/bryan/work/.env.production",
        "/tmp/build/server.pem",
        "/tmp/build/server.key",
        "/tmp/x/id_rsa_backup",
        "/tmp/x/id_ed25519_old",
        "/tmp/vault.kdbx",
        "/home/bryan/proj/credentials.json",
        "/home/bryan/.netrc",
        "/home/bryan/.aws/credentials",
        "/home/bryan/.git-credentials",
        "/home/bryan/.config/gh/hosts.yml",
    ] {
        assert!(
            attachment_sensitive_path_reason(path, home, &[]).is_some(),
            "{path} should be refused"
        );
    }
    for path in [
        "/home/bryan/shots/login.png",
        "/tmp/crash.log",
        "/home/bryan/.ssh_config_pub",
        "/home/bryan/work/env.txt",
        "/home/bryan/pemrose.pem.txt",
    ] {
        assert!(
            attachment_sensitive_path_reason(path, home, &[]).is_none(),
            "{path} should be allowed"
        );
    }
    // Configured extras extend the refusal set.
    assert!(attachment_sensitive_path_reason(
        "/srv/secrets/token",
        home,
        &["/srv/secrets/**".to_string()]
    )
    .is_some());
    assert!(attachment_sensitive_path_reason(
        "/srv/public/readme.md",
        home,
        &["/srv/secrets/**".to_string()]
    )
    .is_none());
}
