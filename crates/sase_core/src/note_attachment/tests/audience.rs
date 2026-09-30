//! Decision-table tests: every rule, precedence, widening, and zones.

use std::collections::HashMap;

use crate::note_attachment::{
    attachment_audience_decision, AttachmentAudienceActorWire,
    AttachmentAudienceCheckoutWire, AttachmentAudienceDecisionWire,
    AttachmentAudienceFactsWire, AttachmentAudienceOutcomeWire,
    AttachmentAudienceRequestWire, AttachmentAudienceRuleWire,
    AttachmentClassWire, AttachmentRemoteVisibilityWire,
    AttachmentScanHitKindWire, AttachmentScanHitWire,
    AttachmentScanOutcomeWire, AttachmentScanWire, AttachmentVisibilityWire,
    ATTACHMENT_SCANNER_RULES_VERSION,
};

fn base_facts() -> AttachmentAudienceFactsWire {
    AttachmentAudienceFactsWire {
        bead_store_visibility: AttachmentVisibilityWire::Public,
        requested: AttachmentAudienceRequestWire::Auto,
        actor: AttachmentAudienceActorWire::Human,
        confirmed: false,
        allow_sensitive: false,
        path: Some("/work/repo/build.log".to_string()),
        home: "/home/bryan".to_string(),
        sase_home: "/home/bryan/.sase".to_string(),
        extra_sensitive_patterns: Vec::new(),
        size_bytes: 1024,
        public_max_bytes: 25 * 1024 * 1024,
        class: AttachmentClassWire::Text,
        scan: Some(AttachmentScanWire {
            outcome: AttachmentScanOutcomeWire::Clean,
            hit: None,
            bytes_scanned: 1024,
            rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
        }),
        owner_only: false,
        checkout: None,
        workspace_root: Some("/work/repo".to_string()),
        scratch_roots: vec!["/tmp/sase-scratch".to_string()],
        produced_during_run: Some(false),
    }
}

fn decide(
    facts: &AttachmentAudienceFactsWire,
) -> AttachmentAudienceDecisionWire {
    attachment_audience_decision(facts)
}

#[test]
fn rule_zero_private_store_never_widens() {
    let mut facts = base_facts();
    facts.bead_store_visibility = AttachmentVisibilityWire::Private;
    facts.requested = AttachmentAudienceRequestWire::Public;
    facts.actor = AttachmentAudienceActorWire::Human;
    facts.confirmed = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::StorePrivate);
    assert!(!decision.widenable);
}

#[test]
fn rule_one_sensitive_path_refuses() {
    let mut facts = base_facts();
    facts.path = Some("/home/bryan/.ssh/id_ed25519".to_string());
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::SensitivePath);
    assert!(!decision.widenable);

    // SASE secret files refuse too.
    facts.path = Some("/home/bryan/.sase/telegram_bot_token".to_string());
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::SensitivePath);

    // allow_sensitive stays private, never public.
    facts.allow_sensitive = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert!(!decision.widenable);
    facts.requested = AttachmentAudienceRequestWire::Public;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
}

#[test]
fn rule_two_explicit_private_and_local_only() {
    let mut facts = base_facts();
    facts.requested = AttachmentAudienceRequestWire::Private;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::Explicit);

    facts.requested = AttachmentAudienceRequestWire::LocalOnly;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::LocalOnly);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::Explicit);
}

#[test]
fn rule_three_size_cap_never_widens() {
    let mut facts = base_facts();
    facts.size_bytes = 26 * 1024 * 1024;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::SizeCap);
    assert!(!decision.widenable);
    facts.requested = AttachmentAudienceRequestWire::Public;
    facts.confirmed = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
}

#[test]
fn rule_four_scan_hits() {
    // Known values never widen.
    let mut facts = base_facts();
    facts.scan = Some(AttachmentScanWire {
        outcome: AttachmentScanOutcomeWire::Hit,
        hit: Some(AttachmentScanHitWire {
            kind: AttachmentScanHitKindWire::KnownValue,
            rule_id: "known-value".to_string(),
            line: 3,
        }),
        bytes_scanned: 100,
        rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
    });
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::ScanHit);
    assert!(!decision.widenable);
    facts.requested = AttachmentAudienceRequestWire::Public;
    facts.confirmed = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);

    // Credential patterns widen for humans with confirmation.
    facts.scan = Some(AttachmentScanWire {
        outcome: AttachmentScanOutcomeWire::Hit,
        hit: Some(AttachmentScanHitWire {
            kind: AttachmentScanHitKindWire::CredentialPattern,
            rule_id: "github-token".to_string(),
            line: 1,
        }),
        bytes_scanned: 100,
        rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
    });
    facts.requested = AttachmentAudienceRequestWire::Auto;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert!(decision.widenable);

    // Env dumps widen too.
    facts.scan = Some(AttachmentScanWire {
        outcome: AttachmentScanOutcomeWire::Hit,
        hit: Some(AttachmentScanHitWire {
            kind: AttachmentScanHitKindWire::EnvDump,
            rule_id: "env-dump-window".to_string(),
            line: 10,
        }),
        bytes_scanned: 200,
        rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
    });
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::ScanHit);
    assert!(decision.widenable);
}

#[test]
fn rule_five_private_provenance() {
    // Owner-only.
    let mut facts = base_facts();
    facts.owner_only = true;
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);
    assert!(decision.widenable);

    // Ignored.
    facts.owner_only = false;
    facts.checkout = Some(AttachmentAudienceCheckoutWire {
        root: "/work/repo".to_string(),
        remote_visibility: AttachmentRemoteVisibilityWire::Public,
        ignored: true,
        tracked_identical_to_remote: false,
    });
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);

    // Private remote.
    facts.checkout = Some(AttachmentAudienceCheckoutWire {
        root: "/work/repo".to_string(),
        remote_visibility: AttachmentRemoteVisibilityWire::Private,
        ignored: false,
        tracked_identical_to_remote: false,
    });
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);

    // Unknown remote fails private.
    facts.checkout = Some(AttachmentAudienceCheckoutWire {
        root: "/work/repo".to_string(),
        remote_visibility: AttachmentRemoteVisibilityWire::Unknown,
        ignored: false,
        tracked_identical_to_remote: false,
    });
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);

    // Personal zones.
    facts.checkout = None;
    facts.path = Some("/home/bryan/Documents/notes.md".to_string());
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);
    facts.path = Some("/home/bryan/.sase/notifications/n.jsonl".to_string());
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);
}

#[test]
fn rule_five_skipped_inside_public_checkout() {
    // A public dotfiles checkout under a personal-zone dir is judged by
    // checkout facts, not zone membership.
    let mut facts = base_facts();
    facts.path = Some("/home/bryan/.local/share/dotfiles/vimrc".to_string());
    facts.checkout = Some(AttachmentAudienceCheckoutWire {
        root: "/home/bryan/.local/share/dotfiles".to_string(),
        remote_visibility: AttachmentRemoteVisibilityWire::Public,
        ignored: false,
        tracked_identical_to_remote: true,
    });
    facts.class = AttachmentClassWire::Text;
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PublicEvidence);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Public);
}

#[test]
fn rule_six_opaque_types() {
    for class in [
        AttachmentClassWire::Archive,
        AttachmentClassWire::Binary,
        AttachmentClassWire::Pdf,
        AttachmentClassWire::Audio,
    ] {
        let mut facts = base_facts();
        facts.class = class;
        facts.scan = None;
        let decision = decide(&facts);
        assert_eq!(
            decision.rule,
            AttachmentAudienceRuleWire::OpaqueType,
            "{class:?}"
        );
        assert!(decision.widenable);
    }
    // Skipped scans read as opaque.
    let mut facts = base_facts();
    facts.scan = Some(AttachmentScanWire {
        outcome: AttachmentScanOutcomeWire::Skipped,
        hit: None,
        bytes_scanned: 0,
        rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
    });
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::OpaqueType);
}

#[test]
fn rule_seven_unverified_media() {
    // Human screenshot not produced in this run stays private.
    let mut facts = base_facts();
    facts.class = AttachmentClassWire::Image;
    facts.scan = None;
    facts.produced_during_run = None;
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::UnverifiedMedia);

    // No run window is private.
    facts.produced_during_run = None;
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::UnverifiedMedia);

    // Produced outside publishable locations stays private.
    facts.produced_during_run = Some(true);
    facts.path = Some("/home/bryan/Documents/shot.png".to_string());
    facts.checkout = None;
    let decision = decide(&facts);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PrivateProvenance);
}

#[test]
fn rule_eight_public_evidence() {
    // Scan-clean workspace text.
    let facts = base_facts();
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Public);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PublicEvidence);

    // Self-produced media in a scratch root.
    let mut facts = base_facts();
    facts.class = AttachmentClassWire::Image;
    facts.scan = None;
    facts.path = Some("/tmp/sase-scratch/shot.png".to_string());
    facts.produced_during_run = Some(true);
    facts.checkout = None;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Public);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::PublicEvidence);
}

#[test]
fn rule_nine_stdin_fails_private() {
    let mut facts = base_facts();
    facts.path = None;
    facts.scan = None;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Private);
    assert_eq!(decision.rule, AttachmentAudienceRuleWire::FailPrivate);
    assert!(decision.widenable);
}

#[test]
fn widening_agent_refuses_human_confirms() {
    // Agent over widenable private refuses with a publish hint.
    let mut facts = base_facts();
    facts.path = None;
    facts.scan = None;
    facts.requested = AttachmentAudienceRequestWire::Public;
    facts.actor = AttachmentAudienceActorWire::Agent;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
    assert!(decision.reason.contains("sase bead attachment publish"));

    // Human without confirmation gets Confirm.
    facts.actor = AttachmentAudienceActorWire::Human;
    facts.confirmed = false;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Confirm);

    // Human with confirmation gets Public.
    facts.confirmed = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Public);

    // allow_sensitive plus public always refuses.
    facts.allow_sensitive = true;
    facts.confirmed = true;
    let decision = decide(&facts);
    assert_eq!(decision.outcome, AttachmentAudienceOutcomeWire::Refuse);
    assert!(!decision.widenable);

    let _ = HashMap::<String, String>::new();
}
