//! Ordered attachment audience decision (`attachment_audience_decision`).
//!
//! Pure first-match rules 0-9 over [`AttachmentAudienceFactsWire`]. Python
//! gathers facts; core decides so every frontend agrees. Uncertainty
//! resolves to private. Reasons stay generic and local, never carrying a
//! secret value.

use serde::{Deserialize, Serialize};

use super::extensions::AttachmentClassWire;
use super::manifest::attachment_sensitive_path_reason;
use super::manifest::AttachmentVisibilityWire;
use super::scanner::AttachmentScanWire;
use super::zones::{
    is_personal_zone_path, is_sase_personal_path, is_sase_publishable_path,
    is_sase_secret_file, path_is_within_root,
};

/// What the author asked for.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentAudienceRequestWire {
    #[default]
    Auto,
    Public,
    Private,
    LocalOnly,
}

/// Who is asking.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentAudienceActorWire {
    Human,
    #[default]
    Agent,
}

/// Policy outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentAudienceOutcomeWire {
    Public,
    Private,
    LocalOnly,
    Refuse,
    Confirm,
}

/// First-match rule that produced the outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentAudienceRuleWire {
    StorePrivate,
    SensitivePath,
    Explicit,
    SizeCap,
    ScanHit,
    PrivateProvenance,
    OpaqueType,
    UnverifiedMedia,
    PublicEvidence,
    FailPrivate,
}

impl AttachmentAudienceRuleWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StorePrivate => "store_private",
            Self::SensitivePath => "sensitive_path",
            Self::Explicit => "explicit",
            Self::SizeCap => "size_cap",
            Self::ScanHit => "scan_hit",
            Self::PrivateProvenance => "private_provenance",
            Self::OpaqueType => "opaque_type",
            Self::UnverifiedMedia => "unverified_media",
            Self::PublicEvidence => "public_evidence",
            Self::FailPrivate => "fail_private",
        }
    }
}

/// Visibility of the checkout remote.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentRemoteVisibilityWire {
    Public,
    Private,
    #[default]
    Unknown,
}

/// Git facts for the source path, when known.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AttachmentAudienceCheckoutWire {
    pub root: String,
    #[serde(default)]
    pub remote_visibility: AttachmentRemoteVisibilityWire,
    #[serde(default)]
    pub ignored: bool,
    #[serde(default)]
    pub tracked_identical_to_remote: bool,
}

/// Facts Python gathers for one attachment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentAudienceFactsWire {
    #[serde(default)]
    pub bead_store_visibility: AttachmentVisibilityWire,
    #[serde(default)]
    pub requested: AttachmentAudienceRequestWire,
    #[serde(default)]
    pub actor: AttachmentAudienceActorWire,
    #[serde(default)]
    pub confirmed: bool,
    #[serde(default)]
    pub allow_sensitive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub home: String,
    #[serde(default)]
    pub sase_home: String,
    #[serde(default)]
    pub extra_sensitive_patterns: Vec<String>,
    pub size_bytes: u64,
    pub public_max_bytes: u64,
    pub class: AttachmentClassWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<AttachmentScanWire>,
    #[serde(default)]
    pub owner_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkout: Option<AttachmentAudienceCheckoutWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    #[serde(default)]
    pub scratch_roots: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produced_during_run: Option<bool>,
}

/// Decision returned to Python.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentAudienceDecisionWire {
    pub outcome: AttachmentAudienceOutcomeWire,
    pub rule: AttachmentAudienceRuleWire,
    pub reason: String,
    pub widenable: bool,
}

fn decide(
    outcome: AttachmentAudienceOutcomeWire,
    rule: AttachmentAudienceRuleWire,
    reason: &str,
    widenable: bool,
) -> AttachmentAudienceDecisionWire {
    AttachmentAudienceDecisionWire {
        outcome,
        rule,
        reason: reason.to_string(),
        widenable,
    }
}

fn sensitive_hit(facts: &AttachmentAudienceFactsWire, path: &str) -> bool {
    if attachment_sensitive_path_reason(
        path,
        &facts.home,
        &facts.extra_sensitive_patterns,
    )
    .is_some()
    {
        return true;
    }
    is_sase_secret_file(path, &facts.sase_home)
}

fn in_publishable_location(
    facts: &AttachmentAudienceFactsWire,
    path: &str,
) -> bool {
    if let Some(root) = facts.workspace_root.as_deref() {
        if !root.trim().is_empty() && path_is_within_root(path, root) {
            return true;
        }
    }
    for root in &facts.scratch_roots {
        if !root.trim().is_empty() && path_is_within_root(path, root) {
            return true;
        }
    }
    is_sase_publishable_path(path, &facts.sase_home)
}

fn public_checkout(facts: &AttachmentAudienceFactsWire) -> bool {
    facts.checkout.as_ref().is_some_and(|checkout| {
        checkout.remote_visibility == AttachmentRemoteVisibilityWire::Public
    })
}

fn tracked_identical_public(facts: &AttachmentAudienceFactsWire) -> bool {
    facts.checkout.as_ref().is_some_and(|checkout| {
        checkout.remote_visibility == AttachmentRemoteVisibilityWire::Public
            && checkout.tracked_identical_to_remote
            && !checkout.ignored
    })
}

fn private_provenance_hit(
    facts: &AttachmentAudienceFactsWire,
    path: &str,
) -> bool {
    if facts.owner_only {
        return true;
    }
    if let Some(checkout) = facts.checkout.as_ref() {
        if checkout.ignored {
            return true;
        }
        if checkout.remote_visibility != AttachmentRemoteVisibilityWire::Public
        {
            return true;
        }
    }
    if public_checkout(facts) {
        return false;
    }
    if is_personal_zone_path(path, &facts.home) {
        return true;
    }
    if is_sase_personal_path(path, &facts.sase_home) {
        return true;
    }
    false
}

fn scan_hit_kind(facts: &AttachmentAudienceFactsWire) -> Option<String> {
    let scan = facts.scan.as_ref()?;
    if scan.outcome != super::scanner::AttachmentScanOutcomeWire::Hit {
        return None;
    }
    let hit = scan.hit.as_ref()?;
    Some(format!("{:?}", hit.kind))
}

/// First-match rules 0-9, then widening for `requested: public`.
///
/// Never returns a secret value in `reason`. `allow_sensitive` with
/// `public` always refuses. `-W` with `-L` is rejected in Python before
/// core (the request is single-valued); a `public` request over a
/// `local_only` policy outcome also refuses here.
pub fn attachment_audience_decision(
    facts: &AttachmentAudienceFactsWire,
) -> AttachmentAudienceDecisionWire {
    // Rule 0: private bead store.
    if facts.bead_store_visibility == AttachmentVisibilityWire::Private {
        let base = decide(
            AttachmentAudienceOutcomeWire::Private,
            AttachmentAudienceRuleWire::StorePrivate,
            "bead store is private",
            false,
        );
        return apply_widening(facts, base);
    }

    // Rule 1: sensitive path.
    if let Some(path) = facts.path.as_deref() {
        if sensitive_hit(facts, path) {
            if facts.allow_sensitive {
                let outcome = if facts.requested
                    == AttachmentAudienceRequestWire::LocalOnly
                {
                    AttachmentAudienceOutcomeWire::LocalOnly
                } else {
                    AttachmentAudienceOutcomeWire::Private
                };
                let base = decide(
                    outcome,
                    AttachmentAudienceRuleWire::SensitivePath,
                    "sensitive path (allowed, staying private)",
                    false,
                );
                return apply_widening(facts, base);
            }
            let base = decide(
                AttachmentAudienceOutcomeWire::Refuse,
                AttachmentAudienceRuleWire::SensitivePath,
                "sensitive path",
                false,
            );
            return apply_widening(facts, base);
        }
    }

    // Rule 2: explicit private / local-only.
    if facts.requested == AttachmentAudienceRequestWire::Private {
        return decide(
            AttachmentAudienceOutcomeWire::Private,
            AttachmentAudienceRuleWire::Explicit,
            "explicit private request",
            false,
        );
    }
    if facts.requested == AttachmentAudienceRequestWire::LocalOnly {
        return decide(
            AttachmentAudienceOutcomeWire::LocalOnly,
            AttachmentAudienceRuleWire::Explicit,
            "explicit local-only request",
            false,
        );
    }

    // Rule 3: size cap.
    if facts.size_bytes > facts.public_max_bytes {
        let base = decide(
            AttachmentAudienceOutcomeWire::Private,
            AttachmentAudienceRuleWire::SizeCap,
            "exceeds public size cap",
            false,
        );
        return apply_widening(facts, base);
    }

    // Rule 4: scan hit.
    if facts.scan.as_ref().is_some_and(|scan| {
        scan.outcome == super::scanner::AttachmentScanOutcomeWire::Hit
    }) {
        let kind = facts
            .scan
            .as_ref()
            .and_then(|scan| scan.hit.as_ref().map(|hit| hit.kind));
        let known =
            kind == Some(super::scanner::AttachmentScanHitKindWire::KnownValue);
        let reason = match kind {
            Some(super::scanner::AttachmentScanHitKindWire::KnownValue) => {
                "scanner found a known secret value"
            }
            Some(
                super::scanner::AttachmentScanHitKindWire::CredentialPattern,
            ) => "scanner found a credential pattern",
            _ => "scanner found an environment dump",
        };
        let base = decide(
            AttachmentAudienceOutcomeWire::Private,
            AttachmentAudienceRuleWire::ScanHit,
            reason,
            !known,
        );
        let _ = scan_hit_kind(facts);
        return apply_widening(facts, base);
    }

    // Rules 5-8 need a source path; stdin (None) falls to rule 9.
    if let Some(path) = facts.path.as_deref() {
        // Rule 5: private provenance.
        if private_provenance_hit(facts, path) {
            let base = decide(
                AttachmentAudienceOutcomeWire::Private,
                AttachmentAudienceRuleWire::PrivateProvenance,
                "private provenance",
                true,
            );
            return apply_widening(facts, base);
        }

        // Rule 6: opaque type or unreadable content.
        let opaque = matches!(
            facts.class,
            AttachmentClassWire::Archive
                | AttachmentClassWire::Binary
                | AttachmentClassWire::Pdf
                | AttachmentClassWire::Audio
        );
        let unreadable = facts.scan.as_ref().is_some_and(|scan| {
            scan.outcome == super::scanner::AttachmentScanOutcomeWire::Skipped
        });
        if opaque || unreadable {
            let base = decide(
                AttachmentAudienceOutcomeWire::Private,
                AttachmentAudienceRuleWire::OpaqueType,
                "opaque file type",
                true,
            );
            return apply_widening(facts, base);
        }

        // Rule 7: unverified image/video.
        if matches!(
            facts.class,
            AttachmentClassWire::Image | AttachmentClassWire::Video
        ) {
            let produced_here = facts.produced_during_run == Some(true)
                && in_publishable_location(facts, path);
            if !produced_here && !tracked_identical_public(facts) {
                let base = decide(
                    AttachmentAudienceOutcomeWire::Private,
                    AttachmentAudienceRuleWire::UnverifiedMedia,
                    "image not produced in this run",
                    true,
                );
                return apply_widening(facts, base);
            }
        }

        // Rule 8: positive public evidence.
        if tracked_identical_public(facts) {
            let base = decide(
                AttachmentAudienceOutcomeWire::Public,
                AttachmentAudienceRuleWire::PublicEvidence,
                "already public",
                false,
            );
            return apply_widening(facts, base);
        }
        if facts.class == AttachmentClassWire::Text {
            let clean = facts.scan.as_ref().is_some_and(|scan| {
                scan.outcome == super::scanner::AttachmentScanOutcomeWire::Clean
            });
            if clean && in_publishable_location(facts, path) {
                let base = decide(
                    AttachmentAudienceOutcomeWire::Public,
                    AttachmentAudienceRuleWire::PublicEvidence,
                    "workspace text",
                    false,
                );
                return apply_widening(facts, base);
            }
        }
        if matches!(
            facts.class,
            AttachmentClassWire::Image | AttachmentClassWire::Video
        ) && facts.produced_during_run == Some(true)
            && in_publishable_location(facts, path)
        {
            let base = decide(
                AttachmentAudienceOutcomeWire::Public,
                AttachmentAudienceRuleWire::PublicEvidence,
                "self-produced media",
                false,
            );
            return apply_widening(facts, base);
        }
    }

    // Rule 9: fail private.
    let base = decide(
        AttachmentAudienceOutcomeWire::Private,
        AttachmentAudienceRuleWire::FailPrivate,
        "no public evidence",
        true,
    );
    apply_widening(facts, base)
}

fn apply_widening(
    facts: &AttachmentAudienceFactsWire,
    base: AttachmentAudienceDecisionWire,
) -> AttachmentAudienceDecisionWire {
    if facts.requested != AttachmentAudienceRequestWire::Public {
        return base;
    }
    if facts.allow_sensitive {
        return decide(
            AttachmentAudienceOutcomeWire::Refuse,
            base.rule,
            "allow-sensitive cannot be combined with public",
            false,
        );
    }
    match base.outcome {
        AttachmentAudienceOutcomeWire::Public => base,
        AttachmentAudienceOutcomeWire::LocalOnly => decide(
            AttachmentAudienceOutcomeWire::Refuse,
            base.rule,
            "local-only cannot be widened to public",
            false,
        ),
        AttachmentAudienceOutcomeWire::Refuse => base,
        AttachmentAudienceOutcomeWire::Confirm => base,
        AttachmentAudienceOutcomeWire::Private => {
            if !base.widenable {
                return decide(
                    AttachmentAudienceOutcomeWire::Refuse,
                    base.rule,
                    "cannot be widened to public",
                    false,
                );
            }
            if facts.actor == AttachmentAudienceActorWire::Agent {
                return decide(
                    AttachmentAudienceOutcomeWire::Refuse,
                    base.rule,
                    "SASE classified it private; a human can publish it later with `sase bead attachment publish`",
                    true,
                );
            }
            if facts.confirmed {
                return decide(
                    AttachmentAudienceOutcomeWire::Public,
                    base.rule,
                    "human confirmed widening",
                    true,
                );
            }
            decide(
                AttachmentAudienceOutcomeWire::Confirm,
                base.rule,
                "requires human confirmation",
                true,
            )
        }
    }
}
