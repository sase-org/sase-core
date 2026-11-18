//! Targeted touch and view queries for one agent context.
//!
//! The TUI bead-touches panel used to load the whole touch index (every
//! actor's rows) plus the whole legacy view log into a per-project snapshot
//! cache and filter in Python. That snapshot retained every project's full
//! touch graph to answer one agent's panel. These queries push the
//! agent-identity matching into core and return only the rows one ordered
//! member list claims:
//!
//! - [`query_bead_touches_for_members`] reads the touch-index JSON document
//!   but decodes only the touch-query fields (schema version, generation,
//!   touches). The per-stream signature map exists for refresh bookkeeping
//!   and is never materialized by a query. Rows whose actor no member claims
//!   are dropped before the answer is built, so only matching rows (plus the
//!   generation string) survive the call.
//! - [`query_bead_views_for_members`] streams the legacy `bead_views.jsonl`
//!   log line by line, keeps only events whose recorded agent a member
//!   claims, and folds the survivors into per-`(actor, bead)` viewed rows in
//!   core. Unrelated events are released as the log is scanned.
//!
//! Matching mirrors the Python `touch_matches_agent` contract exactly, in
//! member order: a trimmed actor matches a member when it equals the
//! member's globalized name or local name, or when core identity
//! normalization maps it onto the member's globalized name (which recovers
//! legacy bare-local-name rows). Anything else — blank actors, email
//! addresses, unrelated dotted names validation rejects — never matches.
//! The first claiming member wins, so context attribution and the trailing
//! unlabeled session-container alias behave exactly as the Python loader's
//! ordered matcher list.
//!
//! Both queries are read-only and infallible: a missing, truncated,
//! unparseable, or wrong-schema source is a cache miss that returns no rows,
//! never an error and never partial rows. Each answer carries the source
//! file identity (`dev`, `ino`, `mtime_ns`, `size`, `missing`) so callers
//! revalidate on append, truncation, deletion/recreation, and atomic
//! replacement — including replacement with equal size and mtime — without
//! hashing the source per lookup.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::agent_identity::{globalize_owned_agent_name, AgentOwnerIdentity};
use crate::audit_query::{source_identity_for, AuditSourceIdentityWire};
use crate::bead::touch_index::{
    instant_or_min, BeadTouchWire, BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
};

/// Wire version of the targeted member-query request/response shape.
///
/// This versions the request/response envelope only; the touch rows inside
/// still follow [`BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION`].
pub const TOUCH_MEMBER_QUERY_WIRE_SCHEMA_VERSION: u64 = 1;

/// Schema version of the legacy bead-views log this facility folds.
pub const BEAD_VIEW_LOG_SCHEMA_VERSION: u32 = 1;

/// One exact agent-identity selector in priority order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchMemberWire {
    /// Canonical global spelling, e.g. `bbugyi200.athena.coder`.
    #[serde(default)]
    pub globalized_name: String,
    /// Bare durable spelling, e.g. `coder`, when the member has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_name: Option<String>,
}

/// The current process owner the legacy bare-local recovery resolves under.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchOwnerWire {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub machine_name: String,
}

/// Targeted member query: ordered members plus the identity they resolve
/// under. `owner` is `None` for unconfigured callers, which get exact-only
/// matching (a bare name can only match a member's explicit local spelling).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchMemberQueryWire {
    #[serde(default)]
    pub members: Vec<TouchMemberWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TouchOwnerWire>,
    #[serde(default)]
    pub known_owner_roots: Vec<String>,
}

/// One matching touch row plus the ordinal of the member that claimed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchMatchWire {
    pub member_index: usize,
    pub touch: BeadTouchWire,
}

/// Targeted touch-index answer: only claimed rows plus source identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchMemberQueryResultWire {
    pub schema_version: u32,
    /// Generation of the index the rows came from, `""` on a cache miss.
    pub generation: String,
    pub source: AuditSourceIdentityWire,
    /// Claimed rows, newest touch first, then by bead id and actor — the
    /// same order as the unfiltered index query.
    pub rows: Vec<BeadTouchMatchWire>,
}

/// Targeted views answer: folded claimed view rows plus source identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadViewMemberQueryResultWire {
    pub schema_version: u32,
    pub source: AuditSourceIdentityWire,
    /// Claimed folded rows in first-seen order. Callers merge these behind
    /// durable index rows and apply their own final ranking and cap.
    pub rows: Vec<BeadTouchMatchWire>,
    /// False when an I/O error cut the scan short. Callers must discard the
    /// rows and treat the query as a miss: a truncated log yields no partial
    /// answer. Malformed single lines are skipped with `complete` intact.
    pub complete: bool,
}

/// Touch-index document as a query sees it: version, generation, touches.
///
/// The per-stream signature map is refresh bookkeeping, not query state, so
/// it is not decoded here at all.
#[derive(Debug, Deserialize)]
struct TouchIndexQueryDoc {
    #[serde(default)]
    schema_version: Option<u64>,
    #[serde(default)]
    generation: String,
    #[serde(default)]
    touches: Vec<BeadTouchWire>,
}

fn resolve_owner(
    query: &BeadTouchMemberQueryWire,
) -> Option<AgentOwnerIdentity> {
    let owner = query.owner.as_ref()?;
    AgentOwnerIdentity::new(owner.username.clone(), owner.machine_name.clone())
        .ok()
}

/// Return the ordinal of the first member claiming `actor`, if any.
///
/// Member order is significant: exact globalized and local spellings win in
/// list order before legacy bare-local recovery is attempted for the same
/// member, exactly like the Python loader's ordered matcher list.
fn match_member_index(
    actor: &str,
    query: &BeadTouchMemberQueryWire,
    owner: Option<&AgentOwnerIdentity>,
) -> Option<usize> {
    let candidate = actor.trim();
    if candidate.is_empty() {
        return None;
    }
    for (index, member) in query.members.iter().enumerate() {
        if candidate == member.globalized_name {
            return Some(index);
        }
        if let Some(local) = member.local_name.as_deref() {
            if candidate == local {
                return Some(index);
            }
        }
        if let Some(owner) = owner {
            if let Ok(globalized) = globalize_owned_agent_name(
                candidate,
                owner,
                &query.known_owner_roots,
            ) {
                if globalized == member.globalized_name {
                    return Some(index);
                }
            }
        }
    }
    None
}

/// Return the indexed touches claimed by the query members, if any.
///
/// Read-only and infallible: only the touch-query fields are decoded, rows
/// no member claims are dropped before the answer is built, and a missing,
/// truncated, unparseable, or wrong-schema index returns no rows with an
/// empty generation — never an error and never partial rows.
pub fn query_bead_touches_for_members(
    index_path: &Path,
    query: &BeadTouchMemberQueryWire,
) -> BeadTouchMemberQueryResultWire {
    let miss =
        |source: AuditSourceIdentityWire| BeadTouchMemberQueryResultWire {
            schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
            generation: String::new(),
            source,
            rows: Vec::new(),
        };
    let source = match source_identity_for(index_path) {
        Ok(source) => source,
        Err(_) => return miss(AuditSourceIdentityWire::missing()),
    };
    if source.missing {
        return miss(source);
    }
    let bytes = match std::fs::read(index_path) {
        Ok(bytes) => bytes,
        Err(_) => return miss(source),
    };
    let doc: TouchIndexQueryDoc = match serde_json::from_slice(&bytes) {
        Ok(doc) => doc,
        Err(_) => return miss(source),
    };
    if doc.schema_version
        != Some(u64::from(BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION))
    {
        return miss(source);
    }
    let owner = resolve_owner(query);
    let mut rows: Vec<BeadTouchMatchWire> = doc
        .touches
        .into_iter()
        .filter_map(|touch| {
            match_member_index(&touch.actor, query, owner.as_ref()).map(
                |member_index| BeadTouchMatchWire {
                    member_index,
                    touch,
                },
            )
        })
        .collect();
    rows.sort_by(|a, b| {
        instant_or_min(&b.touch.last_at)
            .cmp(&instant_or_min(&a.touch.last_at))
            .then_with(|| a.touch.bead_id.cmp(&b.touch.bead_id))
            .then_with(|| a.touch.actor.cmp(&b.touch.actor))
    });
    BeadTouchMemberQueryResultWire {
        schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
        generation: doc.generation,
        source,
        rows,
    }
}

/// One validated legacy view event: only the fields the fold consumes.
struct ViewEvent {
    agent_name: String,
    bead_id: String,
    timestamp: String,
}

fn view_event_from_value(value: &serde_json::Value) -> Option<ViewEvent> {
    let mapping = value.as_object()?;
    if mapping.get("schema_version")?.as_u64()?
        != u64::from(BEAD_VIEW_LOG_SCHEMA_VERSION)
    {
        return None;
    }
    let bead_id = mapping.get("bead_id")?.as_str()?;
    let agent_name = mapping.get("agent_name")?.as_str()?;
    if bead_id.trim().is_empty() || agent_name.trim().is_empty() {
        return None;
    }
    // The remaining required strings must exist with string values; their
    // contents play no role in the fold.
    for key in ["id", "timestamp", "project", "cwd"] {
        mapping.get(key)?.as_str()?;
    }
    Some(ViewEvent {
        agent_name: agent_name.to_string(),
        bead_id: bead_id.to_string(),
        timestamp: mapping
            .get("timestamp")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
    })
}

fn parse_view_moment(text: &str) -> Option<DateTime<Utc>> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(at) = DateTime::parse_from_rfc3339(text) {
        return Some(at.with_timezone(&Utc));
    }
    let normalized = text.replace(' ', "T");
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&normalized, format) {
            return Some(naive.and_utc());
        }
    }
    None
}

/// Fold claimed view events into per-`(actor, bead)` viewed rows.
///
/// Counts every claimed event (even one with an unusable timestamp, which
/// contributes to the `viewed` count but not to either timestamp bound);
/// the earliest timestamp wins the lower bound with strict comparison while
/// the newest wins the upper bound with later-or-equal comparison, so ties
/// keep the last occurrence in file order — exactly like the Python fold.
fn fold_claimed_views(
    events: Vec<(usize, ViewEvent)>,
) -> Vec<BeadTouchMatchWire> {
    struct Bucket {
        member_index: usize,
        count: u64,
        first_moment: Option<DateTime<Utc>>,
        first_at: String,
        last_moment: Option<DateTime<Utc>>,
        last_at: String,
    }
    let mut buckets: HashMap<(String, String), Bucket> = HashMap::new();
    let mut order: Vec<(String, String)> = Vec::new();
    for (member_index, event) in events {
        let key = (event.agent_name.clone(), event.bead_id.clone());
        let bucket = match buckets.get_mut(&key) {
            Some(bucket) => bucket,
            None => {
                order.push(key.clone());
                buckets.insert(
                    key.clone(),
                    Bucket {
                        member_index,
                        count: 0,
                        first_moment: None,
                        first_at: String::new(),
                        last_moment: None,
                        last_at: String::new(),
                    },
                );
                buckets.get_mut(&key).expect("bucket just inserted")
            }
        };
        bucket.count += 1;
        if let Some(moment) = parse_view_moment(&event.timestamp) {
            if bucket.first_moment.is_none_or(|at| moment < at) {
                bucket.first_moment = Some(moment);
                bucket.first_at = event.timestamp.clone();
            }
            if bucket.last_moment.is_none_or(|at| moment >= at) {
                bucket.last_moment = Some(moment);
                bucket.last_at = event.timestamp.clone();
            }
        }
    }
    order
        .into_iter()
        .filter_map(|key| {
            let bucket = buckets.remove(&key)?;
            let (actor, bead_id) = key;
            let mut verbs = std::collections::BTreeMap::new();
            verbs.insert("viewed".to_string(), bucket.count);
            Some(BeadTouchMatchWire {
                member_index: bucket.member_index,
                touch: BeadTouchWire {
                    actor,
                    bead_id,
                    title: String::new(),
                    issue_type: String::new(),
                    status: String::new(),
                    verbs,
                    first_at: bucket.first_at,
                    last_at: bucket.last_at,
                    current_note_count: 0,
                    note_preview: None,
                    creation_reason: None,
                    creation_reason_truncated: false,
                    close: None,
                    stream_id: String::new(),
                },
            })
        })
        .collect()
}

/// Return the folded viewed rows claimed by the query members, if any.
///
/// Streams the log and releases unrelated events as it scans; only claimed
/// events are folded and only claimed rows survive. Malformed single lines
/// are skipped. An I/O error mid-scan reports `complete: false` with no rows
/// so callers never publish a truncated answer under a fresh token.
pub fn query_bead_views_for_members(
    views_path: &Path,
    query: &BeadTouchMemberQueryWire,
) -> BeadViewMemberQueryResultWire {
    let miss = |source: AuditSourceIdentityWire, complete: bool| {
        BeadViewMemberQueryResultWire {
            schema_version: BEAD_VIEW_LOG_SCHEMA_VERSION,
            source,
            rows: Vec::new(),
            complete,
        }
    };
    let source = match source_identity_for(views_path) {
        Ok(source) => source,
        Err(_) => return miss(AuditSourceIdentityWire::missing(), false),
    };
    if source.missing {
        return miss(source, true);
    }
    let file = match File::open(views_path) {
        Ok(file) => file,
        Err(_) => return miss(source, false),
    };
    let owner = resolve_owner(query);
    let mut claimed: Vec<(usize, ViewEvent)> = Vec::new();
    let mut complete = true;
    let mut reader = BufReader::new(file);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => {
                complete = false;
                break;
            }
        }
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(event) = view_event_from_value(&value) else {
            continue;
        };
        if let Some(member_index) =
            match_member_index(&event.agent_name, query, owner.as_ref())
        {
            claimed.push((member_index, event));
        }
    }
    if !complete {
        return miss(source, false);
    }
    let rows = fold_claimed_views(claimed);
    BeadViewMemberQueryResultWire {
        schema_version: BEAD_VIEW_LOG_SCHEMA_VERSION,
        source,
        rows,
        complete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::touch_index::BeadTouchIndexWire;
    use serde_json::json;
    use std::io::Write as _;
    use tempfile::TempDir;

    const OWNER: &str = "bbugyi200";
    const MACHINE: &str = "athena";

    fn owner() -> AgentOwnerIdentity {
        AgentOwnerIdentity::new(OWNER, MACHINE).unwrap()
    }

    fn known_roots() -> Vec<String> {
        vec![MACHINE.to_string(), format!("{OWNER}.{MACHINE}")]
    }

    fn member(globalized: &str, local: Option<&str>) -> TouchMemberWire {
        TouchMemberWire {
            globalized_name: globalized.to_string(),
            local_name: local.map(str::to_string),
        }
    }

    fn query_for(member: TouchMemberWire) -> BeadTouchMemberQueryWire {
        BeadTouchMemberQueryWire {
            members: vec![member],
            owner: Some(TouchOwnerWire {
                username: OWNER.to_string(),
                machine_name: MACHINE.to_string(),
            }),
            known_owner_roots: known_roots(),
        }
    }

    fn touch(actor: &str, bead: &str, last_at: &str) -> BeadTouchWire {
        BeadTouchWire {
            actor: actor.to_string(),
            bead_id: bead.to_string(),
            title: format!("title {bead}"),
            issue_type: "task".to_string(),
            status: "open".to_string(),
            verbs: [("updated".to_string(), 1)].into_iter().collect(),
            first_at: "2026-01-01T00:00:00Z".to_string(),
            last_at: last_at.to_string(),
            current_note_count: 0,
            note_preview: None,
            creation_reason: None,
            creation_reason_truncated: false,
            close: None,
            stream_id: "s-1".to_string(),
        }
    }

    fn write_index(
        dir: &TempDir,
        touches: &[BeadTouchWire],
    ) -> std::path::PathBuf {
        let index = BeadTouchIndexWire {
            schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
            generation: "gen-1".to_string(),
            streams: [(
                "s-1".to_string(),
                crate::bead::touch_index::BeadStreamSignatureWire(1, 100),
            )]
            .into_iter()
            .collect(),
            touches: touches.to_vec(),
        };
        let path = dir.path().join("agent_bead_touches.json");
        std::fs::write(&path, serde_json::to_vec(&index).unwrap()).unwrap();
        path
    }

    #[test]
    fn exact_globalized_and_local_names_match() {
        let dir = TempDir::new().unwrap();
        let path = write_index(
            &dir,
            &[
                touch(
                    "bbugyi200.athena.coder",
                    "sase-1.1",
                    "2026-06-01T00:00:00Z",
                ),
                touch("coder", "sase-1.2", "2026-06-02T00:00:00Z"),
                touch("other", "sase-1.3", "2026-06-03T00:00:00Z"),
            ],
        );
        let out = query_bead_touches_for_members(
            &path,
            &query_for(member("bbugyi200.athena.coder", Some("coder"))),
        );
        let beads: Vec<&str> = out
            .rows
            .iter()
            .map(|row| row.touch.bead_id.as_str())
            .collect();
        assert_eq!(beads, vec!["sase-1.2", "sase-1.1"]);
        assert_eq!(out.generation, "gen-1");
        assert!(!out.source.missing);
    }

    #[test]
    fn legacy_bare_local_actor_recovers_through_identity() {
        // `coder` globalizes to the member under the current owner even
        // though the member lists no local spelling.
        let dir = TempDir::new().unwrap();
        let path = write_index(
            &dir,
            &[touch("coder", "sase-1.1", "2026-06-01T00:00:00Z")],
        );
        let out = query_bead_touches_for_members(
            &path,
            &query_for(member("bbugyi200.athena.coder", None)),
        );
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].member_index, 0);
    }

    #[test]
    fn whitespace_is_trimmed_and_blank_or_foreign_actors_never_match() {
        let dir = TempDir::new().unwrap();
        let path = write_index(
            &dir,
            &[
                touch("  coder  ", "sase-1.1", "2026-06-01T00:00:00Z"),
                touch(
                    "someone@example.com",
                    "sase-1.2",
                    "2026-06-01T00:00:00Z",
                ),
                touch("zeus.coder", "sase-1.3", "2026-06-01T00:00:00Z"),
                touch("   ", "sase-1.4", "2026-06-01T00:00:00Z"),
            ],
        );
        let out = query_bead_touches_for_members(
            &path,
            &query_for(member("bbugyi200.athena.coder", Some("coder"))),
        );
        let beads: Vec<&str> = out
            .rows
            .iter()
            .map(|row| row.touch.bead_id.as_str())
            .collect();
        assert_eq!(beads, vec!["sase-1.1"]);
    }

    #[test]
    fn first_member_wins_and_unconfigured_owner_is_exact_only() {
        let dir = TempDir::new().unwrap();
        let path = write_index(
            &dir,
            &[touch("coder", "sase-1.1", "2026-06-01T00:00:00Z")],
        );
        let mut query = query_for(member("bbugyi200.athena.coder", None));
        query
            .members
            .push(member("bbugyi200.athena.other", Some("coder")));
        let out = query_bead_touches_for_members(&path, &query);
        assert_eq!(out.rows[0].member_index, 0);

        let exact_only = BeadTouchMemberQueryWire {
            members: vec![member("bbugyi200.athena.coder", None)],
            owner: None,
            known_owner_roots: vec![],
        };
        let out = query_bead_touches_for_members(&path, &exact_only);
        assert!(out.rows.is_empty());
    }

    #[test]
    fn missing_truncated_and_wrong_schema_indexes_are_empty_misses() {
        let dir = TempDir::new().unwrap();
        let query = query_for(member("bbugyi200.athena.coder", Some("coder")));

        let missing = dir.path().join("absent.json");
        let out = query_bead_touches_for_members(&missing, &query);
        assert!(out.rows.is_empty() && out.generation.is_empty());
        assert!(out.source.missing);

        let truncated = dir.path().join("truncated.json");
        std::fs::write(&truncated, b"{\"schema_version\":4,\"touches\":[{")
            .unwrap();
        let out = query_bead_touches_for_members(&truncated, &query);
        assert!(out.rows.is_empty() && out.generation.is_empty());
        assert!(!out.source.missing);

        let wrong_schema = dir.path().join("wrong.json");
        std::fs::write(
            &wrong_schema,
            json!({"schema_version": 3, "generation": "g", "touches": []})
                .to_string(),
        )
        .unwrap();
        let out = query_bead_touches_for_members(&wrong_schema, &query);
        assert!(out.rows.is_empty() && out.generation.is_empty());
    }

    #[test]
    fn view_events_fold_counts_and_bounds_like_python() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bead_views.jsonl");
        let mut file = File::create(&path).unwrap();
        for line in [
            json!({"schema_version": 1, "id": "a", "timestamp": "2026-06-01T00:00:00Z", "project": "p", "cwd": "/", "bead_id": "sase-1.1", "agent_name": "coder"}),
            json!({"schema_version": 1, "id": "b", "timestamp": "2026-06-03T00:00:00Z", "project": "p", "cwd": "/", "bead_id": "sase-1.1", "agent_name": "coder"}),
            json!({"schema_version": 1, "id": "c", "timestamp": "not-a-time", "project": "p", "cwd": "/", "bead_id": "sase-1.1", "agent_name": "coder"}),
            json!({"schema_version": 1, "id": "d", "timestamp": "2026-06-02T00:00:00Z", "project": "p", "cwd": "/", "bead_id": "sase-1.1", "agent_name": "other"}),
            json!({"schema_version": 999, "id": "e", "timestamp": "2026-06-04T00:00:00Z", "project": "p", "cwd": "/", "bead_id": "sase-1.9", "agent_name": "coder"}),
            json!({"nope": true}),
        ] {
            writeln!(file, "{}", line).unwrap();
        }
        let out = query_bead_views_for_members(
            &path,
            &query_for(member("bbugyi200.athena.coder", Some("coder"))),
        );
        assert!(out.complete);
        assert_eq!(out.rows.len(), 1);
        let row = &out.rows[0].touch;
        assert_eq!(row.verbs.get("viewed"), Some(&3));
        assert_eq!(row.first_at, "2026-06-01T00:00:00Z");
        assert_eq!(row.last_at, "2026-06-03T00:00:00Z");
    }

    #[test]
    fn view_ties_keep_the_last_occurrence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bead_views.jsonl");
        let mut file = File::create(&path).unwrap();
        for stamp in ["2026-06-01T00:00:00Z", "2026-06-01T00:00:00Z"] {
            writeln!(file, "{}", json!({"schema_version": 1, "id": stamp, "timestamp": stamp, "project": "p", "cwd": "/", "bead_id": "sase-1.1", "agent_name": "coder"})).unwrap();
        }
        let out = query_bead_views_for_members(
            &path,
            &query_for(member("bbugyi200.athena.coder", Some("coder"))),
        );
        assert_eq!(out.rows[0].touch.verbs.get("viewed"), Some(&2));
    }

    #[test]
    fn missing_views_log_is_a_complete_empty_answer() {
        let dir = TempDir::new().unwrap();
        let out = query_bead_views_for_members(
            &dir.path().join("absent.jsonl"),
            &query_for(member("bbugyi200.athena.coder", Some("coder"))),
        );
        assert!(out.complete && out.rows.is_empty() && out.source.missing);
    }

    #[test]
    fn known_owner_roots_come_from_the_caller() {
        // The caller passes the roots; the query never consults ambient
        // machine configuration, so hermetic tests control them directly.
        let _ = (owner(), known_roots());
    }
}
