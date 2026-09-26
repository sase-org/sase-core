//! Receipt policy, redacted proof snapshots, and path diffs.
//!
//! Pure helpers for the schema-1 ToolRun receipt contract. SQL lives in
//! `store::receipt`; this module never touches SQLite.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::canonical::canonical_digest;
use super::wire::ToolFingerprintWire;
use super::ToolRunError;

pub const RECEIPT_POLICY_VERSION: u32 = 1;
pub const RECEIPT_MAX_CHANGED_PATHS: usize = 32;
pub const RECEIPT_MAX_TTL_SECONDS: i64 = 7200;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofDirtyWire {
    pub path: String,
    pub status: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofRepoWire {
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_tree: Option<String>,
    #[serde(default)]
    pub dirty: Vec<ReceiptProofDirtyWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofInputMatchWire {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofInputWire {
    pub pattern: String,
    #[serde(default)]
    pub matches: Vec<ReceiptProofInputMatchWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofToolchainWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptProofWire {
    #[serde(default)]
    pub repos: Vec<ReceiptProofRepoWire>,
    #[serde(default)]
    pub inputs: Vec<ReceiptProofInputWire>,
    #[serde(default)]
    pub toolchain: BTreeMap<String, ReceiptProofToolchainWire>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

pub fn receipt_id_for_run(run_id: &str) -> Result<String, ToolRunError> {
    canonical_digest(&run_id).map_err(ToolRunError::invalid)
}

pub fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn path_component_rejected(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return true;
    }
    if path.contains('\0') || path.contains('\\') {
        return true;
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return true;
        }
    }
    false
}

pub fn is_safe_relative_path(path: &str) -> bool {
    !path_component_rejected(path)
}

pub fn build_receipt_proof(
    fingerprint: &ToolFingerprintWire,
) -> Result<ReceiptProofWire, ToolRunError> {
    let mut repos = Vec::new();
    for repo in &fingerprint.repos {
        if repo.identity.trim().is_empty() {
            return Err(ToolRunError::invalid(
                "fingerprint repo identity must not be empty",
            ));
        }
        let mut dirty = Vec::new();
        for entry in &repo.dirty_paths {
            if !is_safe_relative_path(&entry.path) {
                continue;
            }
            dirty.push(ReceiptProofDirtyWire {
                path: entry.path.clone(),
                status: entry.status.clone(),
                kind: entry.kind.clone(),
                content_hash: entry.content_hash.clone(),
            });
        }
        dirty.sort_by(|left, right| left.path.cmp(&right.path));
        repos.push(ReceiptProofRepoWire {
            identity: repo.identity.clone(),
            head: repo.head.clone(),
            index_tree: repo.index_tree.clone(),
            dirty,
        });
    }
    repos.sort_by(|left, right| left.identity.cmp(&right.identity));
    let mut inputs = Vec::new();
    for input in &fingerprint.inputs {
        let mut matches = Vec::new();
        for entry in &input.matches {
            if !is_safe_relative_path(&entry.path) {
                continue;
            }
            matches.push(ReceiptProofInputMatchWire {
                path: entry.path.clone(),
                content_hash: entry.content_hash.clone(),
            });
        }
        matches.sort_by(|left, right| left.path.cmp(&right.path));
        inputs.push(ReceiptProofInputWire {
            pattern: input.pattern.clone(),
            matches,
        });
    }
    inputs.sort_by(|left, right| left.pattern.cmp(&right.pattern));
    let mut toolchain = BTreeMap::new();
    for (name, probe) in &fingerprint.toolchain {
        if name.trim().is_empty() {
            continue;
        }
        toolchain.insert(
            name.clone(),
            ReceiptProofToolchainWire {
                output: probe.output.clone(),
                exit_code: probe.exit_code,
            },
        );
    }
    let mut env = BTreeMap::new();
    for (key, value) in &fingerprint.env {
        let Some(value) = value else {
            continue;
        };
        env.insert(key.clone(), sha256_hex(value));
    }
    Ok(ReceiptProofWire {
        repos,
        inputs,
        toolchain,
        env,
    })
}

pub fn proof_to_json(proof: &ReceiptProofWire) -> Result<String, ToolRunError> {
    serde_json::to_string(proof)
        .map_err(|error| ToolRunError::store(error.to_string()))
}

pub fn proof_from_json(raw: &str) -> Result<ReceiptProofWire, ToolRunError> {
    serde_json::from_str(raw)
        .map_err(|error| ToolRunError::store(error.to_string()))
}

pub fn diff_proof_against_fingerprint(
    proof: &ReceiptProofWire,
    current: &ToolFingerprintWire,
) -> Vec<String> {
    let mut out = BTreeSet::new();
    let current_proof =
        build_receipt_proof(current).unwrap_or(ReceiptProofWire {
            repos: Vec::new(),
            inputs: Vec::new(),
            toolchain: BTreeMap::new(),
            env: BTreeMap::new(),
        });
    let mut proof_repos: BTreeMap<&str, &ReceiptProofRepoWire> =
        BTreeMap::new();
    for repo in &proof.repos {
        proof_repos.insert(repo.identity.as_str(), repo);
    }
    let mut current_repos: BTreeMap<&str, &ReceiptProofRepoWire> =
        BTreeMap::new();
    for repo in &current_proof.repos {
        current_repos.insert(repo.identity.as_str(), repo);
    }
    let mut identities = BTreeSet::new();
    for identity in proof_repos.keys() {
        identities.insert(*identity);
    }
    for identity in current_repos.keys() {
        identities.insert(*identity);
    }
    for identity in identities {
        let left = proof_repos.get(identity);
        let right = current_repos.get(identity);
        match (left, right) {
            (Some(left), Some(right)) => {
                if left.head != right.head {
                    let candidate = format!("head:{identity}");
                    if is_safe_relative_path(&candidate)
                        || candidate.starts_with("head:")
                    {
                        out.insert(candidate);
                    }
                }
                if left.index_tree != right.index_tree {
                    out.insert(format!("index:{identity}"));
                }
                let mut left_dirty: BTreeMap<&str, &ReceiptProofDirtyWire> =
                    BTreeMap::new();
                for entry in &left.dirty {
                    left_dirty.insert(entry.path.as_str(), entry);
                }
                let mut right_dirty: BTreeMap<&str, &ReceiptProofDirtyWire> =
                    BTreeMap::new();
                for entry in &right.dirty {
                    right_dirty.insert(entry.path.as_str(), entry);
                }
                let mut paths = BTreeSet::new();
                for path in left_dirty.keys() {
                    paths.insert(*path);
                }
                for path in right_dirty.keys() {
                    paths.insert(*path);
                }
                for path in paths {
                    let changed =
                        match (left_dirty.get(path), right_dirty.get(path)) {
                            (Some(left), Some(right)) => {
                                left.status != right.status
                                    || left.content_hash != right.content_hash
                            }
                            (Some(_), None) | (None, Some(_)) => true,
                            (None, None) => false,
                        };
                    if changed && is_safe_relative_path(path) {
                        out.insert(path.to_string());
                    }
                }
            }
            (Some(left), None) => {
                for entry in &left.dirty {
                    if is_safe_relative_path(&entry.path) {
                        out.insert(entry.path.clone());
                    }
                }
                if left.head.is_some() {
                    out.insert(format!("head:{identity}"));
                }
                if left.index_tree.is_some() {
                    out.insert(format!("index:{identity}"));
                }
            }
            (None, Some(right)) => {
                for entry in &right.dirty {
                    if is_safe_relative_path(&entry.path) {
                        out.insert(entry.path.clone());
                    }
                }
                if right.head.is_some() {
                    out.insert(format!("head:{identity}"));
                }
                if right.index_tree.is_some() {
                    out.insert(format!("index:{identity}"));
                }
            }
            (None, None) => {}
        }
    }
    let mut proof_inputs: BTreeMap<&str, BTreeMap<&str, Option<String>>> =
        BTreeMap::new();
    for input in &proof.inputs {
        let mut matches = BTreeMap::new();
        for entry in &input.matches {
            matches.insert(entry.path.as_str(), entry.content_hash.clone());
        }
        proof_inputs.insert(input.pattern.as_str(), matches);
    }
    let mut current_inputs: BTreeMap<&str, BTreeMap<&str, Option<String>>> =
        BTreeMap::new();
    for input in &current_proof.inputs {
        let mut matches = BTreeMap::new();
        for entry in &input.matches {
            matches.insert(entry.path.as_str(), entry.content_hash.clone());
        }
        current_inputs.insert(input.pattern.as_str(), matches);
    }
    let mut patterns = BTreeSet::new();
    for pattern in proof_inputs.keys() {
        patterns.insert(*pattern);
    }
    for pattern in current_inputs.keys() {
        patterns.insert(*pattern);
    }
    for pattern in patterns {
        let left = proof_inputs.get(pattern);
        let right = current_inputs.get(pattern);
        let mut paths = BTreeSet::new();
        if let Some(map) = left {
            for path in map.keys() {
                paths.insert(*path);
            }
        }
        if let Some(map) = right {
            for path in map.keys() {
                paths.insert(*path);
            }
        }
        for path in paths {
            let left_hash = left.and_then(|map| map.get(path));
            let right_hash = right.and_then(|map| map.get(path));
            if left_hash != right_hash && is_safe_relative_path(path) {
                out.insert(path.to_string());
            }
        }
    }
    let mut probes = BTreeSet::new();
    for name in proof.toolchain.keys() {
        probes.insert(name.as_str());
    }
    for name in current_proof.toolchain.keys() {
        probes.insert(name.as_str());
    }
    for name in probes {
        let left = proof.toolchain.get(name);
        let right = current_proof.toolchain.get(name);
        if left != right {
            out.insert(format!("toolchain:{name}"));
        }
    }
    let mut keys = BTreeSet::new();
    for key in proof.env.keys() {
        keys.insert(key.as_str());
    }
    for key in current_proof.env.keys() {
        keys.insert(key.as_str());
    }
    for key in keys {
        if proof.env.get(key) != current_proof.env.get(key) {
            out.insert(format!("env:{key}"));
        }
    }
    let mut sorted: Vec<String> = out.into_iter().collect();
    sorted.sort();
    sorted.dedup();
    sorted
        .into_iter()
        .filter(|path| {
            if path.starts_with("head:")
                || path.starts_with("index:")
                || path.starts_with("toolchain:")
                || path.starts_with("env:")
            {
                return true;
            }
            is_safe_relative_path(path)
        })
        .collect()
}
