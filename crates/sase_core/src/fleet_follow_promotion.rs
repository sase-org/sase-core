//! Derive singleton-to-family follow promotions from followed-batch
//! observations.
//!
//! Existing follow reconciliation applies caller-supplied promotions. This
//! module is the shared derivation: active explicit singleton follows plus
//! followed-batch locators in, validated [`FollowFamilyPromotionWire`] values
//! out. TUI projection and store persistence stay in the caller. Unfollow
//! tombstones continue to win when the promotions are applied through
//! [`crate::fleet_contract::reconcile_follow_records`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::fleet_contract::{
    follow_record_key, logical_locator_key, validate_schema,
    FleetContractError, FollowCreatedByWire, FollowFamilyPromotionWire,
    FollowRecordWire, FollowStateWire, LogicalAgentLocatorWire,
    FLEET_CONTRACT_SCHEMA_VERSION,
};

/// Request to derive family promotions from active follows and observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowedBatchFamilyPromotionRequestWire {
    pub schema_version: u32,
    pub records: Vec<FollowRecordWire>,
    pub observations: Vec<LogicalAgentLocatorWire>,
}

/// Validated promotions ready for follow reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowedBatchFamilyPromotionResultWire {
    pub schema_version: u32,
    pub promotions: Vec<FollowFamilyPromotionWire>,
}

type PromotionIdentity = (String, String, String);

/// Derive singleton-to-family promotions for explicit active follows.
///
/// A promotion is emitted only when:
/// - the source record is an explicit, active singleton
/// - followed-batch observations contain exactly one family locator with the
///   same origin, project, and agent
/// - the source has not already been promoted in this request
pub fn followed_batch_family_promotions(
    request: &FollowedBatchFamilyPromotionRequestWire,
) -> Result<FollowedBatchFamilyPromotionResultWire, FleetContractError> {
    validate_schema(
        "followed-batch family promotion request",
        request.schema_version,
    )?;
    let family_locators = family_locators_by_identity(&request.observations)?;
    if family_locators.is_empty() {
        return Ok(empty_result());
    }

    let mut promotions = Vec::new();
    let mut promoted_sources = BTreeSet::new();
    for record in &request.records {
        follow_record_key(record)?;
        let Some(promotion) =
            promotion_for_record(record, &family_locators, &promoted_sources)?
        else {
            continue;
        };
        let source_key = logical_locator_key(&promotion.from)?;
        promotions.push(promotion);
        promoted_sources.insert(source_key);
    }
    Ok(FollowedBatchFamilyPromotionResultWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        promotions,
    })
}

fn empty_result() -> FollowedBatchFamilyPromotionResultWire {
    FollowedBatchFamilyPromotionResultWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        promotions: Vec::new(),
    }
}

fn promotion_for_record(
    record: &FollowRecordWire,
    family_locators: &BTreeMap<
        PromotionIdentity,
        BTreeMap<String, LogicalAgentLocatorWire>,
    >,
    promoted_sources: &BTreeSet<String>,
) -> Result<Option<FollowFamilyPromotionWire>, FleetContractError> {
    if record.created_by != FollowCreatedByWire::Explicit
        || record.state != FollowStateWire::Active
        || record.logical_locator.family_id.is_some()
    {
        return Ok(None);
    }
    let identity = promotion_identity(&record.logical_locator);
    let Some(matches) = family_locators.get(&identity) else {
        return Ok(None);
    };
    if matches.len() != 1 {
        return Ok(None);
    }
    let target = matches
        .values()
        .next()
        .expect("len == 1 family match")
        .clone();
    let source_key = logical_locator_key(&record.logical_locator)?;
    let target_key = logical_locator_key(&target)?;
    if promoted_sources.contains(&source_key) || source_key == target_key {
        return Ok(None);
    }
    Ok(Some(FollowFamilyPromotionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        from: record.logical_locator.clone(),
        to: target,
    }))
}

fn family_locators_by_identity(
    observations: &[LogicalAgentLocatorWire],
) -> Result<
    BTreeMap<PromotionIdentity, BTreeMap<String, LogicalAgentLocatorWire>>,
    FleetContractError,
> {
    let mut locators: BTreeMap<
        PromotionIdentity,
        BTreeMap<String, LogicalAgentLocatorWire>,
    > = BTreeMap::new();
    for locator in observations {
        locator.validate()?;
        if locator.family_id.is_none() {
            continue;
        }
        let key = logical_locator_key(locator)?;
        locators
            .entry(promotion_identity(locator))
            .or_default()
            .insert(key, locator.clone());
    }
    Ok(locators)
}

fn promotion_identity(locator: &LogicalAgentLocatorWire) -> PromotionIdentity {
    (
        locator.project.origin.installation_id.clone(),
        locator.project.project_id.clone(),
        locator.agent_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet_contract::{
        logical_key_unchecked, reconcile_follow_records, FollowCreatedByWire,
        FollowReconciliationRequestWire, FollowStateWire, FollowTombstoneWire,
        OriginLocatorWire, ProjectLocatorWire, ScopedOperationKeyWire,
        FLEET_INSTALLATION_ID_PREFIX,
    };

    fn id(hex: char) -> String {
        format!(
            "{FLEET_INSTALLATION_ID_PREFIX}{}",
            hex.to_string().repeat(64)
        )
    }

    fn origin(hex: char) -> OriginLocatorWire {
        OriginLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            installation_id: id(hex),
        }
    }

    fn logical(
        hex: char,
        agent: &str,
        family_id: Option<&str>,
    ) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: ProjectLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: origin(hex),
                project_id: "project-1".to_string(),
            },
            agent_id: agent.to_string(),
            family_id: family_id.map(str::to_string),
        }
    }

    fn follow_record(
        locator: LogicalAgentLocatorWire,
        created_by: FollowCreatedByWire,
        state: FollowStateWire,
        timestamp: f64,
    ) -> FollowRecordWire {
        let logical_key = logical_key_unchecked(&locator);
        FollowRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_locator: locator,
            logical_key,
            created_by,
            state,
            created_at_unix: timestamp,
            updated_at_unix: timestamp,
            activated_at_unix: match state {
                FollowStateWire::Active => Some(timestamp),
                FollowStateWire::Pending => None,
            },
            operation_key: match created_by {
                FollowCreatedByWire::Explicit => None,
                FollowCreatedByWire::Dispatch => Some(ScopedOperationKeyWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    controller_id: "controller-1".to_string(),
                    operation_id: "op-1".to_string(),
                }),
            },
        }
    }

    fn derive(
        records: Vec<FollowRecordWire>,
        observations: Vec<LogicalAgentLocatorWire>,
    ) -> FollowedBatchFamilyPromotionResultWire {
        followed_batch_family_promotions(
            &FollowedBatchFamilyPromotionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records,
                observations,
            },
        )
        .unwrap()
    }

    #[test]
    fn promotes_explicit_singleton_when_exactly_one_family_matches() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let result = derive(
            vec![follow_record(
                singleton.clone(),
                FollowCreatedByWire::Explicit,
                FollowStateWire::Active,
                10.0,
            )],
            vec![family.clone()],
        );
        assert_eq!(
            result.promotions,
            vec![FollowFamilyPromotionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                from: singleton,
                to: family,
            }]
        );
    }

    #[test]
    fn skips_when_observations_match_multiple_families() {
        let singleton = logical('a', "worker", None);
        let result = derive(
            vec![follow_record(
                singleton,
                FollowCreatedByWire::Explicit,
                FollowStateWire::Active,
                10.0,
            )],
            vec![
                logical('a', "worker", Some("family-1")),
                logical('a', "worker", Some("family-2")),
            ],
        );
        assert!(result.promotions.is_empty());
    }

    #[test]
    fn skips_family_records_and_dispatch_records() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let result = derive(
            vec![
                follow_record(
                    singleton,
                    FollowCreatedByWire::Dispatch,
                    FollowStateWire::Active,
                    10.0,
                ),
                follow_record(
                    family.clone(),
                    FollowCreatedByWire::Explicit,
                    FollowStateWire::Active,
                    10.0,
                ),
            ],
            vec![family],
        );
        assert!(result.promotions.is_empty());
    }

    #[test]
    fn skips_pending_explicit_singletons() {
        let singleton = logical('a', "worker", None);
        let result = derive(
            vec![follow_record(
                singleton,
                FollowCreatedByWire::Explicit,
                FollowStateWire::Pending,
                10.0,
            )],
            vec![logical('a', "worker", Some("family-1"))],
        );
        assert!(result.promotions.is_empty());
    }

    #[test]
    fn requires_same_origin_project_and_agent() {
        let singleton = logical('a', "worker", None);
        let record = follow_record(
            singleton,
            FollowCreatedByWire::Explicit,
            FollowStateWire::Active,
            10.0,
        );
        assert!(derive(
            vec![record.clone()],
            vec![logical('b', "worker", Some("family-1"))],
        )
        .promotions
        .is_empty());

        let mut other_project = logical('a', "worker", Some("family-1"));
        other_project.project.project_id = "other-project".to_string();
        assert!(derive(vec![record.clone()], vec![other_project])
            .promotions
            .is_empty());

        assert!(derive(
            vec![record],
            vec![logical('a', "other-agent", Some("family-1"))],
        )
        .promotions
        .is_empty());
    }

    #[test]
    fn duplicate_same_family_observations_are_not_ambiguous() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let result = derive(
            vec![follow_record(
                singleton.clone(),
                FollowCreatedByWire::Explicit,
                FollowStateWire::Active,
                10.0,
            )],
            vec![family.clone(), family.clone()],
        );
        assert_eq!(result.promotions.len(), 1);
        assert_eq!(result.promotions[0].from, singleton);
        assert_eq!(result.promotions[0].to, family);
    }

    #[test]
    fn skips_redundant_source_and_ignores_singleton_observations() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let record = follow_record(
            singleton.clone(),
            FollowCreatedByWire::Explicit,
            FollowStateWire::Active,
            10.0,
        );
        let result = derive(
            vec![record.clone(), record],
            vec![singleton, family.clone()],
        );
        assert_eq!(result.promotions.len(), 1);
        assert_eq!(result.promotions[0].to, family);
    }

    #[test]
    fn empty_observations_or_records_yield_no_promotions() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        assert!(derive(
            vec![follow_record(
                singleton.clone(),
                FollowCreatedByWire::Explicit,
                FollowStateWire::Active,
                10.0,
            )],
            vec![],
        )
        .promotions
        .is_empty());
        assert!(derive(vec![], vec![family]).promotions.is_empty());
    }

    #[test]
    fn non_tui_consumer_promotions_are_accepted_by_follow_reconciliation() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let record = follow_record(
            singleton.clone(),
            FollowCreatedByWire::Explicit,
            FollowStateWire::Active,
            10.0,
        );
        let derived = derive(vec![record.clone()], vec![family.clone()]);
        let reconciled =
            reconcile_follow_records(&FollowReconciliationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![record],
                tombstones: Vec::new(),
                promotions: derived.promotions.clone(),
                activations: Vec::new(),
                now_unix: 12.0,
            })
            .unwrap();
        assert!(reconciled.changed);
        assert_eq!(reconciled.records.len(), 1);
        assert_eq!(reconciled.records[0].logical_locator, family);
        assert_eq!(
            reconciled.records[0].logical_key,
            logical_key_unchecked(&family)
        );
        assert_eq!(
            reconciled.records[0].created_by,
            FollowCreatedByWire::Explicit
        );
    }

    #[test]
    fn unfollow_tombstones_win_when_derived_promotions_are_reconciled() {
        let singleton = logical('a', "worker", None);
        let family = logical('a', "worker", Some("family-1"));
        let record = follow_record(
            singleton.clone(),
            FollowCreatedByWire::Dispatch,
            FollowStateWire::Pending,
            20.0,
        );
        let derived = derive(
            vec![follow_record(
                singleton.clone(),
                FollowCreatedByWire::Explicit,
                FollowStateWire::Active,
                10.0,
            )],
            vec![family.clone()],
        );
        assert_eq!(derived.promotions.len(), 1);

        let resurrected =
            reconcile_follow_records(&FollowReconciliationRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![record],
                tombstones: vec![FollowTombstoneWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    logical_locator: singleton.clone(),
                    logical_key: logical_key_unchecked(&singleton),
                    unfollowed_at_unix: 21.0,
                }],
                promotions: derived.promotions,
                activations: Vec::new(),
                now_unix: 22.0,
            })
            .unwrap();
        assert!(resurrected.records.is_empty());
        assert!(resurrected.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "follow_promotion_source_tombstoned"
        }));
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let error = followed_batch_family_promotions(
            &FollowedBatchFamilyPromotionRequestWire {
                schema_version: 9,
                records: Vec::new(),
                observations: Vec::new(),
            },
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("schema_version 9 is not supported"));
    }

    #[test]
    fn rejects_malformed_records_and_observations() {
        let mut bad_record = follow_record(
            logical('a', "worker", None),
            FollowCreatedByWire::Explicit,
            FollowStateWire::Active,
            10.0,
        );
        bad_record.logical_locator.agent_id.clear();
        let record_error = followed_batch_family_promotions(
            &FollowedBatchFamilyPromotionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![bad_record],
                observations: vec![logical('a', "worker", Some("family-1"))],
            },
        )
        .unwrap_err();
        assert!(record_error.to_string().contains("agent_id"));

        let mut bad_observation = logical('a', "worker", Some("family-1"));
        bad_observation.agent_id.clear();
        let observation_error = followed_batch_family_promotions(
            &FollowedBatchFamilyPromotionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                records: vec![follow_record(
                    logical('a', "worker", None),
                    FollowCreatedByWire::Explicit,
                    FollowStateWire::Active,
                    10.0,
                )],
                observations: vec![bad_observation],
            },
        )
        .unwrap_err();
        assert!(observation_error.to_string().contains("agent_id"));
    }
}
