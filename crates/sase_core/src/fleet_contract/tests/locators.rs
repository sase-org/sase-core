use super::super::*;
use super::support::*;

#[test]
fn locator_keys_are_stable_and_names_do_not_become_identity() {
    let left = logical('a', "worker");
    let right = logical('b', "worker");
    assert_ne!(
        logical_locator_key(&left).unwrap(),
        logical_locator_key(&right).unwrap()
    );
    let renamed = LogicalAgentLocatorWire {
        agent_id: "worker".to_string(),
        ..left.clone()
    };
    assert_eq!(
        logical_locator_key(&left).unwrap(),
        logical_locator_key(&renamed).unwrap()
    );
    let display = associate_owner_display_name(&OwnerDisplayNameRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: left.clone(),
        owner_username: "bryan".to_string(),
        owner_machine_name: "athena".to_string(),
        display_name: "athena.worker".to_string(),
        display_alias: Some("worker".to_string()),
    })
    .unwrap();
    assert_eq!(display.owner_label, "bryan.athena");
    assert_eq!(display.logical_key, logical_key_unchecked(&left));
    assert!(LogicalAgentLocatorWire {
        agent_id: "bad\nid".to_string(),
        ..left
    }
    .validate()
    .is_err());
}
