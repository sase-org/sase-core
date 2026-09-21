use super::super::*;
use serde_json::json;

#[tokio::test]
async fn cache_persists_and_ignores_newer_schema() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("fleet").join("worker_cache.json");
    let mut cache = FederationCache::load(path.clone(), 2, 1024);
    cache.store("host-a", "summary:{}", json!({"ok": true}));
    cache.persist(&path).unwrap();
    let mut loaded = FederationCache::load(path.clone(), 2, 1024);
    assert_eq!(
        loaded.get("host-a", "summary:{}").unwrap().payload["ok"],
        json!(true)
    );
    fs::write(
        &path,
        br#"{"schema_version":999,"entries":[{"host_id":"host-a"}]}"#,
    )
    .unwrap();
    let mut empty = FederationCache::load(path, 2, 1024);
    assert!(empty.get("host-a", "summary:{}").is_none());
}

#[test]
fn attention_read_operation_cache_key_is_namespaced_and_stable() {
    let request = FleetLogicalBatchRequestWire {
        schema_version: 1,
        logical_keys: vec!["logical:alpha".to_string()],
    };
    let operation = ReadOperation::Attention(request.clone());
    assert_eq!(operation.name(), "attention");
    let key = operation.cache_key().unwrap();
    assert!(key.starts_with("attention:"));
    assert_eq!(key, ReadOperation::Attention(request).cache_key().unwrap());
}

#[test]
fn attention_inventory_read_operation_cache_key_is_namespaced_and_stable() {
    let request = FleetAttentionInventoryRequestWire {
        schema_version: 1,
        cursor: Some("off:50".to_string()),
        limit: Some(50),
    };
    let operation = ReadOperation::AttentionInventory(request.clone());
    assert_eq!(operation.name(), "attention_inventory");
    let key = operation.cache_key().unwrap();
    assert!(key.starts_with("attention_inventory:"));
    assert_eq!(
        key,
        ReadOperation::AttentionInventory(request)
            .cache_key()
            .unwrap()
    );
}
