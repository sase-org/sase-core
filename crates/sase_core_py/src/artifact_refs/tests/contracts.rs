use super::*;
use crate::artifact_links::{
    py_artifact_link_alias_producer_id, py_artifact_link_canonicalize,
    py_artifact_link_cutover_attestation,
    py_artifact_link_cutover_baseline_event,
    py_artifact_link_cutover_import_identity,
    py_artifact_link_cutover_marker_build,
    py_artifact_link_cutover_marker_canonical_json,
    py_artifact_link_cutover_marker_parse, py_artifact_link_cutover_progress,
    py_artifact_link_cutover_read_state,
    py_artifact_link_cutover_wire_schema_version,
    py_artifact_link_derived_producer_id,
    py_artifact_link_event_canonical_json, py_artifact_link_event_canonicalize,
    py_artifact_link_event_digest, py_artifact_link_event_owner_requirements,
    py_artifact_link_event_path_for_digest,
    py_artifact_link_event_resolve_aliases,
    py_artifact_link_event_schema_version,
    py_artifact_link_event_validate_bytes,
    py_artifact_link_event_validate_path, py_artifact_link_events_reduce,
    py_artifact_link_frontmatter_inlet, py_artifact_link_machine_run_id,
    py_artifact_link_merge_indexes, py_artifact_link_publication_due,
    py_artifact_link_publication_mark_attempt,
    py_artifact_link_publication_ownership_wire_schema_version,
    py_artifact_link_publication_receipt,
    py_artifact_link_publication_record_key,
    py_artifact_link_publication_register_pending,
    py_artifact_link_publication_state_wire_schema_version,
    py_artifact_link_ref_parts, py_artifact_link_row_schema_version,
    py_artifact_link_stable_fact_created_at,
    py_artifact_link_stable_operation_id, py_artifact_relation_label,
    py_artifact_relations_builtins, py_artifact_row_index_keys,
    py_artifact_row_ref_lookup_keys,
    py_artifact_row_resolution_wire_schema_version, py_artifact_row_resolve,
    py_links_block_strip, py_links_block_upsert,
};
use crate::editor_content::{
    py_markdown_link_refs_wire_schema_version,
    py_markdown_reference_definitions_append,
    py_markdown_reference_label_allocate, py_markdown_reference_links_scan,
};
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn artifact_ref_contract_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "artifact_ref_kind_catalog",
            "artifact_ref_kind_canonicalize",
            "artifact_ref_parse_canonical",
            "artifact_ref_quote_argument",
            "artifact_ref_expansion_placeholders",
            "artifact_ref_expansion_validate",
            "artifact_ref_expansion_render",
            "artifact_ref_provider_spec_validate",
            "artifact_ref_provider_spec_digest",
            "artifact_ref_provider_spec_wire_schema_version",
            "artifact_ref_entry_validate",
            "artifact_ref_entry_wire_schema_version",
            "artifact_ref_use_manifest_parse",
            "artifact_ref_use_record_render",
            "artifact_ref_use_wire_schema_version",
            "markdown_link_refs_wire_schema_version",
            "markdown_reference_links_scan",
            "markdown_reference_label_allocate",
            "markdown_reference_definitions_append",
            "referenced_by_wire_schema_version",
            "referenced_by_block_parse",
            "referenced_by_block_render",
            "referenced_by_block_upsert",
            "referenced_by_block_remove",
            "referenced_by_block_strip",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let catalog = py_artifact_ref_kind_catalog(py).unwrap();
        let catalog = py_to_json_value(catalog.bind(py)).unwrap();
        assert!(catalog
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["kind"] == json!("stitch")
                && entry["reserved"] == json!(true)));

        let commit_alias =
            py_artifact_ref_kind_canonicalize(py, "commit").unwrap();
        let commit_alias = py_to_json_value(commit_alias.bind(py)).unwrap();
        assert_eq!(commit_alias["canonical"], json!("stitch"));

        let canonical = py_artifact_ref_parse_canonical(
            py,
            "commit:sase@0123456789abcdef0123456789abcdef01234567",
        )
        .unwrap();
        let canonical = py_to_json_value(canonical.bind(py)).unwrap();
        assert_eq!(
            canonical["reference"]["rendered"],
            json!("stitch:sase@0123456789abcdef0123456789abcdef01234567")
        );

        assert_eq!(
            py_artifact_ref_quote_argument("has space"),
            "\"has space\""
        );
        assert_eq!(py_artifact_ref_quote_argument("plain"), "plain");

        assert!(py_artifact_ref_expansion_placeholders()
            .contains(&"kind".to_string()));
        assert_eq!(
            py_artifact_ref_expansion_validate("{kind}:{argument}").unwrap(),
            ["kind", "argument"]
        );
        assert_eq!(
            py_artifact_ref_expansion_render(
                "{kind}:{argument}",
                BTreeMap::from([
                    ("kind".to_string(), "stitch".to_string()),
                    ("argument".to_string(), "abc1234".to_string()),
                ]),
            )
            .unwrap(),
            "stitch:abc1234"
        );

        let spec_value = json!({
            "schema_version": 1,
            "provider": "research",
            "ref": {
                "kind": "research",
                "icon": "∴",
                "expansion_format": "{kind}:{argument}",
                "properties": {},
                "detail": {"fields": []},
                "identity": {},
                "inventory": {},
                "publication": {
                    "link": "vcs_permalink",
                    "referenced_by": "markdown_table"
                }
            }
        });
        let spec_object = json_value_to_py(py, &spec_value).unwrap();
        let spec = spec_object.bind(py).downcast::<PyDict>().unwrap();
        py_artifact_ref_provider_spec_validate(spec).unwrap();
        let digest = py_artifact_ref_provider_spec_digest(spec).unwrap();
        assert_eq!(digest.len(), 64);
        assert_eq!(py_artifact_ref_provider_spec_wire_schema_version(), 1);

        let entry_value = json!({
            "schema_version": 1,
            "stable_id": "research:notes/x.md",
            "ref_kind": "research",
            "canonical_argument": "notes/x.md",
            "display_label": "notes/x.md",
            "properties": {},
            "origin": "prompt_ref"
        });
        let entry_object = json_value_to_py(py, &entry_value).unwrap();
        let entry = entry_object.bind(py).downcast::<PyDict>().unwrap();
        py_artifact_ref_entry_validate(entry).unwrap();
        assert_eq!(py_artifact_ref_entry_wire_schema_version(), 1);

        let use_record_value = json!({
            "schema_version": 1,
            "recorded_at": "2026-08-01T14:22:03Z",
            "agent_name": "bbugyi200.athena.sase-js.1",
            "raw_ref": "bead:sase-js.1",
            "canonical_ref": "bead:sase-js.1",
            "ref_kind": "bead",
            "prompt_text": "@bead:sase-js.1"
        });
        let use_record_object =
            json_value_to_py(py, &use_record_value).unwrap();
        let use_record =
            use_record_object.bind(py).downcast::<PyDict>().unwrap();
        let rendered_use =
            py_artifact_ref_use_record_render(use_record).unwrap();
        let parsed_manifest = py_artifact_ref_use_manifest_parse(
            py,
            &PyBytes::new_bound(py, rendered_use.as_bytes()),
        )
        .unwrap();
        let parsed_manifest =
            py_to_json_value(parsed_manifest.bind(py)).unwrap();
        assert_eq!(parsed_manifest[0]["raw_ref"], json!("bead:sase-js.1"));
        assert_eq!(py_artifact_ref_use_wire_schema_version(), 1);

        assert_eq!(py_markdown_link_refs_wire_schema_version(), 1);
        let scan =
            py_markdown_reference_links_scan(py, "[1]: https://one\n").unwrap();
        let scan_value = py_to_json_value(scan.bind(py)).unwrap();
        assert_eq!(scan_value["definitions"][0]["label"], json!("1"));
        let scan_dict = scan.bind(py).downcast::<PyDict>().unwrap();
        let allocated = py_markdown_reference_label_allocate(
            scan_dict,
            "https://two",
            BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(allocated, "2");
        let definitions = json_value_to_py(
            py,
            &json!([{"label": "2", "destination": "https://two"}]),
        )
        .unwrap();
        let definitions = definitions.bind(py).downcast::<PyList>().unwrap();
        let appended = py_markdown_reference_definitions_append(
            "[1]: https://one\n",
            definitions,
        )
        .unwrap();
        assert!(appended.contains("[2]: https://two"));

        assert_eq!(py_referenced_by_wire_schema_version(), 1);
        let table_value = json!({
            "schema_version": 1,
            "columns": [{"key": "agent", "label": "Agent", "numeric": false}],
            "rows": [{"values": {"agent": "alpha"}, "link_targets": {}}],
            "omitted": 0
        });
        let table_object = json_value_to_py(py, &table_value).unwrap();
        let table = table_object.bind(py).downcast::<PyDict>().unwrap();
        let rendered_block = py_referenced_by_block_render(table).unwrap();
        assert!(rendered_block.contains("## Referenced By"));
        let upserted = py_referenced_by_block_upsert("Body\n", table).unwrap();
        assert!(upserted.contains("<!-- sase:referenced-by:start -->"));
        let parsed_block = py_referenced_by_block_parse(py, &upserted).unwrap();
        let parsed_block = py_to_json_value(parsed_block.bind(py)).unwrap();
        assert_eq!(
            parsed_block["table"]["rows"][0]["values"]["agent"],
            json!("alpha")
        );
        assert_eq!(py_referenced_by_block_remove(&upserted), "Body\n");
        assert_eq!(py_referenced_by_block_strip(&upserted), "Body");

        for name in [
            "artifact_link_row_schema_version",
            "artifact_link_event_schema_version",
            "artifact_link_cutover_wire_schema_version",
            "artifact_link_cutover_marker_parse",
            "artifact_link_cutover_marker_canonical_json",
            "artifact_link_cutover_marker_build",
            "artifact_link_cutover_import_identity",
            "artifact_link_cutover_baseline_event",
            "artifact_link_cutover_attestation",
            "artifact_link_cutover_read_state",
            "artifact_link_cutover_progress",
            "artifact_link_outbox_classify_line",
            "artifact_link_outbox_legacy_conversion",
            "artifact_link_derived_producer_id",
            "artifact_link_alias_producer_id",
            "artifact_link_machine_run_id",
            "artifact_link_stable_fact_created_at",
            "artifact_link_stable_operation_id",
            "artifact_link_event_canonicalize",
            "artifact_link_event_canonical_json",
            "artifact_link_event_digest",
            "artifact_link_event_path_for_digest",
            "artifact_link_event_validate_path",
            "artifact_link_event_validate_bytes",
            "artifact_link_event_resolve_aliases",
            "artifact_link_events_reduce",
            "artifact_row_resolution_wire_schema_version",
            "artifact_link_publication_state_wire_schema_version",
            "artifact_link_publication_ownership_wire_schema_version",
            "artifact_link_event_owner_requirements",
            "artifact_link_publication_receipt",
            "artifact_link_publication_record_key",
            "artifact_link_publication_register_pending",
            "artifact_link_publication_due",
            "artifact_link_publication_mark_attempt",
            "artifact_link_ref_parts",
            "artifact_row_index_keys",
            "artifact_row_ref_lookup_keys",
            "artifact_row_resolve",
            "artifact_link_canonicalize",
            "artifact_link_validate_row",
            "artifact_link_upsert_row",
            "artifact_link_merge_indexes",
            "artifact_relations_builtins",
            "artifact_relation_lookup",
            "artifact_relation_label",
            "links_block_parse",
            "links_block_render",
            "links_block_upsert",
            "links_block_remove",
            "links_block_strip",
            "artifact_md_path",
            "companion_md_path",
            "artifact_link_frontmatter_inlet",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_artifact_link_row_schema_version(), 2);
        assert_eq!(py_artifact_link_event_schema_version(), 1);
        assert_eq!(py_artifact_link_cutover_wire_schema_version(), 1);
        assert_eq!(
            py_artifact_link_derived_producer_id(),
            "sase.artifact-link-derived"
        );
        assert_eq!(
            py_artifact_link_alias_producer_id(),
            "sase.artifact-link-renames"
        );
        assert_eq!(py_artifact_link_machine_run_id(), "machine");
        assert_eq!(
            py_artifact_link_stable_fact_created_at(),
            "1970-01-01T00:00:00Z"
        );
        let stable_parts =
            json_value_to_py(py, &json!(["derived", {"b": 2, "a": 1}]))
                .unwrap();
        let stable_id =
            py_artifact_link_stable_operation_id(stable_parts.bind(py))
                .unwrap();
        assert_eq!(stable_id.len(), 32);
        let cutover_request_value = json!({
            "project_key": "gh_acme__widget",
            "roles": [{
                "role": "plans",
                "kind": "plan",
                "head": "abc123",
                "links_tree": "sha256:abc123",
                "remote_url": "<none>",
                "commit_time": "2026-09-10T00:00:00Z"
            }],
            "rows": [{
                "schema_version": 2,
                "source_ref": "agent:reader",
                "relation": "read",
                "target_ref": "plan:202609/old.md",
                "description": "read the artifact",
                "origin": "read",
                "created_by": "agent:reader",
                "created_at": "2026-09-09T12:00:00Z",
                "uses": 1
            }]
        });
        let cutover_request_object =
            json_value_to_py(py, &cutover_request_value).unwrap();
        let cutover_request = cutover_request_object
            .bind(py)
            .downcast::<PyDict>()
            .unwrap();
        let import_identity =
            py_artifact_link_cutover_import_identity(py, cutover_request)
                .unwrap();
        let import_identity_value =
            py_to_json_value(import_identity.bind(py)).unwrap();
        assert!(import_identity_value["import_id"]
            .as_str()
            .unwrap()
            .starts_with("legacy-v2-links-"));
        let baseline_request_value = json!({
            "project_key": "gh_acme__widget",
            "import": import_identity_value,
            "rows": cutover_request_value["rows"]
        });
        let baseline_request_object =
            json_value_to_py(py, &baseline_request_value).unwrap();
        let baseline_event = py_artifact_link_cutover_baseline_event(
            py,
            baseline_request_object
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap();
        let baseline_event_value =
            py_to_json_value(baseline_event.bind(py)).unwrap();
        let baseline_event_object =
            json_value_to_py(py, &baseline_event_value).unwrap();
        let baseline_digest = py_artifact_link_event_digest(
            baseline_event_object.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let baseline_path =
            py_artifact_link_event_path_for_digest(&baseline_digest).unwrap();
        let event_store_value = json!({
            "schema_version": 1,
            "minimum_event_schema_version": 1
        });
        let event_store_object =
            json_value_to_py(py, &event_store_value).unwrap();
        let marker_roles_value = json!([{
            "role": "plans",
            "kind": "plan",
            "head": "abc123",
            "links_tree": "sha256:abc123",
            "remote_url": "<none>"
        }]);
        let marker_roles_object =
            json_value_to_py(py, &marker_roles_value).unwrap();
        let baseline_identity_value = json!({
            "digest": baseline_digest,
            "path": baseline_path
        });
        let baseline_identity_object =
            json_value_to_py(py, &baseline_identity_value).unwrap();
        let marker = py_artifact_link_cutover_marker_build(
            py,
            "fenced",
            "gh_acme__widget",
            event_store_object.bind(py).downcast::<PyDict>().unwrap(),
            import_identity.bind(py).downcast::<PyDict>().unwrap(),
            marker_roles_object.bind(py).downcast::<PyList>().unwrap(),
            baseline_identity_object
                .bind(py)
                .downcast::<PyDict>()
                .unwrap(),
        )
        .unwrap();
        let marker_dict = marker.bind(py).downcast::<PyDict>().unwrap();
        let marker_json =
            py_artifact_link_cutover_marker_canonical_json(marker_dict)
                .unwrap();
        let parsed =
            py_artifact_link_cutover_marker_parse(py, &marker_json).unwrap();
        assert_eq!(
            py_to_json_value(parsed.bind(py)).unwrap(),
            py_to_json_value(marker.bind(py)).unwrap()
        );
        let attestation =
            py_artifact_link_cutover_attestation(marker_dict).unwrap();
        assert!(attestation.starts_with("fleet-capable-"));
        let read_state_object = json_value_to_py(
                py,
                &json!([{"role": "plans", "marker": py_to_json_value(marker.bind(py)).unwrap()}]),
            )
            .unwrap();
        let read_state = py_artifact_link_cutover_read_state(
            py,
            read_state_object.bind(py).downcast::<PyList>().unwrap(),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(read_state.bind(py)).unwrap()["state"],
            json!("fenced")
        );
        let progress_object = json_value_to_py(
            py,
            &json!({
                "expected": py_to_json_value(marker.bind(py)).unwrap(),
                "roots": [{
                    "role": "plans",
                    "marker": py_to_json_value(marker.bind(py)).unwrap(),
                    "marker_committed": true,
                    "baseline_durable": false
                }]
            }),
        )
        .unwrap();
        let progress = py_artifact_link_cutover_progress(
            py,
            progress_object.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(progress.bind(py)).unwrap()["phase"],
            json!("publish_baseline")
        );
        assert_eq!(py_artifact_row_resolution_wire_schema_version(), 1);
        assert_eq!(py_artifact_link_publication_state_wire_schema_version(), 1);
        assert_eq!(
            py_artifact_link_publication_ownership_wire_schema_version(),
            1
        );
        let publication_key = py_artifact_link_publication_record_key(
            "gh_acme__widget",
            "plans",
            "/tmp/plans",
            "git@example.com:acme/widget--plans.git",
            "origin/main",
        )
        .unwrap();
        assert!(publication_key.starts_with("artifact-link-publication:v1:"));
        let observation_value = json!({
            "version": 1,
            "project_key": "gh_acme__widget",
            "role": "plans",
            "repo_root": "/tmp/plans",
            "remote_url": "git@example.com:acme/widget--plans.git",
            "upstream": "origin/main",
            "head_revision": "abc123",
            "oldest_unpublished_at": 1_000.0
        });
        let observation_object =
            json_value_to_py(py, &observation_value).unwrap();
        let observation =
            observation_object.bind(py).downcast::<PyDict>().unwrap();
        let record = py_artifact_link_publication_register_pending(
            py,
            observation,
            1_200.0,
            None,
        )
        .unwrap();
        let record_value = py_to_json_value(record.bind(py)).unwrap();
        assert_eq!(record_value["key"], json!(publication_key));
        assert_eq!(record_value["first_pending_at"], json!(1_000.0));
        assert_eq!(record_value["next_due_at"], json!(3_600.0));
        let record_dict = record.bind(py).downcast::<PyDict>().unwrap();
        let due =
            py_artifact_link_publication_due(py, record_dict, 3_600.0).unwrap();
        let due_value = py_to_json_value(due.bind(py)).unwrap();
        assert_eq!(due_value["due"], json!(true));
        let attempt_value = json!({
            "status": "failed",
            "error": "network",
            "log_path": "/tmp/sase-sync.log"
        });
        let attempt_object = json_value_to_py(py, &attempt_value).unwrap();
        let attempt = attempt_object.bind(py).downcast::<PyDict>().unwrap();
        let failed_record = py_artifact_link_publication_mark_attempt(
            py,
            record_dict,
            attempt,
            3_600.0,
        )
        .unwrap();
        let failed_value = py_to_json_value(failed_record.bind(py)).unwrap();
        assert_eq!(failed_value["attempt_count"], json!(1));
        assert_eq!(failed_value["next_due_at"], json!(7_200.0));

        let event_value = json!({
            "schema_version": 1,
            "project_key": "gh_acme__widget",
            "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "created_by": "agent:reader",
            "origin": "read",
            "created_at": "2026-09-09T12:00:00Z",
            "kind": {
                "type": "observation",
                "edge": {
                    "kind": "directed",
                    "source_ref": "agent:reader",
                    "relation": "read",
                    "target_ref": "plan:202609/old.md"
                },
                "description": "read the artifact",
                "occurrences": 2
            }
        });
        let event_object = json_value_to_py(py, &event_value).unwrap();
        let event = event_object.bind(py).downcast::<PyDict>().unwrap();
        let canonical_event =
            py_artifact_link_event_canonicalize(py, event).unwrap();
        let canonical_event_value =
            py_to_json_value(canonical_event.bind(py)).unwrap();
        assert_eq!(
            canonical_event_value["kind"]["edge"]["target_ref"],
            json!("plan:202609/old.md")
        );
        let canonical_json =
            py_artifact_link_event_canonical_json(event).unwrap();
        assert!(canonical_json.ends_with('\n'));
        let digest = py_artifact_link_event_digest(event).unwrap();
        assert_eq!(digest.len(), 64);
        let path = py_artifact_link_event_path_for_digest(&digest).unwrap();
        assert_eq!(
            py_artifact_link_event_validate_path(&path, &digest).unwrap(),
            path
        );
        let bytes = PyBytes::new_bound(py, canonical_json.as_bytes());
        let canonical_from_bytes = py_artifact_link_event_validate_bytes(
            py,
            bytes.as_any(),
            Some(&path),
        )
        .unwrap();
        let canonical_from_bytes =
            py_to_json_value(canonical_from_bytes.bind(py)).unwrap();
        assert_eq!(canonical_from_bytes["digest"], json!(digest));
        let document_kinds_object =
            json_value_to_py(py, &json!(["plan"])).unwrap();
        let document_kinds =
            document_kinds_object.bind(py).downcast::<PyList>().unwrap();
        let requirements = py_artifact_link_event_owner_requirements(
            py,
            event,
            document_kinds,
        )
        .unwrap();
        let requirements_bound = requirements.bind(py);
        let requirements_value = py_to_json_value(requirements_bound).unwrap();
        assert_eq!(
            requirements_value["document_refs"][0]["reference"],
            json!("plan:202609/old.md")
        );
        let pending_evidence_object = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_roots": {},
                "forced_roots": [],
                "durable_roots": [],
                "bead_owner": false,
                "bead_receipt": false,
                "local_receipt": false
            }),
        )
        .unwrap();
        let pending_evidence = pending_evidence_object
            .bind(py)
            .downcast::<PyDict>()
            .unwrap();
        let requirements_dict =
            requirements_bound.downcast::<PyDict>().unwrap();
        let pending_receipt = py_artifact_link_publication_receipt(
            py,
            requirements_dict,
            pending_evidence,
        )
        .unwrap();
        let pending_receipt =
            py_to_json_value(pending_receipt.bind(py)).unwrap();
        assert_eq!(pending_receipt["acknowledged"], json!(false));
        assert!(pending_receipt["pending_reasons"][0]
            .as_str()
            .unwrap()
            .contains("plan:202609/old.md"));
        let acknowledged_evidence_object = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "resolved_roots": {"plan": "/tmp/plans"},
                "forced_roots": [],
                "durable_roots": ["/tmp/plans"],
                "bead_owner": false,
                "bead_receipt": false,
                "local_receipt": false
            }),
        )
        .unwrap();
        let acknowledged_evidence = acknowledged_evidence_object
            .bind(py)
            .downcast::<PyDict>()
            .unwrap();
        let acknowledged_receipt = py_artifact_link_publication_receipt(
            py,
            requirements_dict,
            acknowledged_evidence,
        )
        .unwrap();
        let acknowledged_receipt =
            py_to_json_value(acknowledged_receipt.bind(py)).unwrap();
        assert_eq!(acknowledged_receipt["acknowledged"], json!(true));

        let put_value = json!({
            "schema_version": 1,
            "project_key": "gh_acme__widget",
            "operation_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "created_by": "agent:writer",
            "origin": "manual",
            "created_at": "2026-09-09T12:01:00Z",
            "kind": {
                "type": "edge-put",
                "edge": {
                    "kind": "directed",
                    "source_ref": "agent:reader",
                    "relation": "read",
                    "target_ref": "plan:202609/old.md"
                },
                "description": "updated description",
                "observed_operation_ids": [
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ]
            }
        });
        let alias_value = json!([{
            "old_ref": "plan:202609/old.md",
            "new_ref": "plan:202609/new.md"
        }]);
        let events_object =
            json_value_to_py(py, &json!([event_value, put_value])).unwrap();
        let events = events_object.bind(py).downcast::<PyList>().unwrap();
        let aliases_object = json_value_to_py(py, &alias_value).unwrap();
        let aliases = aliases_object.bind(py).downcast::<PyList>().unwrap();
        let refs_object =
            json_value_to_py(py, &json!(["plan:202609/old.md"])).unwrap();
        let refs = refs_object.bind(py).downcast::<PyList>().unwrap();
        let resolved =
            py_artifact_link_event_resolve_aliases(py, aliases, refs).unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(
            resolved["resolved_refs"]["plan:202609/old.md"],
            json!("plan:202609/new.md")
        );
        let reduction =
            py_artifact_link_events_reduce(py, events, Some(aliases)).unwrap();
        let reduction = py_to_json_value(reduction.bind(py)).unwrap();
        assert_eq!(
            reduction["rows"][0]["target_ref"],
            json!("plan:202609/new.md")
        );
        assert_eq!(
            reduction["rows"][0]["description"],
            json!("updated description")
        );
        assert_eq!(reduction["rows"][0]["uses"], json!(2));
        let ref_parts =
            py_artifact_link_ref_parts(py, "@plans:202609/a.md#section")
                .unwrap()
                .unwrap();
        let ref_parts = py_to_json_value(ref_parts.bind(py)).unwrap();
        assert_eq!(ref_parts["schema_version"], json!(1));
        assert_eq!(ref_parts["kind"], json!("plan"));
        assert_eq!(ref_parts["payload"], json!("202609/a.md"));

        let identities_value = json!([
            {"pane_id": "patches", "parts": ["alpha", "same"]},
            {"pane_id": "patches", "parts": ["beta", "same"]},
            {"pane_id": "files", "parts": ["doc", "v1"]}
        ]);
        let identities_object =
            json_value_to_py(py, &identities_value).unwrap();
        let identities =
            identities_object.bind(py).downcast::<PyList>().unwrap();
        let index_keys = py_artifact_row_index_keys(py, identities).unwrap();
        let index_keys = py_to_json_value(index_keys.bind(py)).unwrap();
        assert!(index_keys[2]
            .as_array()
            .unwrap()
            .contains(&json!(["files.id", "doc"])));

        let row_query_value = json!({
            "schema_version": 1,
            "kind": "patch",
            "payload": "same",
            "project_hint": "beta"
        });
        let row_query_object = json_value_to_py(py, &row_query_value).unwrap();
        let row_query = row_query_object.bind(py).downcast::<PyDict>().unwrap();
        let lookup_keys =
            py_artifact_row_ref_lookup_keys(py, row_query).unwrap();
        let lookup_keys = py_to_json_value(lookup_keys.bind(py)).unwrap();
        assert_eq!(
            lookup_keys[0],
            json!(["patches.project.name", "beta", "same"])
        );
        let resolved = py_artifact_row_resolve(py, row_query, identities)
            .unwrap()
            .unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(resolved["pane_id"], json!("patches"));
        assert_eq!(resolved["parts"], json!(["beta", "same"]));
        assert_eq!(
            py_artifact_link_canonicalize("plans:202608/report.md").unwrap(),
            "plan:202608/report.md"
        );
        let base_index_value = json!({
            "schema_version": 2,
            "artifact_ref": "plan:202609/a.md",
            "rows": []
        });
        let local_index_value = json!({
            "schema_version": 2,
            "artifact_ref": "plans:202609/a.md",
            "rows": [{
                "schema_version": 2,
                "source_ref": "agent:local",
                "relation": "cites",
                "target_ref": "plan:202609/a.md",
                "description": "local citation",
                "origin": "manual",
                "created_by": "agent:local",
                "created_at": "2026-09-03T00:00:00Z",
                "uses": 1
            }]
        });
        let upstream_index_value = json!({
            "schema_version": 2,
            "artifact_ref": "plan:202609/a.md",
            "rows": [{
                "schema_version": 2,
                "source_ref": "agent:upstream",
                "relation": "cites",
                "target_ref": "plan:202609/a.md",
                "description": "upstream citation",
                "origin": "manual",
                "created_by": "agent:upstream",
                "created_at": "2026-09-02T00:00:00Z",
                "uses": 1
            }]
        });
        let base_object = json_value_to_py(py, &base_index_value).unwrap();
        let local_object = json_value_to_py(py, &local_index_value).unwrap();
        let upstream_object =
            json_value_to_py(py, &upstream_index_value).unwrap();
        let merged = py_artifact_link_merge_indexes(
            py,
            base_object.bind(py).downcast::<PyDict>().unwrap(),
            local_object.bind(py).downcast::<PyDict>().unwrap(),
            upstream_object.bind(py).downcast::<PyDict>().unwrap(),
        )
        .unwrap();
        let merged = py_to_json_value(merged.bind(py)).unwrap();
        assert_eq!(merged["artifact_ref"], json!("plan:202609/a.md"));
        assert_eq!(merged["rows"].as_array().unwrap().len(), 2);
        assert_eq!(merged["rows"][0]["source_ref"], json!("agent:upstream"));
        let relations = py_artifact_relations_builtins(py).unwrap();
        let relations = py_to_json_value(relations.bind(py)).unwrap();
        assert!(relations
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["slug"] == json!("related")));
        assert_eq!(
            py_artifact_relation_label("implements", false).unwrap(),
            "implemented-by"
        );
        let links_table_value = json!({
            "schema_version": 1,
            "columns": [
                {"key": "relation", "label": "Relation", "numeric": false},
                {"key": "artifact", "label": "Artifact", "numeric": false},
                {"key": "why", "label": "Why", "numeric": false}
            ],
            "rows": [{
                "values": {
                    "relation": "related",
                    "artifact": "bead:sase-ct",
                    "why": "why"
                },
                "link_targets": {}
            }],
            "omitted": 0
        });
        let links_table_object =
            json_value_to_py(py, &links_table_value).unwrap();
        let links_table =
            links_table_object.bind(py).downcast::<PyDict>().unwrap();
        let links_upserted =
            py_links_block_upsert("# Doc\n", links_table).unwrap();
        assert!(links_upserted.contains("<!-- sase:links:start -->"));
        assert!(links_upserted.contains("## Links"));
        assert_eq!(py_links_block_strip(&links_upserted), "# Doc");
        let inlet = py_artifact_link_frontmatter_inlet(
            py,
            "---\nlinks:\n  - Label: path.md\n---\n# Body\n",
        )
        .unwrap();
        let inlet = py_to_json_value(inlet.bind(py)).unwrap();
        assert_eq!(inlet["kind"], json!("unrecognized"));
    });
}
