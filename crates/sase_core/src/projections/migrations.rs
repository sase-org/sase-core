use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::error::ProjectionError;

pub const PROJECTION_SCHEMA_VERSION: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationWire {
    pub version: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMigrationWire {
    pub version: u32,
    pub name: String,
    pub applied_at: String,
}

struct Migration {
    version: u32,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "projection_event_log_base",
        sql: r#"
        CREATE TABLE IF NOT EXISTS event_log (
            seq INTEGER PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            source_json TEXT NOT NULL,
            host_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            idempotency_key TEXT,
            causality_json TEXT NOT NULL DEFAULT '[]',
            source_path TEXT,
            source_revision TEXT
        );

        CREATE TABLE IF NOT EXISTS event_idempotency (
            idempotency_key TEXT PRIMARY KEY,
            seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS projection_meta (
            projection TEXT PRIMARY KEY,
            last_seq INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_event_log_project_seq
            ON event_log(project_id, seq);
        CREATE INDEX IF NOT EXISTS idx_event_log_type_seq
            ON event_log(event_type, seq);
        CREATE INDEX IF NOT EXISTS idx_event_log_source_path
            ON event_log(source_path);

        CREATE TRIGGER IF NOT EXISTS projection_meta_last_seq_insert_guard
        BEFORE INSERT ON projection_meta
        WHEN NEW.last_seq > COALESCE((SELECT MAX(seq) FROM event_log), 0)
        BEGIN
            SELECT RAISE(ABORT, 'projection_meta last_seq cannot advance beyond event_log');
        END;

        CREATE TRIGGER IF NOT EXISTS projection_meta_last_seq_update_guard
        BEFORE UPDATE OF last_seq ON projection_meta
        WHEN NEW.last_seq > COALESCE((SELECT MAX(seq) FROM event_log), 0)
        BEGIN
            SELECT RAISE(ABORT, 'projection_meta last_seq cannot advance beyond event_log');
        END;
    "#,
    },
    Migration {
        version: 2,
        name: "changespec_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS changespecs (
            handle TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            project_id TEXT NOT NULL,
            name TEXT NOT NULL,
            project_basename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            source_path TEXT NOT NULL,
            is_archive INTEGER NOT NULL DEFAULT 0,
            source_start_line INTEGER NOT NULL,
            source_end_line INTEGER NOT NULL,
            status TEXT NOT NULL,
            parent TEXT,
            cl_or_pr TEXT,
            bug TEXT,
            description TEXT NOT NULL,
            searchable_text TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_changespecs_project_status_updated
            ON changespecs(project_id, status, updated_at DESC, handle);
        CREATE INDEX IF NOT EXISTS idx_changespecs_project_source
            ON changespecs(project_id, source_path);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_changespecs_project_name
            ON changespecs(project_id, name);

        CREATE TABLE IF NOT EXISTS changespec_edges (
            source_handle TEXT NOT NULL REFERENCES changespecs(handle) ON DELETE CASCADE,
            edge_type TEXT NOT NULL,
            target_handle TEXT NOT NULL,
            target_name TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (source_handle, edge_type, target_name)
        );

        CREATE INDEX IF NOT EXISTS idx_changespec_edges_target
            ON changespec_edges(target_handle, edge_type);

        CREATE TABLE IF NOT EXISTS changespec_sections (
            handle TEXT NOT NULL REFERENCES changespecs(handle) ON DELETE CASCADE,
            section_name TEXT NOT NULL,
            section_index INTEGER NOT NULL,
            body_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (handle, section_name, section_index)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS changespec_search_fts USING fts5(
            handle UNINDEXED,
            project_id UNINDEXED,
            name,
            status,
            content
        );

        CREATE TABLE IF NOT EXISTS changespec_projection_errors (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            error_kind TEXT NOT NULL,
            message TEXT NOT NULL,
            line INTEGER,
            column INTEGER,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_changespec_projection_errors_project
            ON changespec_projection_errors(project_id, source_path, seq);
    "#,
    },
    Migration {
        version: 3,
        name: "notification_pending_action_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS notifications (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            host_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            sender TEXT NOT NULL,
            notes_json TEXT NOT NULL,
            files_json TEXT NOT NULL,
            action TEXT,
            action_data_json TEXT NOT NULL,
            read INTEGER NOT NULL,
            dismissed INTEGER NOT NULL,
            silent INTEGER NOT NULL,
            muted INTEGER NOT NULL,
            snooze_until TEXT,
            notification_json TEXT NOT NULL,
            source_order INTEGER NOT NULL,
            updated_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_notifications_project_timestamp
            ON notifications(project_id, timestamp DESC, id);
        CREATE INDEX IF NOT EXISTS idx_notifications_state
            ON notifications(read, dismissed, silent, muted);
        CREATE INDEX IF NOT EXISTS idx_notifications_action
            ON notifications(action);

        CREATE TABLE IF NOT EXISTS notification_pending_actions (
            prefix TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            host_id TEXT NOT NULL,
            notification_id TEXT NOT NULL,
            action_kind TEXT NOT NULL,
            action TEXT NOT NULL,
            action_data_json TEXT NOT NULL,
            files_json TEXT NOT NULL,
            created_at_unix REAL NOT NULL,
            updated_at_unix REAL NOT NULL,
            stale_deadline_unix REAL NOT NULL,
            transports_json TEXT NOT NULL,
            state TEXT NOT NULL,
            pending_action_json TEXT NOT NULL,
            updated_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_notification_pending_actions_notification
            ON notification_pending_actions(notification_id);
        CREATE INDEX IF NOT EXISTS idx_notification_pending_actions_state
            ON notification_pending_actions(state);
        CREATE INDEX IF NOT EXISTS idx_notification_pending_actions_stale
            ON notification_pending_actions(stale_deadline_unix);

        CREATE VIRTUAL TABLE IF NOT EXISTS notification_search_fts
            USING fts5(notification_id UNINDEXED, content);
    "#,
    },
    Migration {
        version: 4,
        name: "bead_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS beads (
            bead_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            issue_json TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            tier TEXT,
            parent_id TEXT,
            owner TEXT NOT NULL,
            assignee TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            close_reason TEXT,
            description TEXT NOT NULL,
            notes TEXT NOT NULL,
            design TEXT NOT NULL,
            model TEXT NOT NULL,
            is_ready_to_work INTEGER NOT NULL DEFAULT 0,
            epic_count INTEGER,
            changespec_name TEXT NOT NULL,
            changespec_bug_id TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, bead_id)
        );

        CREATE INDEX IF NOT EXISTS idx_beads_project_status_created
            ON beads(project_id, status, created_at, bead_id);
        CREATE INDEX IF NOT EXISTS idx_beads_project_parent_created
            ON beads(project_id, parent_id, created_at, bead_id);
        CREATE INDEX IF NOT EXISTS idx_beads_project_type_tier
            ON beads(project_id, issue_type, tier, bead_id);
        CREATE INDEX IF NOT EXISTS idx_beads_project_updated
            ON beads(project_id, updated_at DESC, bead_id);

        CREATE TABLE IF NOT EXISTS bead_dependencies (
            project_id TEXT NOT NULL,
            issue_id TEXT NOT NULL,
            depends_on_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, issue_id, depends_on_id)
        );

        CREATE INDEX IF NOT EXISTS idx_bead_dependencies_depends_on
            ON bead_dependencies(project_id, depends_on_id, issue_id);

        CREATE TABLE IF NOT EXISTS bead_events (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            bead_id TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bead_events_project_bead_seq
            ON bead_events(project_id, bead_id, seq);
        CREATE INDEX IF NOT EXISTS idx_bead_events_project_type_seq
            ON bead_events(project_id, event_type, seq);
    "#,
    },
    Migration {
        version: 5,
        name: "agent_artifact_archive_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS agents (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            project_name TEXT NOT NULL,
            project_dir TEXT NOT NULL,
            project_file TEXT NOT NULL,
            workflow_dir_name TEXT NOT NULL,
            artifact_dir TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            agent_type TEXT NOT NULL,
            cl_name TEXT,
            agent_name TEXT,
            model TEXT,
            llm_provider TEXT,
            started_at TEXT,
            finished_at REAL,
            hidden INTEGER NOT NULL DEFAULT 0,
            has_done_marker INTEGER NOT NULL DEFAULT 0,
            has_running_marker INTEGER NOT NULL DEFAULT 0,
            has_waiting_marker INTEGER NOT NULL DEFAULT 0,
            has_workflow_state INTEGER NOT NULL DEFAULT 0,
            record_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, agent_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_project_artifact_dir
            ON agents(project_id, artifact_dir);
        CREATE INDEX IF NOT EXISTS idx_agents_project_active
            ON agents(project_id, hidden, has_done_marker, has_workflow_state, status, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_agents_project_recent
            ON agents(project_id, hidden, has_done_marker, finished_at DESC, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_agents_project_cl
            ON agents(project_id, cl_name, timestamp DESC);

        CREATE TABLE IF NOT EXISTS agent_attempts (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            retry_of_timestamp TEXT,
            retried_as_timestamp TEXT,
            retry_chain_root_timestamp TEXT,
            retry_attempt INTEGER,
            terminal INTEGER NOT NULL DEFAULT 0,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, agent_id, attempt_id)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_attempts_retry_root
            ON agent_attempts(project_id, retry_chain_root_timestamp, retry_attempt);

        CREATE TABLE IF NOT EXISTS agent_edges (
            project_id TEXT NOT NULL,
            parent_agent_id TEXT NOT NULL,
            child_agent_id TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            parent_timestamp TEXT,
            child_timestamp TEXT,
            workflow_name TEXT,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, parent_agent_id, child_agent_id, edge_type)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_edges_child
            ON agent_edges(project_id, child_agent_id, edge_type);

        CREATE TABLE IF NOT EXISTS agent_artifacts (
            project_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_kind TEXT NOT NULL,
            display_name TEXT,
            role TEXT,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, agent_id, artifact_path, artifact_kind)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_artifacts_agent
            ON agent_artifacts(project_id, agent_id, artifact_kind, artifact_path);

        CREATE TABLE IF NOT EXISTS agent_archive (
            project_id TEXT NOT NULL,
            bundle_path TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            raw_suffix TEXT NOT NULL,
            cl_name TEXT NOT NULL,
            agent_name TEXT,
            status TEXT NOT NULL,
            start_time TEXT,
            dismissed_at TEXT,
            revived_at TEXT,
            project_name TEXT,
            model TEXT,
            runtime TEXT,
            llm_provider TEXT,
            step_index INTEGER,
            step_name TEXT,
            step_type TEXT,
            retry_attempt INTEGER NOT NULL DEFAULT 0,
            is_workflow_child INTEGER NOT NULL DEFAULT 0,
            archive_json TEXT NOT NULL,
            bundle_json TEXT NOT NULL,
            purged INTEGER NOT NULL DEFAULT 0,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, bundle_path)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_archive_project_dismissed
            ON agent_archive(project_id, purged, dismissed_at DESC, raw_suffix DESC);
        CREATE INDEX IF NOT EXISTS idx_agent_archive_project_cl
            ON agent_archive(project_id, cl_name, raw_suffix DESC);

        CREATE TABLE IF NOT EXISTS agent_dismissed_identities (
            project_id TEXT NOT NULL,
            agent_type TEXT NOT NULL,
            cl_name TEXT NOT NULL,
            raw_suffix TEXT NOT NULL,
            agent_id TEXT,
            dismissed_name TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            changed_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, agent_type, cl_name, raw_suffix)
        );

        CREATE INDEX IF NOT EXISTS idx_agent_dismissed_active
            ON agent_dismissed_identities(project_id, active, cl_name, raw_suffix);

        CREATE VIRTUAL TABLE IF NOT EXISTS agent_search_fts USING fts5(
            agent_id UNINDEXED,
            project_id UNINDEXED,
            content
        );
    "#,
    },
    Migration {
        version: 6,
        name: "workflow_catalog_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS workflows (
            project_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            workflow_name TEXT NOT NULL,
            status TEXT NOT NULL,
            cl_name TEXT,
            project_name TEXT,
            project_dir TEXT,
            project_file TEXT,
            workflow_dir_name TEXT,
            artifact_dir TEXT,
            agent_id TEXT,
            parent_workflow_id TEXT,
            parent_agent_id TEXT,
            current_step_index INTEGER NOT NULL DEFAULT 0,
            pid INTEGER,
            appears_as_agent INTEGER NOT NULL DEFAULT 0,
            is_anonymous INTEGER NOT NULL DEFAULT 0,
            started_at TEXT,
            finished_at TEXT,
            error TEXT,
            traceback TEXT,
            activity TEXT,
            retry_of_workflow_id TEXT,
            retried_as_workflow_id TEXT,
            retry_attempt INTEGER,
            state_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, workflow_id)
        );

        CREATE INDEX IF NOT EXISTS idx_workflows_project_status_started
            ON workflows(project_id, status, started_at, workflow_id);
        CREATE INDEX IF NOT EXISTS idx_workflows_project_parent
            ON workflows(project_id, parent_workflow_id, workflow_id);
        CREATE INDEX IF NOT EXISTS idx_workflows_project_agent
            ON workflows(project_id, agent_id);

        CREATE TABLE IF NOT EXISTS workflow_steps (
            project_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            step_type TEXT,
            step_source TEXT,
            parent_step_index INTEGER,
            total_steps INTEGER,
            parent_total_steps INTEGER,
            hidden INTEGER NOT NULL DEFAULT 0,
            is_pre_prompt_step INTEGER NOT NULL DEFAULT 0,
            embedded_workflow_name TEXT,
            artifacts_dir TEXT,
            output_json TEXT NOT NULL,
            output_types_json TEXT NOT NULL,
            error TEXT,
            traceback TEXT,
            marker_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, workflow_id, step_index)
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_steps_status
            ON workflow_steps(project_id, status, workflow_id, step_index);

        CREATE TABLE IF NOT EXISTS workflow_events (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            workflow_id TEXT,
            step_index INTEGER,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_seq
            ON workflow_events(project_id, workflow_id, seq);
        CREATE INDEX IF NOT EXISTS idx_workflow_events_type_seq
            ON workflow_events(project_id, event_type, seq);

        CREATE TABLE IF NOT EXISTS xprompt_catalog (
            project_id TEXT NOT NULL,
            catalog_id TEXT NOT NULL,
            name TEXT NOT NULL,
            source_bucket TEXT NOT NULL,
            project TEXT,
            kind TEXT,
            entry_json TEXT NOT NULL,
            xprompt_json TEXT NOT NULL,
            source_path TEXT,
            definition_path TEXT,
            content_preview TEXT,
            is_skill INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT,
            is_present INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, catalog_id)
        );

        CREATE INDEX IF NOT EXISTS idx_xprompt_catalog_lookup
            ON xprompt_catalog(project_id, is_present, name, source_bucket);
        CREATE INDEX IF NOT EXISTS idx_xprompt_catalog_source
            ON xprompt_catalog(project_id, source_path);

        CREATE TABLE IF NOT EXISTS config_catalog (
            project_id TEXT NOT NULL,
            config_id TEXT NOT NULL,
            path TEXT NOT NULL,
            scope TEXT NOT NULL,
            content_preview TEXT,
            metadata_json TEXT NOT NULL,
            config_json TEXT NOT NULL,
            is_present INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, config_id)
        );

        CREATE INDEX IF NOT EXISTS idx_config_catalog_lookup
            ON config_catalog(project_id, is_present, scope, path);

        CREATE TABLE IF NOT EXISTS memory_catalog (
            project_id TEXT NOT NULL,
            memory_id TEXT NOT NULL,
            path TEXT NOT NULL,
            tier TEXT NOT NULL,
            source_name TEXT NOT NULL,
            keywords_json TEXT NOT NULL,
            content_preview TEXT,
            metadata_json TEXT NOT NULL,
            memory_json TEXT NOT NULL,
            is_present INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, memory_id)
        );

        CREATE INDEX IF NOT EXISTS idx_memory_catalog_lookup
            ON memory_catalog(project_id, is_present, tier, path);

        CREATE TABLE IF NOT EXISTS file_history (
            project_id TEXT NOT NULL,
            path TEXT NOT NULL,
            branch_or_workspace TEXT NOT NULL DEFAULT '',
            workspace_name TEXT,
            last_used_at TEXT,
            use_count INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL,
            file_json TEXT NOT NULL,
            is_present INTEGER NOT NULL DEFAULT 1,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, path, branch_or_workspace)
        );

        CREATE INDEX IF NOT EXISTS idx_file_history_recent
            ON file_history(project_id, is_present, branch_or_workspace, last_used_at DESC, path);

        CREATE TABLE IF NOT EXISTS catalog_events (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_catalog_events_type_seq
            ON catalog_events(project_id, event_type, seq);
    "#,
    },
    Migration {
        version: 7,
        name: "source_export_outbox",
        sql: r#"
        CREATE TABLE IF NOT EXISTS source_export_outbox (
            export_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            target_path TEXT NOT NULL,
            export_kind TEXT NOT NULL,
            expected_fingerprint_json TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            plan_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            repair_context_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            applied_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_source_export_outbox_status
            ON source_export_outbox(status, event_seq, export_id);
        CREATE INDEX IF NOT EXISTS idx_source_export_outbox_event
            ON source_export_outbox(event_seq, export_id);
        CREATE INDEX IF NOT EXISTS idx_source_export_outbox_target
            ON source_export_outbox(target_path, status);

        CREATE TABLE IF NOT EXISTS source_export_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_id INTEGER NOT NULL REFERENCES source_export_outbox(export_id) ON DELETE CASCADE,
            attempted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,
            message TEXT,
            actual_fingerprint_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_source_export_attempts_export
            ON source_export_attempts(export_id, attempt_id);
    "#,
    },
    Migration {
        version: 8,
        name: "agent_lifecycle_projection",
        sql: r#"
        ALTER TABLE agents ADD COLUMN batch_id TEXT;
        ALTER TABLE agents ADD COLUMN queue_id TEXT;
        ALTER TABLE agents ADD COLUMN parent_agent_id TEXT;
        ALTER TABLE agents ADD COLUMN workflow_id TEXT;
        ALTER TABLE agents ADD COLUMN retry_of_agent_id TEXT;
        ALTER TABLE agents ADD COLUMN resume_of_agent_id TEXT;
        ALTER TABLE agents ADD COLUMN host_id TEXT;
        ALTER TABLE agents ADD COLUMN pid INTEGER;
        ALTER TABLE agents ADD COLUMN workspace_claim_id TEXT;
        ALTER TABLE agents ADD COLUMN last_heartbeat_at TEXT;
        ALTER TABLE agents ADD COLUMN last_check_at TEXT;
        ALTER TABLE agents ADD COLUMN lifecycle_changed_at TEXT;
        ALTER TABLE agents ADD COLUMN stale_reason TEXT;

        ALTER TABLE agent_attempts ADD COLUMN batch_id TEXT;
        ALTER TABLE agent_attempts ADD COLUMN queue_id TEXT;
        ALTER TABLE agent_attempts ADD COLUMN host_id TEXT;
        ALTER TABLE agent_attempts ADD COLUMN pid INTEGER;
        ALTER TABLE agent_attempts ADD COLUMN artifact_dir TEXT;
        ALTER TABLE agent_attempts ADD COLUMN workspace_claim_id TEXT;
        ALTER TABLE agent_attempts ADD COLUMN created_at TEXT;
        ALTER TABLE agent_attempts ADD COLUMN last_heartbeat_at TEXT;
        ALTER TABLE agent_attempts ADD COLUMN last_check_at TEXT;

        ALTER TABLE agent_edges ADD COLUMN workflow_id TEXT;

        CREATE TABLE IF NOT EXISTS workflow_agent_edges (
            project_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            parent_agent_id TEXT,
            parent_workflow_id TEXT,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, workflow_id, agent_id, edge_type)
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_agent_edges_agent
            ON workflow_agent_edges(project_id, agent_id, workflow_id);

        CREATE TABLE IF NOT EXISTS agent_lifecycle_events (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            agent_id TEXT,
            lifecycle_status TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_agent_lifecycle_events_agent_seq
            ON agent_lifecycle_events(project_id, agent_id, seq);
        CREATE INDEX IF NOT EXISTS idx_agents_lifecycle_project_status
            ON agents(project_id, status, hidden, timestamp DESC, agent_id);
        CREATE INDEX IF NOT EXISTS idx_agents_lifecycle_batch
            ON agents(project_id, batch_id, queue_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_agents_lifecycle_workflow
            ON agents(project_id, workflow_id, timestamp DESC);
    "#,
    },
    Migration {
        version: 9,
        name: "scheduler_queue_projection",
        sql: r#"
        CREATE TABLE IF NOT EXISTS scheduler_batches (
            project_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            queue_id TEXT NOT NULL,
            slot_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            batch_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, batch_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduler_batches_idempotency
            ON scheduler_batches(project_id, idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_scheduler_batches_queue_created
            ON scheduler_batches(project_id, queue_id, created_at, batch_id);

        CREATE TABLE IF NOT EXISTS scheduler_slots (
            project_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            slot_id TEXT NOT NULL,
            queue_id TEXT NOT NULL,
            slot_index INTEGER NOT NULL,
            status TEXT NOT NULL,
            queued_position INTEGER,
            terminal INTEGER NOT NULL DEFAULT 0,
            launch_spec_json TEXT NOT NULL,
            slot_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, slot_id)
        );

        CREATE INDEX IF NOT EXISTS idx_scheduler_slots_batch
            ON scheduler_slots(project_id, batch_id, slot_index, slot_id);
        CREATE INDEX IF NOT EXISTS idx_scheduler_slots_queue_position
            ON scheduler_slots(project_id, queue_id, terminal, queued_position, slot_index);
        CREATE INDEX IF NOT EXISTS idx_scheduler_slots_status
            ON scheduler_slots(project_id, status, terminal, updated_at);

        CREATE TABLE IF NOT EXISTS scheduler_events (
            seq INTEGER PRIMARY KEY REFERENCES event_log(seq) ON DELETE CASCADE,
            project_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            batch_id TEXT,
            slot_id TEXT,
            status TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_scheduler_events_batch_seq
            ON scheduler_events(project_id, batch_id, seq);
        CREATE INDEX IF NOT EXISTS idx_scheduler_events_slot_seq
            ON scheduler_events(project_id, slot_id, seq);
        CREATE INDEX IF NOT EXISTS idx_scheduler_events_type_seq
            ON scheduler_events(project_id, event_type, seq);
    "#,
    },
    Migration {
        version: 10,
        name: "workflow_scheduler_task_projection",
        sql: r#"
        ALTER TABLE workflow_steps ADD COLUMN step_id TEXT;
        ALTER TABLE workflow_steps ADD COLUMN scheduler_task_json TEXT NOT NULL DEFAULT 'null';
        ALTER TABLE workflow_steps ADD COLUMN scheduler_cause_json TEXT NOT NULL DEFAULT 'null';
        CREATE INDEX IF NOT EXISTS idx_workflow_steps_step_id
            ON workflow_steps(project_id, step_id);

        CREATE TABLE IF NOT EXISTS workflow_tasks (
            project_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            task_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            step_id TEXT,
            step_index INTEGER,
            started_at TEXT,
            completed_at TEXT,
            log_summary TEXT,
            cause_json TEXT NOT NULL,
            task_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL REFERENCES event_log(seq) ON DELETE CASCADE,
            PRIMARY KEY (project_id, task_id)
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_tasks_workflow
            ON workflow_tasks(project_id, workflow_id, task_kind, task_id);
        CREATE INDEX IF NOT EXISTS idx_workflow_tasks_status
            ON workflow_tasks(project_id, status, task_kind, task_id);

        ALTER TABLE workflow_events ADD COLUMN step_id TEXT;
        ALTER TABLE workflow_events ADD COLUMN task_id TEXT;
    "#,
    },
];

pub fn run_migrations(conn: &Connection) -> Result<(), ProjectionError> {
    conn.execute_batch(
        r#"
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )?;

    let result = (|| {
        for migration in MIGRATIONS {
            let applied_name: Option<String> = conn
                .query_row(
                    "SELECT name FROM schema_migrations WHERE version = ?1",
                    [migration.version],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(applied_name) = applied_name {
                if applied_name != migration.name {
                    return Err(ProjectionError::MigrationNameMismatch {
                        version: migration.version,
                        applied_name,
                        expected_name: migration.name.to_string(),
                    });
                }
                continue;
            }
            conn.execute_batch(migration.sql)?;
            conn.execute(
                "INSERT INTO schema_migrations(version, name) VALUES (?1, ?2)",
                params![migration.version, migration.name],
            )?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(error) => match conn.execute_batch("ROLLBACK;") {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(ProjectionError::Rollback {
                source: Box::new(error),
                rollback_error,
            }),
        },
    }
}

pub fn applied_migrations(
    conn: &Connection,
) -> Result<Vec<AppliedMigrationWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT version, name, applied_at FROM schema_migrations ORDER BY version",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(AppliedMigrationWire {
            version: row.get(0)?,
            name: row.get(1)?,
            applied_at: row.get(2)?,
        })
    })?;
    let mut migrations = Vec::new();
    for row in rows {
        migrations.push(row?);
    }
    Ok(migrations)
}

pub fn known_migrations() -> Vec<MigrationWire> {
    MIGRATIONS
        .iter()
        .map(|migration| MigrationWire {
            version: migration.version,
            name: migration.name.to_string(),
        })
        .collect()
}

pub(crate) fn recreate_projection_tables(
    conn: &Connection,
) -> Result<Vec<MigrationWire>, ProjectionError> {
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS catalog_events;
        DROP TABLE IF EXISTS file_history;
        DROP TABLE IF EXISTS memory_catalog;
        DROP TABLE IF EXISTS config_catalog;
        DROP TABLE IF EXISTS xprompt_catalog;
        DROP TABLE IF EXISTS workflow_events;
        DROP TABLE IF EXISTS workflow_steps;
        DROP TABLE IF EXISTS workflows;
        DROP TABLE IF EXISTS workflow_tasks;
        DROP TABLE IF EXISTS workflow_agent_edges;
        DROP TABLE IF EXISTS scheduler_events;
        DROP TABLE IF EXISTS scheduler_slots;
        DROP TABLE IF EXISTS scheduler_batches;
        DROP TABLE IF EXISTS agent_lifecycle_events;
        DROP TABLE IF EXISTS agent_search_fts;
        DROP TABLE IF EXISTS agent_dismissed_identities;
        DROP TABLE IF EXISTS agent_archive;
        DROP TABLE IF EXISTS agent_artifacts;
        DROP TABLE IF EXISTS agent_edges;
        DROP TABLE IF EXISTS agent_attempts;
        DROP TABLE IF EXISTS agents;
        DROP TABLE IF EXISTS bead_events;
        DROP TABLE IF EXISTS bead_dependencies;
        DROP TABLE IF EXISTS beads;
        DROP TABLE IF EXISTS notification_search_fts;
        DROP TABLE IF EXISTS notification_pending_actions;
        DROP TABLE IF EXISTS notifications;
        DROP TABLE IF EXISTS changespec_projection_errors;
        DROP TABLE IF EXISTS changespec_search_fts;
        DROP TABLE IF EXISTS changespec_sections;
        DROP TABLE IF EXISTS changespec_edges;
        DROP TABLE IF EXISTS changespecs;
        DELETE FROM projection_meta WHERE projection <> 'event_log';
        "#,
    )?;

    let mut recreated = Vec::new();
    for migration in MIGRATIONS.iter().filter(|migration| migration.version > 1)
    {
        conn.execute_batch(migration.sql)?;
        recreated.push(MigrationWire {
            version: migration.version,
            name: migration.name.to_string(),
        });
    }
    Ok(recreated)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        let first = applied_migrations(&conn).unwrap();

        run_migrations(&conn).unwrap();
        let second = applied_migrations(&conn).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), known_migrations().len());
        assert_eq!(first.last().unwrap().version, PROJECTION_SCHEMA_VERSION);
    }
}
