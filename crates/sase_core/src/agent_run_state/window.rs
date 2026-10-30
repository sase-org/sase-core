use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::wire::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorWire {
    schema_version: u32,
    project: String,
    owner: String,
    local_hood: String,
    source_run_id: String,
}

pub fn select_agent_run_state_window(
    request: AgentRunStateWindowRequestWire,
) -> Result<AgentRunStateWindowResponseWire, AgentRunStateError> {
    validate_schema(request.schema_version)?;
    if request.project.trim().is_empty() || request.owner.trim().is_empty() {
        return Err(AgentRunStateError::InvalidRequest(
            "project and owner must be non-empty".to_string(),
        ));
    }
    if !(AGENT_RUN_STATE_WINDOW_MIN_LIMIT..=AGENT_RUN_STATE_WINDOW_MAX_LIMIT)
        .contains(&request.limit)
    {
        return Err(AgentRunStateError::InvalidRequest(format!(
            "limit must be between {AGENT_RUN_STATE_WINDOW_MIN_LIMIT} and {AGENT_RUN_STATE_WINDOW_MAX_LIMIT}"
        )));
    }

    let after = request
        .after
        .as_deref()
        .map(|cursor| decode_cursor(cursor, &request.project, &request.owner))
        .transpose()?;

    let mut seen = BTreeSet::new();
    let mut rows = request.rows;
    for row in &rows {
        if row.local_hood.trim().is_empty()
            || row.source_run_id.trim().is_empty()
        {
            return Err(AgentRunStateError::InvalidRequest(
                "window rows require non-empty local_hood and source_run_id"
                    .to_string(),
            ));
        }
        if !seen.insert((row.local_hood.clone(), row.source_run_id.clone())) {
            return Err(AgentRunStateError::InvalidRequest(format!(
                "duplicate run identity {}/{}",
                row.local_hood, row.source_run_id
            )));
        }
    }
    rows.sort_by(|left, right| {
        (&left.local_hood, &left.source_run_id)
            .cmp(&(&right.local_hood, &right.source_run_id))
    });
    if let Some(after) = after.as_ref() {
        rows.retain(|row| {
            (&row.local_hood, &row.source_run_id)
                > (&after.local_hood, &after.source_run_id)
        });
    }

    let has_more = rows.len() > request.limit as usize;
    rows.truncate(request.limit as usize);
    let next_cursor = rows.last().map(|row| {
        encode_cursor(&CursorWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: request.project.clone(),
            owner: request.owner.clone(),
            local_hood: row.local_hood.clone(),
            source_run_id: row.source_run_id.clone(),
        })
    });
    Ok(AgentRunStateWindowResponseWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        selected: rows,
        next_cursor,
        pass_complete: !has_more,
    })
}

fn validate_schema(schema_version: u32) -> Result<(), AgentRunStateError> {
    if schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::UnsupportedSchema(schema_version));
    }
    Ok(())
}

fn encode_cursor(cursor: &CursorWire) -> String {
    let bytes =
        serde_json::to_vec(cursor).expect("cursor serialization is infallible");
    let mut result = String::from("ars1:");
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut result, "{byte:02x}")
            .expect("writing to String is infallible");
    }
    result
}

fn decode_cursor(
    encoded: &str,
    project: &str,
    owner: &str,
) -> Result<CursorWire, AgentRunStateError> {
    let Some(hex) = encoded.strip_prefix("ars1:") else {
        return Err(AgentRunStateError::InvalidRequest(
            "after cursor has an unsupported format".to_string(),
        ));
    };
    if hex.len() % 2 != 0 {
        return Err(AgentRunStateError::InvalidRequest(
            "after cursor is malformed".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let (pairs, _) = hex.as_bytes().as_chunks::<2>();
    for [high_byte, low_byte] in pairs {
        let high = (*high_byte as char).to_digit(16);
        let low = (*low_byte as char).to_digit(16);
        let (Some(high), Some(low)) = (high, low) else {
            return Err(AgentRunStateError::InvalidRequest(
                "after cursor is malformed".to_string(),
            ));
        };
        bytes.push(((high << 4) | low) as u8);
    }
    let cursor: CursorWire = serde_json::from_slice(&bytes).map_err(|_| {
        AgentRunStateError::InvalidRequest(
            "after cursor is malformed".to_string(),
        )
    })?;
    if cursor.schema_version != AGENT_RUN_STATE_WIRE_SCHEMA_VERSION {
        return Err(AgentRunStateError::InvalidRequest(
            "after cursor uses an unsupported schema".to_string(),
        ));
    }
    if cursor.project != project || cursor.owner != owner {
        return Err(AgentRunStateError::InvalidRequest(
            "after cursor belongs to a different project or owner".to_string(),
        ));
    }
    if cursor.local_hood.trim().is_empty()
        || cursor.source_run_id.trim().is_empty()
    {
        return Err(AgentRunStateError::InvalidRequest(
            "after cursor is malformed".to_string(),
        ));
    }
    Ok(cursor)
}
