use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};

use chrono::DateTime;
use serde_json::Value as JsonValue;

use super::wire::{
    PerfLogCoverageWire, PerfLogSourceIdWire, PerfLogSourceWire,
    PerfLogsQueryWire,
};

#[derive(Debug, Clone)]
pub(crate) struct PerfLogRecord {
    pub source: PerfLogSourceIdWire,
    pub ts: f64,
    pub value: JsonValue,
}

pub(crate) struct SourceRead {
    pub coverage: PerfLogCoverageWire,
    pub records: Vec<PerfLogRecord>,
}

pub(crate) fn read_source(
    source: &PerfLogSourceWire,
    request: &PerfLogsQueryWire,
) -> Result<SourceRead, String> {
    let path = source.path.to_string_lossy().into_owned();
    let mut coverage = PerfLogCoverageWire {
        source: source.id,
        path,
        present: false,
        records_scanned: 0,
        records_in_window: 0,
        earliest_ts: None,
        latest_ts: None,
        truncated: false,
        malformed_skipped: 0,
    };

    let metadata = match fs::metadata(&source.path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SourceRead {
                coverage,
                records: Vec::new(),
            });
        }
        Err(error) => {
            return Err(format!(
                "failed to inspect perf log {} at {}: {error}",
                source.id.as_str(),
                source.path.display()
            ));
        }
    };
    coverage.present = true;
    if !metadata.is_file() || metadata.len() == 0 {
        return Ok(SourceRead {
            coverage,
            records: Vec::new(),
        });
    }

    let (text, truncated_by_bytes) = read_tail_bounded(
        &source.path,
        metadata.len(),
        request.max_bytes_per_source,
    )
    .map_err(|error| {
        format!(
            "failed to read perf log {} at {}: {error}",
            source.id.as_str(),
            source.path.display()
        )
    })?;
    coverage.truncated = truncated_by_bytes;

    let mut lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let max_records =
        usize::try_from(request.max_records_per_source).unwrap_or(usize::MAX);
    if lines.len() > max_records {
        coverage.truncated = true;
        lines = lines.split_off(lines.len() - max_records);
    } else if max_records == 0 && !lines.is_empty() {
        coverage.truncated = true;
        lines.clear();
    }

    let start_ts = request.start_ts as f64;
    let end_ts = request.end_ts as f64;
    let mut records = Vec::new();
    for line in lines {
        coverage.records_scanned += 1;
        let value: JsonValue = match serde_json::from_str(line.trim()) {
            Ok(value) => value,
            Err(_) => {
                coverage.malformed_skipped += 1;
                continue;
            }
        };
        let Some(ts) = record_timestamp(source.id, &value) else {
            coverage.malformed_skipped += 1;
            continue;
        };
        coverage.earliest_ts = Some(match coverage.earliest_ts {
            Some(existing) => existing.min(ts),
            None => ts,
        });
        coverage.latest_ts = Some(match coverage.latest_ts {
            Some(existing) => existing.max(ts),
            None => ts,
        });
        if ts >= start_ts && ts < end_ts {
            coverage.records_in_window += 1;
            records.push(PerfLogRecord {
                source: source.id,
                ts,
                value,
            });
        }
    }

    Ok(SourceRead { coverage, records })
}

fn read_tail_bounded(
    path: &std::path::Path,
    file_size: u64,
    max_bytes: u64,
) -> std::io::Result<(String, bool)> {
    if max_bytes == 0 {
        return Ok((String::new(), file_size > 0));
    }

    let wanted = file_size.min(max_bytes);
    let start = file_size - wanted;
    let seek_start = if start > 0 { start - 1 } else { 0 };
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(seek_start))?;
    let mut data = Vec::new();
    file.take(file_size - seek_start).read_to_end(&mut data)?;

    if start > 0 {
        if data.first() == Some(&b'\n') {
            data.remove(0);
        } else if let Some(pos) = data.iter().position(|byte| *byte == b'\n') {
            data.drain(..=pos);
        } else {
            data.clear();
        }
    }

    Ok((String::from_utf8_lossy(&data).into_owned(), start > 0))
}

fn record_timestamp(
    source: PerfLogSourceIdWire,
    value: &JsonValue,
) -> Option<f64> {
    let fields: &[&str] = match source {
        PerfLogSourceIdWire::Startup | PerfLogSourceIdWire::AgentLoads => {
            &["timestamp", "ts"]
        }
        PerfLogSourceIdWire::Stalls
        | PerfLogSourceIdWire::LaunchTiming
        | PerfLogSourceIdWire::GitOps
        | PerfLogSourceIdWire::ExternalTools => &["ts", "timestamp"],
    };
    for field in fields {
        if let Some(ts) = value.get(*field).and_then(parse_timestamp_value) {
            return Some(ts);
        }
    }
    None
}

fn parse_timestamp_value(value: &JsonValue) -> Option<f64> {
    if let Some(number) = value.as_f64().filter(|value| value.is_finite()) {
        return Some(number);
    }
    let text = value.as_str()?;
    if let Ok(number) = text.parse::<f64>() {
        if number.is_finite() {
            return Some(number);
        }
    }
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis() as f64 / 1000.0)
}
