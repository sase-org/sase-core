//! Bounded `cat-file --batch` reads of unique committed blob OIDs.

use std::collections::HashMap;
use std::path::Path;

use super::issue;
use crate::project_stats::git::{self, ResolvedRepo};
use crate::project_stats::wire::{
    ProjectStatsBudgetWire, ProjectStatsIssueWire, ProjectStatsRepoSpecWire,
};

const HEADER_OVERHEAD: u64 = 96;
const MAX_OIDS_PER_BATCH: usize = 256;

pub(crate) fn read_blobs(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    wanted: &[(String, u64)],
) -> Result<HashMap<String, Vec<u8>>, ProjectStatsIssueWire> {
    let mut blobs = HashMap::new();
    if wanted.is_empty() {
        return Ok(blobs);
    }
    let batches = pack_batches(wanted, budget.max_output_bytes.max(1))
        .map_err(|message| {
            issue(
                spec,
                "blob_unavailable",
                message,
                Some(
                    "Increase the query output budget or lower the classified blob size cap.",
                ),
            )
        })?;
    for batch in batches {
        let mut stdin = Vec::new();
        for (oid, _) in &batch {
            stdin.extend_from_slice(oid.as_bytes());
            stdin.push(b'\n');
        }
        let result = git::run_checked_with_stdin(
            Path::new(&repo.canonical_path),
            &["cat-file", "--batch"],
            &stdin,
            budget,
        )
        .map_err(|message| {
            issue(
                spec,
                "blob_unavailable",
                format!("cannot read committed blobs: {message}"),
                None,
            )
        })?;
        let parsed = parse_cat_file_batch(&result.stdout, &batch).map_err(
            |message| {
                issue(
                    spec,
                    "blob_unavailable",
                    format!("committed blob batch is malformed: {message}"),
                    None,
                )
            },
        )?;
        for (oid, bytes) in parsed {
            blobs.insert(oid, bytes);
        }
    }
    Ok(blobs)
}

pub(crate) fn pack_batches(
    wanted: &[(String, u64)],
    max_output_bytes: u64,
) -> Result<Vec<Vec<(String, u64)>>, String> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut used = 0u64;
    for (oid, size) in wanted {
        let cost = size.saturating_add(HEADER_OVERHEAD);
        if cost > max_output_bytes {
            return Err(format!(
                "blob {oid} plus framing exceeds the output budget"
            ));
        }
        let would_overflow = !current.is_empty()
            && (used.saturating_add(cost) > max_output_bytes
                || current.len() >= MAX_OIDS_PER_BATCH);
        if would_overflow {
            batches.push(std::mem::take(&mut current));
            used = 0;
        }
        current.push((oid.clone(), *size));
        used = used.saturating_add(cost);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

pub(crate) fn parse_cat_file_batch(
    bytes: &[u8],
    wanted: &[(String, u64)],
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut rest = bytes;
    let mut out = Vec::with_capacity(wanted.len());
    for (oid, declared) in wanted {
        let Some(newline) = rest.iter().position(|byte| *byte == b'\n') else {
            return Err(format!("missing cat-file header for {oid}"));
        };
        let header = &rest[..newline];
        rest = &rest[newline + 1..];
        let header_text = std::str::from_utf8(header)
            .map_err(|_| format!("cat-file header is not UTF-8 for {oid}"))?;
        if header_text == format!("{oid} missing") {
            return Err(format!("blob {oid} is missing from the object store"));
        }
        let mut parts = header_text.split(' ');
        let header_oid = parts
            .next()
            .ok_or_else(|| format!("cat-file header is empty for {oid}"))?;
        let kind = parts.next().unwrap_or("");
        let size_text = parts.next().unwrap_or("");
        if !header_oid.eq_ignore_ascii_case(oid) || parts.next().is_some() {
            return Err(format!("unexpected cat-file header: {header_text}"));
        }
        if kind != "blob" {
            return Err(format!("object {oid} is {kind}, not a blob"));
        }
        let size: u64 = size_text
            .parse()
            .map_err(|_| format!("invalid cat-file size for {oid}"))?;
        if size != *declared {
            return Err(format!(
                "blob {oid} size {size} does not match the tree size {declared}"
            ));
        }
        let count = usize::try_from(size)
            .map_err(|_| format!("blob {oid} is too large to buffer"))?;
        if rest.len() < count + 1 {
            return Err(format!("truncated cat-file body for {oid}"));
        }
        let body = rest[..count].to_vec();
        rest = &rest[count..];
        if rest.first() != Some(&b'\n') {
            return Err(format!(
                "cat-file body for {oid} is missing a trailing newline"
            ));
        }
        rest = &rest[1..];
        out.push((oid.to_ascii_lowercase(), body));
    }
    if !rest.is_empty() {
        return Err("cat-file batch has trailing bytes".to_owned());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(label: u8) -> String {
        std::iter::repeat_n(char::from(label), 40).collect()
    }

    #[test]
    fn packs_batches_by_framing_and_output_budget() {
        let first = (oid(b'a'), 10);
        let second = (oid(b'b'), 10);
        let batches =
            pack_batches(&[first.clone(), second.clone()], 140).expect("pack");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![first]);
        assert_eq!(batches[1], vec![second]);
    }

    #[test]
    fn rejects_a_blob_that_cannot_fit_the_output_budget() {
        let wanted = vec![(oid(b'a'), 1_000)];
        assert!(pack_batches(&wanted, 32).is_err());
    }

    #[test]
    fn parses_present_blobs_and_rejects_missing_or_truncated() {
        let oid = oid(b'a');
        let mut bytes = format!("{oid} blob 4\n").into_bytes();
        bytes.extend_from_slice(b"abcd\n");
        let parsed =
            parse_cat_file_batch(&bytes, &[(oid.clone(), 4)]).expect("parse");
        assert_eq!(parsed[0].1, b"abcd");

        let missing = format!("{oid} missing\n").into_bytes();
        assert!(parse_cat_file_batch(&missing, &[(oid.clone(), 4)]).is_err());

        let truncated = format!("{oid} blob 4\nab").into_bytes();
        assert!(parse_cat_file_batch(&truncated, &[(oid, 4)]).is_err());
    }
}
