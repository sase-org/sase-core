use crate::reference_path::{validate_relative_payload, RelativePayloadError};

use super::{
    ArtifactRefError, ArtifactRefPathFilterBatchWire,
    ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactPathFilter {
    raw: Option<Vec<String>>,
    positive: Vec<GlobPattern>,
    negative: Vec<GlobPattern>,
}

impl ArtifactPathFilter {
    pub(super) fn compile(
        globs: Option<&[String]>,
    ) -> Result<Self, ArtifactRefError> {
        let Some(globs) = globs else {
            return Ok(Self {
                raw: None,
                positive: Vec::new(),
                negative: Vec::new(),
            });
        };
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        for glob in globs {
            let (negated, pattern) = glob
                .strip_prefix('!')
                .map(|pattern| (true, pattern))
                .unwrap_or((false, glob.as_str()));
            let compiled = GlobPattern::compile(pattern)?;
            if negated {
                negative.push(compiled);
            } else {
                positive.push(compiled);
            }
        }
        Ok(Self {
            raw: Some(globs.to_vec()),
            positive,
            negative,
        })
    }

    pub(super) fn allows(
        &self,
        payload: &str,
    ) -> Result<bool, ArtifactRefError> {
        validate_filter_payload(payload)?;
        if self.raw.is_none() {
            return Ok(true);
        }
        if self.positive.is_empty() && self.negative.is_empty() {
            return Ok(false);
        }
        if self.negative.iter().any(|glob| glob.matches(payload)) {
            return Ok(false);
        }
        if self.positive.is_empty() {
            return Ok(true);
        }
        Ok(self.positive.iter().any(|glob| glob.matches(payload)))
    }

    pub(super) fn summary(&self) -> String {
        match self.raw.as_deref() {
            None => "path_globs=<default>".to_string(),
            Some([]) => "path_globs=[]".to_string(),
            Some(globs) => format!("path_globs=[{}]", globs.join(",")),
        }
    }
}

pub fn filter_artifact_ref_path_payloads(
    kind: &str,
    path_globs: Option<&[String]>,
    candidates: &[String],
) -> Result<ArtifactRefPathFilterBatchWire, ArtifactRefError> {
    let filter = ArtifactPathFilter::compile(path_globs)?;
    let mut allowed = Vec::new();
    let mut filtered = Vec::new();
    for candidate in candidates {
        if filter.allows(candidate)? {
            allowed.push(candidate.clone());
        } else {
            filtered.push(candidate.clone());
        }
    }
    Ok(ArtifactRefPathFilterBatchWire {
        schema_version: ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION,
        kind: kind.to_string(),
        allowed,
        filtered,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GlobPattern {
    segments: Vec<GlobSegment>,
}

impl GlobPattern {
    fn compile(pattern: &str) -> Result<Self, ArtifactRefError> {
        validate_glob_payload(pattern)?;
        let segments = pattern
            .split('/')
            .map(GlobSegment::compile)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { segments })
    }

    fn matches(&self, payload: &str) -> bool {
        let path_segments = payload.split('/').collect::<Vec<_>>();
        self.matches_from(0, &path_segments, 0)
    }

    fn matches_from(
        &self,
        pattern_index: usize,
        path_segments: &[&str],
        path_index: usize,
    ) -> bool {
        if pattern_index == self.segments.len() {
            return path_index == path_segments.len();
        }
        match &self.segments[pattern_index] {
            GlobSegment::DoubleStar => {
                if self.matches_from(
                    pattern_index + 1,
                    path_segments,
                    path_index,
                ) {
                    return true;
                }
                (path_index < path_segments.len())
                    && self.matches_from(
                        pattern_index,
                        path_segments,
                        path_index + 1,
                    )
            }
            GlobSegment::Part(pattern) => {
                path_index < path_segments.len()
                    && segment_matches(pattern, path_segments[path_index])
                    && self.matches_from(
                        pattern_index + 1,
                        path_segments,
                        path_index + 1,
                    )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GlobSegment {
    DoubleStar,
    Part(String),
}

impl GlobSegment {
    fn compile(segment: &str) -> Result<Self, ArtifactRefError> {
        if segment == "**" {
            return Ok(Self::DoubleStar);
        }
        if segment.contains("**") {
            return Err(ArtifactRefError::validation(
                "path glob '**' must occupy a whole path segment",
            ));
        }
        Ok(Self::Part(segment.to_string()))
    }
}

fn segment_matches(pattern: &str, value: &str) -> bool {
    segment_matches_bytes(pattern.as_bytes(), value.as_bytes())
}

fn segment_matches_bytes(pattern: &[u8], value: &[u8]) -> bool {
    if pattern.is_empty() {
        return value.is_empty();
    }
    match pattern[0] {
        b'*' => {
            segment_matches_bytes(&pattern[1..], value)
                || (!value.is_empty()
                    && segment_matches_bytes(pattern, &value[1..]))
        }
        b'?' => {
            !value.is_empty()
                && segment_matches_bytes(&pattern[1..], &value[1..])
        }
        byte => {
            value.first() == Some(&byte)
                && segment_matches_bytes(&pattern[1..], &value[1..])
        }
    }
}

fn validate_glob_payload(pattern: &str) -> Result<(), ArtifactRefError> {
    if pattern.is_empty() {
        return Err(ArtifactRefError::validation(
            "path glob must not be empty",
        ));
    }
    validate_relative_payload(pattern).map_err(|error| {
        ArtifactRefError::validation(match error {
            RelativePayloadError::Empty => {
                "path glob must not be empty".to_string()
            }
            RelativePayloadError::Absolute => {
                "path glob must be relative".to_string()
            }
            RelativePayloadError::Backslash => {
                "path glob must use forward slashes".to_string()
            }
            RelativePayloadError::EmptySegment => {
                "path glob contains an empty segment".to_string()
            }
            RelativePayloadError::ParentSegment => {
                "path glob must not contain '..'".to_string()
            }
        })
    })
}

fn validate_filter_payload(payload: &str) -> Result<(), ArtifactRefError> {
    validate_relative_payload(payload).map_err(|error| {
        ArtifactRefError::validation(match error {
            RelativePayloadError::Empty => {
                "artifact path must not be empty".to_string()
            }
            RelativePayloadError::Absolute => {
                "artifact path must be relative".to_string()
            }
            RelativePayloadError::Backslash => {
                "artifact path must use forward slashes".to_string()
            }
            RelativePayloadError::EmptySegment => {
                "artifact path contains an empty segment".to_string()
            }
            RelativePayloadError::ParentSegment => {
                "artifact path must not contain '..'".to_string()
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed(globs: &[&str], payload: &str) -> bool {
        let globs = globs
            .iter()
            .map(|glob| glob.to_string())
            .collect::<Vec<_>>();
        ArtifactPathFilter::compile(Some(&globs))
            .unwrap()
            .allows(payload)
            .unwrap()
    }

    #[test]
    fn globstar_matches_root_and_nested_paths_case_sensitively() {
        assert!(allowed(&["**/*.md"], "README.md"));
        assert!(allowed(&["**/*.md"], "docs/README.md"));
        assert!(!allowed(&["**/*.md"], "docs/README.MD"));
    }

    #[test]
    fn positives_or_together_and_negations_veto() {
        assert!(allowed(&["plans/*.md", "docs/*.md"], "docs/a.md"));
        assert!(!allowed(&["plans/*.md", "docs/*.md"], "other/a.md"));
        assert!(!allowed(&["**/*.md", "!drafts/**"], "drafts/a.md"));
    }

    #[test]
    fn negative_only_allows_everything_except_vetoes() {
        assert!(allowed(&["!drafts/**"], "notes/a.md"));
        assert!(!allowed(&["!drafts/**"], "drafts/a.md"));
    }

    #[test]
    fn explicit_empty_filter_allows_nothing() {
        let globs = Vec::<String>::new();
        let filter = ArtifactPathFilter::compile(Some(&globs)).unwrap();
        assert!(!filter.allows("README.md").unwrap());
    }

    #[test]
    fn malformed_patterns_and_unsafe_payloads_error() {
        for glob in ["", "/abs.md", r"bad\path.md", "../x.md", "a/***/b.md"] {
            let globs = vec![glob.to_string()];
            assert!(
                ArtifactPathFilter::compile(Some(&globs)).is_err(),
                "{glob}"
            );
        }
        let filter = ArtifactPathFilter::compile(None).unwrap();
        assert!(filter.allows("/abs.md").is_err());
        assert!(filter.allows(r"bad\path.md").is_err());
        assert!(filter.allows("../x.md").is_err());
    }

    #[test]
    fn batch_filter_reports_allowed_and_filtered_in_input_order() {
        let globs = vec!["**/*.md".to_string(), "!drafts/**".to_string()];
        let batch = filter_artifact_ref_path_payloads(
            "research",
            Some(&globs),
            &[
                "README.md".to_string(),
                "drafts/a.md".to_string(),
                "image.png".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(batch.allowed, ["README.md"]);
        assert_eq!(batch.filtered, ["drafts/a.md", "image.png"]);
    }
}
