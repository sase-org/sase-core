use super::support::require_schema;
use super::wire::{
    PublicationErrorClassifyRequestWire, PublicationErrorClassifyResponseWire,
    PublicationRecoveryError, PUBLICATION_ERROR_INDEX_LOCK,
    PUBLICATION_ERROR_LOCK_BUSY, PUBLICATION_ERROR_MANIFEST_MISMATCH,
    PUBLICATION_ERROR_OTHER, PUBLICATION_ERROR_REBASE_CONFLICT,
    PUBLICATION_ERROR_TIMEOUT, PUBLICATION_ERROR_UNKNOWN,
    PUBLICATION_ERROR_UNSTAGED_CHANGES,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
};

const ERROR_CLASS_NEEDLES: &[(&str, &str)] = &[
    ("did not complete within", PUBLICATION_ERROR_TIMEOUT),
    ("agents sync lock is busy", PUBLICATION_ERROR_LOCK_BUSY),
    (
        "agents sync clone lock is busy",
        PUBLICATION_ERROR_LOCK_BUSY,
    ),
    ("index.lock", PUBLICATION_ERROR_INDEX_LOCK),
    ("unstaged changes", PUBLICATION_ERROR_UNSTAGED_CHANGES),
    ("would be overwritten", PUBLICATION_ERROR_UNSTAGED_CHANGES),
    (
        "index contains uncommitted changes",
        PUBLICATION_ERROR_UNSTAGED_CHANGES,
    ),
    ("rebase", PUBLICATION_ERROR_REBASE_CONFLICT),
    ("conflict", PUBLICATION_ERROR_REBASE_CONFLICT),
    ("manifest", PUBLICATION_ERROR_MANIFEST_MISMATCH),
    (
        "exceeds the byte limit",
        PUBLICATION_ERROR_MANIFEST_MISMATCH,
    ),
];

pub fn classify_publication_error_class(error: Option<&str>) -> &'static str {
    let Some(error) = error.filter(|value| !value.is_empty()) else {
        return PUBLICATION_ERROR_UNKNOWN;
    };
    let lowered = error.to_lowercase();
    for (needle, error_class) in ERROR_CLASS_NEEDLES {
        if lowered.contains(needle) {
            return error_class;
        }
    }
    PUBLICATION_ERROR_OTHER
}

pub fn classify_publication_error(
    request: &PublicationErrorClassifyRequestWire,
) -> Result<PublicationErrorClassifyResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    Ok(PublicationErrorClassifyResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        error_class: classify_publication_error_class(request.error.as_deref())
            .to_string(),
    })
}
