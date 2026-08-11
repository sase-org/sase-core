//! Content-addressed object paths for byte-backed prompt artifacts.

use crate::artifact_ref::ArtifactRefError;

pub const ARTIFACT_OBJECT_STORE_DIR: &str = "files/objects/sha256";

pub fn artifact_object_relpath(
    sha256: &str,
) -> Result<String, ArtifactRefError> {
    validate_sha256(sha256)?;
    Ok(format!(
        "{}/{}/{}",
        ARTIFACT_OBJECT_STORE_DIR,
        &sha256[..2],
        sha256
    ))
}

pub fn artifact_object_prompt_link(
    relpath: &str,
) -> Result<String, ArtifactRefError> {
    validate_artifact_object_relpath(relpath)?;
    Ok(format!("../../{relpath}"))
}

fn validate_artifact_object_relpath(
    relpath: &str,
) -> Result<(), ArtifactRefError> {
    let prefix = format!("{ARTIFACT_OBJECT_STORE_DIR}/");
    let suffix = relpath.strip_prefix(&prefix).ok_or_else(|| {
        ArtifactRefError::validation(
            "artifact object relpath must start with files/objects/sha256/",
        )
    })?;
    let mut parts = suffix.split('/');
    let shard = parts.next().unwrap_or_default();
    let digest = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(ArtifactRefError::validation(
            "artifact object relpath must have exactly two digest segments",
        ));
    }
    validate_sha256(digest)?;
    if shard.len() != 2 || shard != &digest[..2] {
        return Err(ArtifactRefError::validation(
            "artifact object relpath shard must match the digest prefix",
        ));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), ArtifactRefError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ArtifactRefError::validation(
            "artifact object digest must contain 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_relpath_uses_digest_shard() {
        let digest =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert_eq!(
            artifact_object_relpath(digest).unwrap(),
            format!("files/objects/sha256/ab/{digest}")
        );
        assert_eq!(
            artifact_object_prompt_link(
                &artifact_object_relpath(digest).unwrap()
            )
            .unwrap(),
            format!("../../files/objects/sha256/ab/{digest}")
        );
    }

    #[test]
    fn object_paths_reject_non_full_lowercase_sha256() {
        for value in [
            "",
            "abcdef",
            "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
            "g".repeat(64).as_str(),
        ] {
            assert!(artifact_object_relpath(value).is_err(), "{value}");
        }
        assert!(
            artifact_object_prompt_link("files/objects/sha256/00/abcdef")
                .is_err()
        );
        assert!(artifact_object_prompt_link(
            "files/objects/sha256/ff/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .is_err());
    }
}
