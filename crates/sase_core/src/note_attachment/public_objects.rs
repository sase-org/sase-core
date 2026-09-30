//! Canonical MIME extension table and public object layout.
//!
//! Private objects stay extensionless through `artifact_object_relpath`.
//! Public objects gain a canonical extension so raw serving renders inline.

use super::scan::NoteAttachmentError;

/// Pinned MIME-to-canonical-extension table.
const CANONICAL_EXTENSIONS: &[(&str, &str)] = &[
    ("image/png", "png"),
    ("image/jpeg", "jpg"),
    ("image/gif", "gif"),
    ("image/webp", "webp"),
    ("image/svg+xml", "svg"),
    ("image/bmp", "bmp"),
    ("image/tiff", "tiff"),
    ("image/vnd.microsoft.icon", "ico"),
    ("image/heic", "heic"),
    ("image/heif", "heif"),
    ("image/avif", "avif"),
    ("video/mp4", "mp4"),
    ("video/quicktime", "mov"),
    ("video/x-m4v", "m4v"),
    ("video/webm", "webm"),
    ("video/x-matroska", "mkv"),
    ("video/x-msvideo", "avi"),
    ("video/mpeg", "mpeg"),
    ("audio/mpeg", "mp3"),
    ("audio/mp4", "m4a"),
    ("audio/wav", "wav"),
    ("audio/flac", "flac"),
    ("audio/ogg", "ogg"),
    ("audio/opus", "opus"),
    ("audio/aac", "aac"),
    ("application/pdf", "pdf"),
    ("text/plain", "txt"),
    ("text/markdown", "md"),
    ("text/csv", "csv"),
    ("text/html", "html"),
    ("application/json", "json"),
    ("application/x-ndjson", "jsonl"),
    ("application/yaml", "yaml"),
    ("application/toml", "toml"),
    ("text/x-python", "py"),
    ("text/x-rust", "rs"),
    ("text/typescript", "ts"),
    ("text/x-shellscript", "sh"),
    ("application/zip", "zip"),
    ("application/x-tar", "tar"),
    ("application/gzip", "gz"),
    ("application/x-bzip2", "bz2"),
    ("application/x-xz", "xz"),
    ("application/x-7z-compressed", "7z"),
    ("application/vnd.rar", "rar"),
    ("application/zstd", "zst"),
    ("application/vnd.sqlite3", "sqlite"),
    ("application/wasm", "wasm"),
    ("application/octet-stream", ""),
];

/// Canonical extension for one MIME type, or `None` when the type has no
/// public extension (including `application/octet-stream`).
pub fn attachment_canonical_extension(mime_type: &str) -> Option<&'static str> {
    let lower = mime_type.to_ascii_lowercase();
    for (mime, ext) in CANONICAL_EXTENSIONS {
        if *mime == lower {
            if ext.is_empty() {
                return None;
            }
            return Some(ext);
        }
    }
    None
}

fn validate_sha256(value: &str) -> Result<(), NoteAttachmentError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(NoteAttachmentError::InvalidManifest {
            message: format!(
                "attachment digest {value:?} must be a lowercase 64-character SHA-256 hex digest"
            ),
        });
    }
    Ok(())
}

/// Public object path: `files/objects/sha256/<xx>/<sha>[.<ext>]`.
///
/// Uses the canonical extension when the MIME type has one, otherwise the
/// extensionless form. Tombstone and private object layouts are unchanged.
pub fn attachment_public_object_relpath(
    sha256: &str,
    mime_type: &str,
) -> Result<String, NoteAttachmentError> {
    validate_sha256(sha256)?;
    let shard = &sha256[..2];
    match attachment_canonical_extension(mime_type) {
        Some(ext) => Ok(format!("files/objects/sha256/{shard}/{sha256}.{ext}")),
        None => Ok(format!("files/objects/sha256/{shard}/{sha256}")),
    }
}

fn is_known_canonical_extension(ext: &str) -> bool {
    CANONICAL_EXTENSIONS.iter().any(|(_, known)| *known == ext)
}

/// Digest from a public or private object path.
///
/// Accepts the extensionless form and the canonical-extension form. The
/// extension, when present, must be a known canonical extension.
/// Validates digest, shard, and shape.
pub fn attachment_object_digest_from_relpath(
    relpath: &str,
) -> Result<String, NoteAttachmentError> {
    let invalid = |message: &str| NoteAttachmentError::InvalidManifest {
        message: message.to_string(),
    };
    let prefix = "files/objects/sha256/";
    let suffix = relpath.strip_prefix(prefix).ok_or_else(|| {
        invalid(
            "attachment object relpath must start with files/objects/sha256/",
        )
    })?;
    let mut parts = suffix.split('/');
    let shard = parts.next().unwrap_or_default();
    let file = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(invalid(
            "attachment object relpath must have exactly two digest segments",
        ));
    }
    let (digest, ext) = match file.split_once('.') {
        Some((digest, ext)) => (digest, Some(ext)),
        None => (file, None),
    };
    if digest.contains('.') {
        return Err(invalid(
            "attachment object filename must have at most one extension",
        ));
    }
    validate_sha256(digest).map_err(|error| match error {
        NoteAttachmentError::InvalidManifest { message } => {
            NoteAttachmentError::InvalidManifest { message }
        }
        other => other,
    })?;
    if shard.len() != 2 || shard != &digest[..2] {
        return Err(invalid(
            "attachment object relpath shard must match the digest prefix",
        ));
    }
    if let Some(ext) = ext {
        if ext.is_empty()
            || !ext.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || !is_known_canonical_extension(&ext.to_ascii_lowercase())
        {
            return Err(invalid(
                "attachment object extension must be a known canonical extension",
            ));
        }
    }
    Ok(digest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_table_is_pinned() {
        for (mime, ext) in [
            ("image/png", "png"),
            ("image/jpeg", "jpg"),
            ("image/gif", "gif"),
            ("image/webp", "webp"),
            ("image/svg+xml", "svg"),
            ("video/mp4", "mp4"),
            ("video/webm", "webm"),
            ("text/plain", "txt"),
            ("application/json", "json"),
            ("text/markdown", "md"),
            ("text/csv", "csv"),
            ("application/pdf", "pdf"),
        ] {
            assert_eq!(
                attachment_canonical_extension(mime),
                Some(ext),
                "{mime}"
            );
        }
        assert_eq!(
            attachment_canonical_extension("application/octet-stream"),
            None
        );
        assert_eq!(
            attachment_canonical_extension("application/x-unknown"),
            None
        );
    }

    #[test]
    fn public_and_old_path_forms_round_trip() {
        let digest =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert_eq!(
            attachment_public_object_relpath(digest, "image/png").unwrap(),
            format!("files/objects/sha256/ab/{digest}.png")
        );
        assert_eq!(
            attachment_public_object_relpath(
                digest,
                "application/octet-stream"
            )
            .unwrap(),
            format!("files/objects/sha256/ab/{digest}")
        );
        assert_eq!(
            attachment_object_digest_from_relpath(&format!(
                "files/objects/sha256/ab/{digest}.png"
            ))
            .unwrap(),
            digest
        );
        assert_eq!(
            attachment_object_digest_from_relpath(&format!(
                "files/objects/sha256/ab/{digest}"
            ))
            .unwrap(),
            digest
        );
        attachment_public_object_relpath("not-a-digest", "image/png")
            .unwrap_err();
        attachment_object_digest_from_relpath("files/objects/sha256/ab/xyz")
            .unwrap_err();
        attachment_object_digest_from_relpath(&format!(
            "files/objects/sha256/ff/{digest}.png"
        ))
        .unwrap_err();
    }
}
