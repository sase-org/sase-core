//! Extension table and media classification for note attachments.

use serde::{Deserialize, Serialize};

/// Media class for an attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentClassWire {
    Image,
    Video,
    Audio,
    Pdf,
    Text,
    Archive,
    Binary,
}

impl AttachmentClassWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Pdf => "pdf",
            Self::Text => "text",
            Self::Archive => "archive",
            Self::Binary => "binary",
        }
    }
}

/// Classification result: MIME is metadata, class drives behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentClassificationWire {
    pub mime_type: String,
    pub class: AttachmentClassWire,
}

/// Look up one lowercased extension in the shared table.
///
/// Returns `(mime_type, class)`. `svg` maps to image and never to text.
pub fn extension_mime_for(
    ext: &str,
) -> Option<(&'static str, AttachmentClassWire)> {
    match ext.to_ascii_lowercase().as_str() {
        "png" => Some(("image/png", AttachmentClassWire::Image)),
        "jpg" | "jpeg" => Some(("image/jpeg", AttachmentClassWire::Image)),
        "gif" => Some(("image/gif", AttachmentClassWire::Image)),
        "webp" => Some(("image/webp", AttachmentClassWire::Image)),
        "bmp" => Some(("image/bmp", AttachmentClassWire::Image)),
        "tif" | "tiff" => Some(("image/tiff", AttachmentClassWire::Image)),
        "svg" => Some(("image/svg+xml", AttachmentClassWire::Image)),
        "ico" => Some(("image/vnd.microsoft.icon", AttachmentClassWire::Image)),
        "heic" => Some(("image/heic", AttachmentClassWire::Image)),
        "heif" => Some(("image/heif", AttachmentClassWire::Image)),
        "avif" => Some(("image/avif", AttachmentClassWire::Image)),
        "mp4" => Some(("video/mp4", AttachmentClassWire::Video)),
        "mov" => Some(("video/quicktime", AttachmentClassWire::Video)),
        "m4v" => Some(("video/x-m4v", AttachmentClassWire::Video)),
        "webm" => Some(("video/webm", AttachmentClassWire::Video)),
        "mkv" => Some(("video/x-matroska", AttachmentClassWire::Video)),
        "avi" => Some(("video/x-msvideo", AttachmentClassWire::Video)),
        "mpg" | "mpeg" => Some(("video/mpeg", AttachmentClassWire::Video)),
        "mp3" => Some(("audio/mpeg", AttachmentClassWire::Audio)),
        "m4a" => Some(("audio/mp4", AttachmentClassWire::Audio)),
        "wav" => Some(("audio/wav", AttachmentClassWire::Audio)),
        "flac" => Some(("audio/flac", AttachmentClassWire::Audio)),
        "ogg" => Some(("audio/ogg", AttachmentClassWire::Audio)),
        "opus" => Some(("audio/opus", AttachmentClassWire::Audio)),
        "aac" => Some(("audio/aac", AttachmentClassWire::Audio)),
        "pdf" => Some(("application/pdf", AttachmentClassWire::Pdf)),
        "md" => Some(("text/markdown", AttachmentClassWire::Text)),
        "txt" | "log" => Some(("text/plain", AttachmentClassWire::Text)),
        "json" => Some(("application/json", AttachmentClassWire::Text)),
        "jsonl" => Some(("application/x-ndjson", AttachmentClassWire::Text)),
        "yaml" | "yml" => Some(("application/yaml", AttachmentClassWire::Text)),
        "toml" => Some(("application/toml", AttachmentClassWire::Text)),
        "csv" => Some(("text/csv", AttachmentClassWire::Text)),
        "html" | "htm" => Some(("text/html", AttachmentClassWire::Text)),
        "py" => Some(("text/x-python", AttachmentClassWire::Text)),
        "rs" => Some(("text/x-rust", AttachmentClassWire::Text)),
        "ts" => Some(("text/typescript", AttachmentClassWire::Text)),
        "sh" | "bash" | "zsh" => {
            Some(("text/x-shellscript", AttachmentClassWire::Text))
        }
        "diff" | "patch" | "xml" | "rst" | "ini" | "cfg" | "conf" | "tsx"
        | "js" | "jsx" | "css" | "go" | "rb" | "java" | "c" | "h" | "cpp"
        | "hpp" | "cs" | "php" | "sql" | "vue" | "svelte" | "scss"
        | "graphql" | "proto" => {
            Some(("text/plain", AttachmentClassWire::Text))
        }
        "zip" => Some(("application/zip", AttachmentClassWire::Archive)),
        "tar" => Some(("application/x-tar", AttachmentClassWire::Archive)),
        "gz" | "tgz" => {
            Some(("application/gzip", AttachmentClassWire::Archive))
        }
        "bz2" => Some(("application/x-bzip2", AttachmentClassWire::Archive)),
        "xz" => Some(("application/x-xz", AttachmentClassWire::Archive)),
        "7z" => {
            Some(("application/x-7z-compressed", AttachmentClassWire::Archive))
        }
        "rar" => Some(("application/vnd.rar", AttachmentClassWire::Archive)),
        "zst" => Some(("application/zstd", AttachmentClassWire::Archive)),
        "sqlite" | "db" => {
            Some(("application/vnd.sqlite3", AttachmentClassWire::Binary))
        }
        "wasm" => Some(("application/wasm", AttachmentClassWire::Binary)),
        _ => None,
    }
}

/// TLD-like extensions that never count as bare `stem.ext` paths.
pub fn is_tld_like_extension(ext_lower: &str) -> bool {
    matches!(
        ext_lower,
        "com"
            | "org"
            | "net"
            | "io"
            | "dev"
            | "ai"
            | "app"
            | "me"
            | "co"
            | "uk"
            | "us"
            | "ly"
            | "to"
            | "tv"
            | "cc"
            | "xyz"
    )
}

/// Classify an attachment from its name and leading bytes.
///
/// `head` is the first up to 4096 bytes; the function caps itself at 4096.
/// Magic wins over the extension, then the extension table, then a UTF-8
/// heuristic, then `application/octet-stream`.
pub fn classify_attachment(
    name: &str,
    head: &[u8],
) -> AttachmentClassificationWire {
    let head: &[u8] = if head.len() > 4096 {
        &head[..4096]
    } else {
        head
    };
    if let Some((mime, class)) = classify_magic(head) {
        return AttachmentClassificationWire {
            mime_type: mime.to_string(),
            class,
        };
    }
    if let Some(ext) = final_extension(name) {
        if let Some((mime, class)) = extension_mime_for(&ext) {
            if !is_tld_like_extension(&ext.to_ascii_lowercase()) {
                return AttachmentClassificationWire {
                    mime_type: mime.to_string(),
                    class,
                };
            }
        }
    }
    if is_utf8_text(head) {
        return AttachmentClassificationWire {
            mime_type: "text/plain".to_string(),
            class: AttachmentClassWire::Text,
        };
    }
    AttachmentClassificationWire {
        mime_type: "application/octet-stream".to_string(),
        class: AttachmentClassWire::Binary,
    }
}

fn final_extension(name: &str) -> Option<String> {
    let basename = name.rsplit('/').next().unwrap_or(name);
    let dot = basename.rfind('.')?;
    if dot == 0 || dot + 1 >= basename.len() {
        return None;
    }
    let ext = &basename[dot + 1..];
    if ext.is_empty() || !ext.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext.to_string())
}

fn is_utf8_text(head: &[u8]) -> bool {
    if head.contains(&0) {
        return false;
    }
    std::str::from_utf8(head).is_ok()
}

fn classify_magic(head: &[u8]) -> Option<(&'static str, AttachmentClassWire)> {
    use AttachmentClassWire::{Archive, Audio, Binary, Image, Pdf, Video};
    if head.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some(("image/png", Image));
    }
    if head.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some(("image/jpeg", Image));
    }
    if head.starts_with(b"GIF87a") || head.starts_with(b"GIF89a") {
        return Some(("image/gif", Image));
    }
    if head.len() >= 12 && head[0..4] == *b"RIFF" && head[8..12] == *b"WEBP" {
        return Some(("image/webp", Image));
    }
    if head.len() >= 12 && head[0..4] == *b"RIFF" && head[8..12] == *b"WAVE" {
        return Some(("audio/wav", Audio));
    }
    if head.starts_with(b"BM") {
        return Some(("image/bmp", Image));
    }
    if head.starts_with(&[0x49, 0x49, 0x2A, 0x00])
        || head.starts_with(&[0x4D, 0x4D, 0x00, 0x2A])
    {
        return Some(("image/tiff", Image));
    }
    if head.starts_with(b"%PDF-") {
        return Some(("application/pdf", Pdf));
    }
    if head.starts_with(&[0x50, 0x4B, 0x03, 0x04])
        || head.starts_with(&[0x50, 0x4B, 0x05, 0x06])
        || head.starts_with(&[0x50, 0x4B, 0x07, 0x08])
    {
        return Some(("application/zip", Archive));
    }
    if head.starts_with(&[0x1F, 0x8B]) {
        return Some(("application/gzip", Archive));
    }
    if head.starts_with(b"BZh") {
        return Some(("application/x-bzip2", Archive));
    }
    if head.starts_with(&[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00]) {
        return Some(("application/x-xz", Archive));
    }
    if head.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Some(("application/zstd", Archive));
    }
    if head.starts_with(&[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]) {
        return Some(("application/x-7z-compressed", Archive));
    }
    if head.starts_with(&[0x7F, b'E', b'L', b'F']) {
        return Some(("application/x-elf", Binary));
    }
    if head.len() >= 8 && head[4..8] == *b"ftyp" {
        if head.len() >= 12 && head[8..12] == *b"qt  " {
            return Some(("video/quicktime", Video));
        }
        return Some(("video/mp4", Video));
    }
    if head.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        if contains_subslice(head, b"webm") {
            return Some(("video/webm", Video));
        }
        return Some(("video/x-matroska", Video));
    }
    if head.starts_with(b"OggS") {
        return Some(("audio/ogg", Audio));
    }
    if head.starts_with(b"fLaC") {
        return Some(("audio/flac", Audio));
    }
    if head.starts_with(b"SQLite format 3\x00") {
        return Some(("application/vnd.sqlite3", Binary));
    }
    None
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
