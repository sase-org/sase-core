//! Pure ownership and mode policy for pairing store objects.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Directory,
    File,
    Symlink,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObjectMeta {
    pub kind: ObjectKind,
    pub mode: u32,
    pub uid: u32,
    pub nlink: u64,
}

const OTHER_WRITE: u32 = 0o002;
const GROUP_WRITE: u32 = 0o020;
const STICKY: u32 = 0o1000;
const PERM_MASK: u32 = 0o777;

pub(crate) fn trust_containing_directory(
    meta: ObjectMeta,
    euid: u32,
) -> Result<(), String> {
    if meta.kind != ObjectKind::Directory {
        return Err("containing path is not a directory".to_string());
    }
    if meta.uid == euid {
        if meta.mode & (GROUP_WRITE | OTHER_WRITE) != 0 {
            return Err("containing directory is writable by another account"
                .to_string());
        }
        return Ok(());
    }
    if meta.uid == 0 {
        let world_writable = meta.mode & OTHER_WRITE != 0;
        let sticky = meta.mode & STICKY != 0;
        if world_writable && !sticky {
            return Err(
                "root-owned containing directory is world-writable without the sticky bit"
                    .to_string(),
            );
        }
        return Ok(());
    }
    Err("containing directory is owned by another account".to_string())
}

pub(crate) fn trust_protected_directory(
    meta: ObjectMeta,
    euid: u32,
) -> Result<(), String> {
    if meta.kind != ObjectKind::Directory {
        return Err("protected pairing path is not a directory".to_string());
    }
    if meta.uid != euid {
        return Err(
            "protected pairing directory has the wrong owner".to_string()
        );
    }
    if meta.mode & PERM_MASK != 0o700 {
        return Err("protected pairing directory mode must be 0700".to_string());
    }
    Ok(())
}

pub(crate) fn trust_protected_file(
    meta: ObjectMeta,
    euid: u32,
) -> Result<(), String> {
    if meta.kind != ObjectKind::File {
        return Err(
            "protected pairing object is not a regular file".to_string()
        );
    }
    if meta.uid != euid {
        return Err("protected pairing file has the wrong owner".to_string());
    }
    if meta.nlink != 1 {
        return Err("protected pairing file is hard-linked".to_string());
    }
    if meta.mode & PERM_MASK != 0o600 {
        return Err("protected pairing file mode must be 0600".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(uid: u32, mode: u32) -> ObjectMeta {
        ObjectMeta {
            kind: ObjectKind::Directory,
            mode,
            uid,
            nlink: 2,
        }
    }

    fn file(uid: u32, mode: u32, nlink: u64) -> ObjectMeta {
        ObjectMeta {
            kind: ObjectKind::File,
            mode,
            uid,
            nlink,
        }
    }

    #[test]
    fn owner_controlled_0755_containing_dir_is_trusted() {
        trust_containing_directory(dir(1000, 0o755), 1000).unwrap();
    }

    #[test]
    fn world_writable_owner_dir_is_rejected() {
        let error =
            trust_containing_directory(dir(1000, 0o777), 1000).unwrap_err();
        assert!(error.contains("writable by another account"));
    }

    #[test]
    fn other_user_containing_dir_is_rejected() {
        let error =
            trust_containing_directory(dir(1001, 0o755), 1000).unwrap_err();
        assert!(error.contains("owned by another account"));
    }

    #[test]
    fn root_owned_sticky_tmp_is_trusted() {
        trust_containing_directory(dir(0, 0o1777), 1000).unwrap();
    }

    #[test]
    fn root_owned_world_writable_without_sticky_is_rejected() {
        let error =
            trust_containing_directory(dir(0, 0o777), 1000).unwrap_err();
        assert!(error.contains("sticky"));
    }

    #[test]
    fn protected_dir_requires_owner_0700() {
        trust_protected_directory(dir(1000, 0o700), 1000).unwrap();
        assert!(trust_protected_directory(dir(1000, 0o755), 1000).is_err());
        assert!(trust_protected_directory(dir(0, 0o700), 1000).is_err());
        assert!(trust_protected_directory(
            ObjectMeta {
                kind: ObjectKind::Symlink,
                mode: 0o700,
                uid: 1000,
                nlink: 1,
            },
            1000
        )
        .is_err());
    }

    #[test]
    fn protected_file_rejects_hard_links_and_wrong_mode() {
        trust_protected_file(file(1000, 0o600, 1), 1000).unwrap();
        assert!(trust_protected_file(file(1000, 0o600, 2), 1000).is_err());
        assert!(trust_protected_file(file(1000, 0o644, 1), 1000).is_err());
        assert!(trust_protected_file(file(0, 0o600, 1), 1000).is_err());
    }
}
