//! Descriptor-relative, no-follow filesystem operations for the pairing store.

use std::path::{Path, PathBuf};

use super::error::MobilePairingError as Error;

pub(crate) const PAIRING_DIR_NAME: &str = "pairing";
pub(crate) const MOBILE_GATEWAY_DIR_NAME: &str = "mobile_gateway";
pub(crate) const STORE_FILE_NAME: &str = "challenges.json";
pub(crate) const LOCK_FILE_NAME: &str = "challenges.lock";

pub(crate) fn resolve_state_root(
    explicit: Option<&str>,
) -> Result<PathBuf, Error> {
    if let Some(value) =
        explicit.map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(value));
    }
    if let Some(home) = std::env::var_os("SASE_HOME") {
        let path = PathBuf::from(home);
        if !path.as_os_str().is_empty() {
            return Ok(path);
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".sase"));
    }
    Err(Error::unsafe_path(
        "neither an explicit state root, SASE_HOME, nor HOME is available",
    ))
}

pub(crate) fn pairing_dir(state_root: &Path) -> PathBuf {
    state_root
        .join(MOBILE_GATEWAY_DIR_NAME)
        .join(PAIRING_DIR_NAME)
}

#[cfg(unix)]
mod unix {
    use std::ffi::{CString, OsStr};
    use std::fs::File;
    use std::io::{self, ErrorKind, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{fchown, MetadataExt};
    use std::path::{Component, Path, PathBuf};
    use std::time::{Duration, Instant};

    use fs2::FileExt;

    use super::super::error::MobilePairingError as Error;
    use super::super::trust::{
        trust_containing_directory, trust_protected_directory,
        trust_protected_file, ObjectKind, ObjectMeta,
    };
    use super::{
        pairing_dir, LOCK_FILE_NAME, MOBILE_GATEWAY_DIR_NAME, PAIRING_DIR_NAME,
        STORE_FILE_NAME,
    };

    const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
    const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
    const MAX_BACKOFF: Duration = Duration::from_millis(250);
    // mode_t is u32 on Linux and u16 on macOS; keep these native at the syscall
    // boundary so mkdirat/fchmod/openat compile on both.
    const CONTAINING_MODE: libc::mode_t = 0o755;
    const PROTECTED_DIR_MODE: libc::mode_t = 0o700;
    const PROTECTED_FILE_MODE: libc::mode_t = 0o600;

    pub(crate) struct ProtectedDir {
        dir: OwnedFd,
        path: PathBuf,
    }

    pub(crate) struct HeldLock {
        file: File,
    }

    impl Drop for HeldLock {
        fn drop(&mut self) {
            let _ = FileExt::unlock(&self.file);
        }
    }

    pub(crate) fn open_pairing_dir(
        state_root: &Path,
        create: bool,
    ) -> Result<ProtectedDir, Error> {
        let pairing = pairing_dir(state_root);
        let containing = pairing
            .parent()
            .ok_or_else(|| Error::unsafe_path("pairing path has no parent"))?;
        let existing = deepest_existing_ancestor(containing)?;
        let canonical = fs_canonicalize(&existing)?;
        let mut current = open_root()?;
        verify_containing(current.as_raw_fd(), Path::new("/"))?;
        for component in canonical.components() {
            match component {
                Component::RootDir => continue,
                Component::Normal(name) => {
                    let child = c_os(name)?;
                    current = openat_dir(current.as_raw_fd(), &child, false)?;
                    verify_containing(current.as_raw_fd(), Path::new(name))?;
                }
                Component::CurDir
                | Component::ParentDir
                | Component::Prefix(_) => {
                    return Err(Error::unsafe_path(
                        "pairing path contains an unsupported component",
                    ));
                }
            }
        }
        let remainder = remaining_components(containing, &existing)?;
        if !remainder.is_empty() && !create {
            return Err(Error::MissingStore);
        }
        for component in remainder {
            current = open_or_create_containing(&current, &component)?;
        }
        let pairing_fd = if create {
            open_or_create_protected(&current, PAIRING_DIR_NAME)?
        } else {
            open_existing_protected(&current, PAIRING_DIR_NAME)?
        };
        Ok(ProtectedDir {
            dir: pairing_fd,
            path: pairing,
        })
    }

    fn open_root() -> Result<OwnedFd, Error> {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC;
        let root = CString::new("/").expect("static path");
        let fd = unsafe { libc::open(root.as_ptr(), flags) };
        if fd < 0 {
            Err(Error::io(Path::new("/"), io::Error::last_os_error()))
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    }

    impl ProtectedDir {
        pub(crate) fn lock(&self) -> Result<HeldLock, Error> {
            let file = open_protected_file(
                self.dir.as_raw_fd(),
                LOCK_FILE_NAME,
                true,
            )?;
            wait_for_exclusive_lock(file)
        }

        pub(crate) fn read_store(&self) -> Result<Option<Vec<u8>>, Error> {
            match open_protected_file(
                self.dir.as_raw_fd(),
                STORE_FILE_NAME,
                false,
            ) {
                Ok(mut file) => {
                    let mut bytes = Vec::new();
                    std::io::Read::read_to_end(&mut file, &mut bytes)
                        .map_err(|source| Error::io(&self.path, source))?;
                    Ok(Some(bytes))
                }
                Err(Error::MissingStore) => Ok(None),
                Err(error) => Err(error),
            }
        }

        pub(crate) fn write_store(&self, bytes: &[u8]) -> Result<(), Error> {
            let tmp_name = format!(
                ".{}.{}.{}.tmp",
                STORE_FILE_NAME,
                std::process::id(),
                nanos()
            );
            let tmp_c = c_name(&tmp_name)?;
            let store_c = c_name(STORE_FILE_NAME)?;
            let flags = libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC;
            let fd = unsafe {
                libc::openat(
                    self.dir.as_raw_fd(),
                    tmp_c.as_ptr(),
                    flags,
                    PROTECTED_FILE_MODE,
                )
            };
            if fd < 0 {
                return Err(Error::io(&self.path, io::Error::last_os_error()));
            }
            let mut file = unsafe { File::from_raw_fd(fd) };
            let write_result = (|| -> Result<(), Error> {
                verify_protected_file(&file)?;
                fchmod_file(&file, PROTECTED_FILE_MODE)?;
                file.write_all(bytes)
                    .map_err(|source| Error::io(&self.path, source))?;
                file.sync_all()
                    .map_err(|source| Error::io(&self.path, source))?;
                let rc = unsafe {
                    libc::renameat(
                        self.dir.as_raw_fd(),
                        tmp_c.as_ptr(),
                        self.dir.as_raw_fd(),
                        store_c.as_ptr(),
                    )
                };
                if rc != 0 {
                    return Err(Error::io(
                        &self.path,
                        io::Error::last_os_error(),
                    ));
                }
                fsync_dir(self.dir.as_raw_fd(), &self.path)?;
                Ok(())
            })();
            if write_result.is_err() {
                let _ = unsafe {
                    libc::unlinkat(self.dir.as_raw_fd(), tmp_c.as_ptr(), 0)
                };
            }
            write_result
        }
    }

    fn wait_for_exclusive_lock(file: File) -> Result<HeldLock, Error> {
        let started = Instant::now();
        let mut backoff = INITIAL_BACKOFF;
        let mut attempt = 0_u64;
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(HeldLock { file }),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if started.elapsed() >= LOCK_TIMEOUT {
                        return Err(Error::LockTimeout);
                    }
                    let remaining =
                        LOCK_TIMEOUT.saturating_sub(started.elapsed());
                    std::thread::sleep(backoff.min(remaining));
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    attempt = attempt.saturating_add(1);
                    let _ = attempt;
                }
                Err(_) => return Err(Error::Unavailable),
            }
        }
    }

    fn deepest_existing_ancestor(path: &Path) -> Result<PathBuf, Error> {
        let mut current = path.to_path_buf();
        loop {
            match std::fs::symlink_metadata(&current) {
                Ok(_) => return Ok(current),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match current.parent() {
                        Some(parent) if parent != current => {
                            current = parent.to_path_buf();
                        }
                        _ => {
                            return Err(Error::unsafe_path(
                                "pairing path has no existing ancestor",
                            ))
                        }
                    }
                }
                Err(source) => return Err(Error::io(path, source)),
            }
        }
    }

    fn fs_canonicalize(path: &Path) -> Result<PathBuf, Error> {
        std::fs::canonicalize(path).map_err(|source| Error::io(path, source))
    }

    fn remaining_components(
        target: &Path,
        existing: &Path,
    ) -> Result<Vec<CString>, Error> {
        let target_canon_hint = target.to_path_buf();
        let suffix = target_canon_hint
            .strip_prefix(existing)
            .unwrap_or(Path::new(""));
        suffix
            .components()
            .filter_map(|component| match component {
                Component::Normal(name) => Some(c_os(name)),
                Component::CurDir => None,
                _ => Some(Err(Error::unsafe_path(
                    "pairing path contains an unsupported component",
                ))),
            })
            .collect()
    }

    fn open_or_create_containing(
        parent: &OwnedFd,
        name: &CString,
    ) -> Result<OwnedFd, Error> {
        match openat_dir(parent.as_raw_fd(), name, false) {
            Ok(fd) => {
                verify_containing(
                    fd.as_raw_fd(),
                    Path::new(MOBILE_GATEWAY_DIR_NAME),
                )?;
                Ok(fd)
            }
            Err(Error::MissingStore) => {
                mkdirat(parent.as_raw_fd(), name, CONTAINING_MODE)?;
                let fd = openat_dir(parent.as_raw_fd(), name, false)?;
                fchmod_fd(fd.as_raw_fd(), CONTAINING_MODE)?;
                fchown_current(fd.as_raw_fd())?;
                verify_containing(
                    fd.as_raw_fd(),
                    Path::new(MOBILE_GATEWAY_DIR_NAME),
                )?;
                Ok(fd)
            }
            Err(error) => Err(error),
        }
    }

    fn open_or_create_protected(
        parent: &OwnedFd,
        name: &str,
    ) -> Result<OwnedFd, Error> {
        let c_name = c_name(name)?;
        match openat_dir(parent.as_raw_fd(), &c_name, false) {
            Ok(fd) => {
                verify_protected_dir(fd.as_raw_fd())?;
                Ok(fd)
            }
            Err(Error::MissingStore) => {
                mkdirat(parent.as_raw_fd(), &c_name, PROTECTED_DIR_MODE)?;
                let fd = openat_dir(parent.as_raw_fd(), &c_name, false)?;
                fchmod_fd(fd.as_raw_fd(), PROTECTED_DIR_MODE)?;
                fchown_current(fd.as_raw_fd())?;
                verify_protected_dir(fd.as_raw_fd())?;
                Ok(fd)
            }
            Err(error) => Err(error),
        }
    }

    fn open_existing_protected(
        parent: &OwnedFd,
        name: &str,
    ) -> Result<OwnedFd, Error> {
        let c_name = c_name(name)?;
        match openat_dir(parent.as_raw_fd(), &c_name, false) {
            Ok(fd) => {
                verify_protected_dir(fd.as_raw_fd())?;
                Ok(fd)
            }
            Err(Error::MissingStore) => Err(Error::MissingStore),
            Err(error) => Err(error),
        }
    }

    fn open_protected_file(
        dir_fd: libc::c_int,
        name: &str,
        create: bool,
    ) -> Result<File, Error> {
        let c_name = c_name(name)?;
        let existing_flags = libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        let existing =
            unsafe { libc::openat(dir_fd, c_name.as_ptr(), existing_flags) };
        if existing >= 0 {
            let file = unsafe { File::from_raw_fd(existing) };
            verify_protected_file(&file)?;
            return Ok(file);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::ENOENT) if create => {}
            Some(libc::ENOENT) => return Err(Error::MissingStore),
            Some(libc::ELOOP) => {
                return Err(Error::unsafe_path("protected file is a symlink"));
            }
            _ => return Err(Error::Unavailable),
        }
        let create_flags = existing_flags | libc::O_CREAT | libc::O_EXCL;
        let created = unsafe {
            libc::openat(
                dir_fd,
                c_name.as_ptr(),
                create_flags,
                PROTECTED_FILE_MODE,
            )
        };
        if created < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == ErrorKind::AlreadyExists {
                return open_protected_file(dir_fd, name, false);
            }
            return Err(Error::Unavailable);
        }
        let file = unsafe { File::from_raw_fd(created) };
        fchmod_file(&file, PROTECTED_FILE_MODE)?;
        verify_protected_file(&file)?;
        Ok(file)
    }

    fn openat_dir(
        parent: libc::c_int,
        name: &CString,
        _create: bool,
    ) -> Result<OwnedFd, Error> {
        let flags = libc::O_RDONLY
            | libc::O_DIRECTORY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC;
        let fd = unsafe { libc::openat(parent, name.as_ptr(), flags) };
        if fd < 0 {
            let err = io::Error::last_os_error();
            return match err.raw_os_error() {
                Some(libc::ENOENT) => Err(Error::MissingStore),
                Some(libc::ELOOP) => {
                    Err(Error::unsafe_path("directory is a symlink"))
                }
                Some(libc::ENOTDIR) if is_symlink_at(parent, name) => {
                    Err(Error::unsafe_path("directory is a symlink"))
                }
                Some(libc::ENOTDIR) => {
                    Err(Error::unsafe_path("pairing path is not a directory"))
                }
                _ => Err(Error::Unavailable),
            };
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn mkdirat(
        parent: libc::c_int,
        name: &CString,
        mode: libc::mode_t,
    ) -> Result<(), Error> {
        let rc = unsafe { libc::mkdirat(parent, name.as_ptr(), mode) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == ErrorKind::AlreadyExists {
                return Ok(());
            }
            return Err(Error::Unavailable);
        }
        Ok(())
    }

    fn verify_containing(fd: libc::c_int, path: &Path) -> Result<(), Error> {
        let meta = metadata_fd(fd, path)?;
        trust_containing_directory(meta, euid()).map_err(Error::unsafe_path)
    }

    fn verify_protected_dir(fd: libc::c_int) -> Result<(), Error> {
        let meta = metadata_fd(fd, Path::new(PAIRING_DIR_NAME))?;
        trust_protected_directory(meta, euid()).map_err(Error::unsafe_path)
    }

    fn verify_protected_file(file: &File) -> Result<(), Error> {
        let meta = file.metadata().map_err(|_| Error::Unavailable)?;
        trust_protected_file(inspect_std(&meta), euid())
            .map_err(Error::unsafe_path)
    }

    fn metadata_fd(fd: libc::c_int, path: &Path) -> Result<ObjectMeta, Error> {
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let rc = unsafe { libc::fstat(fd, &mut stat) };
        if rc != 0 {
            return Err(Error::io(path, io::Error::last_os_error()));
        }
        Ok(inspect_stat(&stat))
    }

    fn inspect_std(meta: &std::fs::Metadata) -> ObjectMeta {
        let file_type = meta.file_type();
        let kind = if file_type.is_symlink() {
            ObjectKind::Symlink
        } else if file_type.is_dir() {
            ObjectKind::Directory
        } else if file_type.is_file() {
            ObjectKind::File
        } else {
            ObjectKind::Other
        };
        ObjectMeta {
            kind,
            mode: meta.mode() & 0o7777,
            uid: meta.uid(),
            nlink: meta.nlink(),
        }
    }

    fn inspect_stat(stat: &libc::stat) -> ObjectMeta {
        let kind = match stat.st_mode & libc::S_IFMT {
            libc::S_IFDIR => ObjectKind::Directory,
            libc::S_IFREG => ObjectKind::File,
            libc::S_IFLNK => ObjectKind::Symlink,
            _ => ObjectKind::Other,
        };
        // mode_t and nlink_t widths differ across Unix; keep the widening casts.
        #[allow(clippy::unnecessary_cast, clippy::useless_conversion)]
        let mode = u32::from(stat.st_mode) & 0o7777;
        #[allow(clippy::unnecessary_cast)]
        let nlink = stat.st_nlink as u64;
        ObjectMeta {
            kind,
            mode,
            uid: stat.st_uid,
            nlink,
        }
    }

    fn fchmod_fd(fd: libc::c_int, mode: libc::mode_t) -> Result<(), Error> {
        let rc = unsafe { libc::fchmod(fd, mode) };
        if rc != 0 {
            Err(Error::Unavailable)
        } else {
            Ok(())
        }
    }

    fn fchmod_file(file: &File, mode: libc::mode_t) -> Result<(), Error> {
        fchmod_fd(file.as_raw_fd(), mode)
    }

    fn fchown_current(fd: libc::c_int) -> Result<(), Error> {
        let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
        fchown(borrowed, Some(euid()), None).map_err(|_| Error::Unavailable)
    }

    fn fsync_dir(fd: libc::c_int, path: &Path) -> Result<(), Error> {
        let rc = unsafe { libc::fsync(fd) };
        if rc != 0 {
            Err(Error::io(path, io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    fn euid() -> u32 {
        unsafe { libc::geteuid() }
    }

    fn is_symlink_at(parent: libc::c_int, name: &CString) -> bool {
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let rc = unsafe {
            libc::fstatat(
                parent,
                name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        rc == 0 && (stat.st_mode & libc::S_IFMT) == libc::S_IFLNK
    }

    fn c_name(name: &str) -> Result<CString, Error> {
        CString::new(name).map_err(|_| Error::unsafe_path("invalid file name"))
    }

    fn c_os(name: &OsStr) -> Result<CString, Error> {
        CString::new(name.as_bytes())
            .map_err(|_| Error::unsafe_path("invalid path component"))
    }

    fn nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    }
}

#[cfg(unix)]
pub(crate) use unix::{open_pairing_dir, ProtectedDir};
