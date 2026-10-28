//! Process environment access with thread-local overrides for tests.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::env::VarError;
use std::ffi::{OsStr, OsString};
use std::marker::PhantomData;
use std::rc::Rc;

thread_local! {
    static TEST_OVERRIDES: RefCell<Option<BTreeMap<OsString, Option<OsString>>>> = const { RefCell::new(None) };
}

/// Read a process environment variable, honoring any current thread's test
/// override first.
pub fn var(key: &str) -> Result<String, VarError> {
    var_os(key)
        .ok_or(VarError::NotPresent)
        .and_then(|value| value.into_string().map_err(VarError::NotUnicode))
}

/// Read an OS-string process environment variable, honoring any current
/// thread's test override first.
pub fn var_os(key: impl AsRef<OsStr>) -> Option<OsString> {
    let key = key.as_ref();
    let override_value = TEST_OVERRIDES.with(|overrides| {
        overrides
            .borrow()
            .as_ref()
            .and_then(|overrides| overrides.get(key).cloned())
    });
    override_value.unwrap_or_else(|| std::env::var_os(key))
}

/// Installs environment overrides visible only to the current thread.
///
/// Production reads still fall back to the real process environment for keys
/// without an override. Dropping the guard restores the previous override set.
#[doc(hidden)]
pub struct TestEnvGuard {
    previous: Option<BTreeMap<OsString, Option<OsString>>>,
    _not_send: PhantomData<Rc<()>>,
}

impl TestEnvGuard {
    /// Start an isolated override scope, preserving an enclosing scope.
    pub fn new() -> Self {
        let previous = TEST_OVERRIDES
            .with(|overrides| overrides.replace(Some(BTreeMap::new())));
        Self {
            previous,
            _not_send: PhantomData,
        }
    }

    /// Set a variable for reads on the current thread.
    pub fn set(&self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) {
        TEST_OVERRIDES.with(|overrides| {
            overrides
                .borrow_mut()
                .as_mut()
                .expect("TestEnvGuard must be active")
                .insert(
                    key.as_ref().to_os_string(),
                    Some(value.as_ref().to_os_string()),
                );
        });
    }

    /// Make a variable absent for reads on the current thread.
    pub fn remove(&self, key: impl AsRef<OsStr>) {
        TEST_OVERRIDES.with(|overrides| {
            overrides
                .borrow_mut()
                .as_mut()
                .expect("TestEnvGuard must be active")
                .insert(key.as_ref().to_os_string(), None);
        });
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        TEST_OVERRIDES.with(|overrides| {
            overrides.replace(previous);
        });
    }
}

impl Default for TestEnvGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "SASE_CORE_TEST_THREAD_LOCAL_ENV";

    #[test]
    fn overrides_are_thread_local_and_nested() {
        let real_value = std::env::var_os(KEY);
        let outer = TestEnvGuard::new();
        outer.set(KEY, "outer");
        assert_eq!(var(KEY).unwrap(), "outer");

        let other_thread_value =
            std::thread::spawn(|| var_os(KEY)).join().unwrap();
        assert_eq!(other_thread_value, real_value);

        {
            let inner = TestEnvGuard::new();
            inner.remove(KEY);
            assert!(matches!(var(KEY), Err(VarError::NotPresent)));
            inner.set(KEY, "inner");
            assert_eq!(var(KEY).unwrap(), "inner");
        }

        assert_eq!(var(KEY).unwrap(), "outer");
    }
}
