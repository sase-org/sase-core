pub const SERVICE_PROC_MODE_DAEMON: &str = "daemon";
pub const SERVICE_PROC_MODE_ONESHOT: &str = "oneshot";
pub const SERVICE_PROC_MODES: [&str; 2] =
    [SERVICE_PROC_MODE_DAEMON, SERVICE_PROC_MODE_ONESHOT];

pub const SERVICE_PROC_SOURCE_BUILTIN: &str = "builtin";
pub const SERVICE_PROC_SOURCE_PLUGIN: &str = "plugin";
pub const SERVICE_PROC_SOURCE_USER: &str = "user";
pub const SERVICE_PROC_SOURCE_TRANSIENT: &str = "transient";
pub const SERVICE_PROC_SOURCES: [&str; 4] = [
    SERVICE_PROC_SOURCE_BUILTIN,
    SERVICE_PROC_SOURCE_PLUGIN,
    SERVICE_PROC_SOURCE_USER,
    SERVICE_PROC_SOURCE_TRANSIENT,
];

pub const RESERVED_BUILTIN_SERVICE_PROCS: [&str; 2] = ["gateway", "scheduler"];

pub fn validate_service_proc_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("service proc name must not be empty".to_string());
    }
    if name.len() > 64 {
        return Err(
            "service proc name must be no more than 64 characters".to_string()
        );
    }
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return Err("service proc name must not be empty".to_string());
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(
            "service proc name must start with a lowercase letter or digit"
                .to_string(),
        );
    }
    if !bytes.all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'_' | b'-')
    }) {
        return Err(
            "service proc name must contain only lowercase letters, digits, underscores, and hyphens"
                .to_string(),
        );
    }
    Ok(())
}

pub fn is_service_proc_mode(value: &str) -> bool {
    SERVICE_PROC_MODES.contains(&value)
}

pub fn is_service_proc_source(value: &str) -> bool {
    SERVICE_PROC_SOURCES.contains(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_proc_names_accept_path_safe_slugs() {
        for value in ["gateway", "scheduler_1", "build-agent2", "a"] {
            validate_service_proc_name(value).unwrap();
        }
    }

    #[test]
    fn service_proc_names_reject_unsafe_values() {
        for value in ["", "_bad", "Bad", "bad.name", "bad/name", "a b"] {
            assert!(validate_service_proc_name(value).is_err(), "{value}");
        }
        assert!(validate_service_proc_name(&"a".repeat(65)).is_err());
    }
}
