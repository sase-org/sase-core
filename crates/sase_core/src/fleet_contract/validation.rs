use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_MIN_READABLE_SCHEMA_VERSION;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_INSTALLATION_ID_PREFIX;
use super::error::MAX_IDENTIFIER_BYTES;
use super::error::MAX_KEY_BYTES;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

pub(crate) fn validate_allowed_fields(
    object: &Map<String, Value>,
    label: &str,
    allowed: &[&str],
) -> Result<(), FleetContractError> {
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(FleetContractError::Validation(format!(
                "{label} contains unknown field {key}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_optional_schema(
    object: &Map<String, Value>,
    label: &str,
) -> Result<(), FleetContractError> {
    let Some(value) = object.get("schema_version") else {
        return Ok(());
    };
    let Some(version) = value.as_u64() else {
        return Err(FleetContractError::Validation(format!(
            "{label} schema_version must be an unsigned integer"
        )));
    };
    let version = u32::try_from(version).map_err(|_| {
        FleetContractError::Validation(format!(
            "{label} schema_version is out of range"
        ))
    })?;
    validate_schema(label, version)
}

pub(crate) fn optional_string_field(
    object: &Map<String, Value>,
    field: &str,
    label: &str,
    max_bytes: usize,
) -> Result<Option<String>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(text) = value.as_str() else {
        return Err(FleetContractError::Validation(format!(
            "{label} must be a string"
        )));
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    validate_label(label, text, max_bytes)?;
    reject_secretish(label, text)?;
    Ok(Some(text.to_string()))
}

pub(crate) fn optional_bool_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<bool>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .ok_or_else(|| {
            FleetContractError::Validation(format!("{field} must be a boolean"))
        })
        .map(Some)
}

pub(crate) fn required_bool_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<bool, FleetContractError> {
    optional_bool_field(object, field)?.ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })
}

pub(crate) fn optional_u64_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .ok_or_else(|| {
            FleetContractError::Validation(format!(
                "{field} must be an unsigned integer"
            ))
        })
        .map(Some)
}

pub(crate) fn required_u64_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<u64, FleetContractError> {
    optional_u64_field(object, field)?.ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })
}

pub(crate) fn optional_non_negative_seconds_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<f64>, FleetContractError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(number) = value.as_f64() else {
        return Err(FleetContractError::Validation(format!(
            "{field} must be a finite number"
        )));
    };
    validate_non_negative_seconds(field, number)?;
    Ok(Some(number))
}

pub(crate) fn optional_f64_value(
    value: Option<&Value>,
    label: &str,
) -> Result<Option<f64>, FleetContractError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(number) = value.as_f64() else {
        return Err(FleetContractError::Validation(format!(
            "{label} must be a finite number"
        )));
    };
    validate_timestamp(label, number)?;
    Ok(Some(number))
}

pub(crate) fn array_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Vec<Value>, FleetContractError> {
    let value = object.get(field).ok_or_else(|| {
        FleetContractError::Validation(format!("{field} is required"))
    })?;
    value.as_array().ok_or_else(|| {
        FleetContractError::Validation(format!("{field} must be a JSON array"))
    })
}

pub(crate) fn wire_from_json_value<T: DeserializeOwned>(
    value: &Value,
    label: &str,
) -> Result<T, FleetContractError> {
    serde_json::from_value(value.clone()).map_err(|error| {
        FleetContractError::Validation(format!(
            "{label} is not a valid fleet wire value: {error}"
        ))
    })
}

pub(crate) fn length_key<'a>(
    segments: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
    let mut out = String::from("v1");
    for (name, value) in segments {
        out.push('|');
        out.push_str(name);
        out.push(':');
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(value);
    }
    out
}

pub(crate) fn validate_schema(
    label: &str,
    version: u32,
) -> Result<(), FleetContractError> {
    if !(FLEET_CONTRACT_MIN_READABLE_SCHEMA_VERSION
        ..=FLEET_CONTRACT_SCHEMA_VERSION)
        .contains(&version)
    {
        return Err(FleetContractError::Validation(format!(
            "{label} schema_version {version} is not supported (expected {FLEET_CONTRACT_MIN_READABLE_SCHEMA_VERSION}..={FLEET_CONTRACT_SCHEMA_VERSION})"
        )));
    }
    Ok(())
}

pub(crate) fn validate_installation_id(
    value: &str,
) -> Result<(), FleetContractError> {
    if !value.starts_with(FLEET_INSTALLATION_ID_PREFIX) {
        return Err(FleetContractError::Validation(format!(
            "installation_id must start with {FLEET_INSTALLATION_ID_PREFIX:?}"
        )));
    }
    let suffix = &value[FLEET_INSTALLATION_ID_PREFIX.len()..];
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(
            "installation_id must end with 64 lowercase hex characters"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_identifier(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

pub(crate) fn validate_reference_id(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    validate_identifier(field, value)?;
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'_' | b'-' | b'.' | b':')
    }) {
        return Err(FleetContractError::Validation(format!(
            "{field} must be an opaque reference identifier, not a path or inline secret"
        )));
    }
    reject_secretish(field, value)?;
    Ok(())
}

pub(crate) fn validate_key(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    if value.is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > MAX_KEY_BYTES {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

pub(crate) fn validate_label(
    field: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), FleetContractError> {
    if value.trim().is_empty() {
        return Err(FleetContractError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {max_bytes} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

pub(crate) fn validate_timestamp(
    field: &str,
    value: f64,
) -> Result<(), FleetContractError> {
    if !value.is_finite() || value < 0.0 {
        return Err(FleetContractError::Validation(format!(
            "{field} must be finite and non-negative"
        )));
    }
    Ok(())
}

pub(crate) fn validate_non_negative_seconds(
    field: &str,
    value: f64,
) -> Result<(), FleetContractError> {
    if !value.is_finite() || value < 0.0 {
        return Err(FleetContractError::Validation(format!(
            "{field} must be finite and non-negative"
        )));
    }
    Ok(())
}

pub(crate) fn validate_sha256_digest(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must be a lowercase 64-character SHA-256 digest"
        )));
    }
    Ok(())
}

pub(crate) fn validate_absolute_https_endpoint(
    value: &str,
) -> Result<(), FleetContractError> {
    validate_label("endpoint", value, MAX_KEY_BYTES)?;
    reject_secretish("endpoint", value)?;
    if value.contains('#') {
        return Err(FleetContractError::Validation(
            "endpoint must not include a URL fragment".to_string(),
        ));
    }
    let Some(rest) = value.strip_prefix("https://") else {
        return Err(FleetContractError::Validation(
            "endpoint must be an absolute https:// URL".to_string(),
        ));
    };
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return Err(FleetContractError::Validation(
            "endpoint must include a host".to_string(),
        ));
    }
    if authority.contains('@') {
        return Err(FleetContractError::Validation(
            "endpoint must not include URL userinfo".to_string(),
        ));
    }
    if authority
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(FleetContractError::Validation(
            "endpoint host must not contain whitespace or control characters"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn reject_path_like(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    if value.contains('/')
        || value.contains('\\')
        || value.starts_with('.')
        || value.starts_with('~')
        || value.contains("://")
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must be opaque and must not look like a path or URL"
        )));
    }
    Ok(())
}

pub(crate) fn reject_secretish(
    field: &str,
    value: &str,
) -> Result<(), FleetContractError> {
    let lowercase = value.to_ascii_lowercase();
    if lowercase.contains("authorization:")
        || lowercase.contains("bearer ")
        || lowercase.contains("token=")
        || lowercase.contains("access_token")
        || lowercase.contains("password=")
        || lowercase.contains("secret=")
        || lowercase.contains("auth_header")
    {
        return Err(FleetContractError::Validation(format!(
            "{field} must not contain inline credentials or auth headers"
        )));
    }
    Ok(())
}

pub(crate) fn trim_to_limit(value: &str, max_bytes: usize) -> String {
    let value = value.trim();
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

pub(crate) fn timestamp_ms(
    field: &str,
    value: f64,
) -> Result<u64, FleetContractError> {
    validate_timestamp(field, value)?;
    let millis = value * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(FleetContractError::Validation(format!(
            "{field} is too large to represent in milliseconds"
        )));
    }
    Ok(millis.round() as u64)
}

pub(crate) fn duration_ms(
    field: &str,
    value: f64,
) -> Result<u64, FleetContractError> {
    validate_non_negative_seconds(field, value)?;
    let millis = value * 1000.0;
    if millis > u64::MAX as f64 {
        return Err(FleetContractError::Validation(format!(
            "{field} is too large to represent in milliseconds"
        )));
    }
    Ok(millis.round() as u64)
}
