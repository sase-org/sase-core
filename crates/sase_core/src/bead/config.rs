//! `sase bead` config JSON handling.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use super::wire::BeadError;

fn empty_string() -> String {
    String::new()
}

fn one() -> u64 {
    1
}

fn deserialize_string_default_empty<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadConfigWire {
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub issue_prefix: String,
    #[serde(default = "one")]
    pub next_counter: u64,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub owner: String,
}

pub fn default_config(
    issue_prefix: impl Into<String>,
    owner: impl Into<String>,
) -> BeadConfigWire {
    BeadConfigWire {
        issue_prefix: issue_prefix.into(),
        next_counter: 1,
        owner: owner.into(),
    }
}

pub fn load_config_from_str(input: &str) -> Result<BeadConfigWire, BeadError> {
    Ok(serde_json::from_str(input)?)
}

pub fn load_config(
    beads_dir: &Path,
    fallback: BeadConfigWire,
) -> Result<BeadConfigWire, BeadError> {
    let path = beads_dir.join("config.json");
    if !path.exists() {
        return Ok(fallback);
    }
    load_config_from_str(&fs::read_to_string(path)?)
}

pub fn save_config(
    beads_dir: &Path,
    config: &BeadConfigWire,
) -> Result<(), BeadError> {
    fs::create_dir_all(beads_dir)?;
    let path = beads_dir.join("config.json");
    let mut output = serde_json::to_string_pretty(config)?;
    output.push('\n');
    fs::write(path, output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn load_current_shape() {
        let config = load_config_from_str(
            r#"{"issue_prefix":"gold","next_counter":42,"owner":"owner@example.com"}"#,
        )
        .unwrap();
        assert_eq!(
            config,
            BeadConfigWire {
                issue_prefix: "gold".to_string(),
                next_counter: 42,
                owner: "owner@example.com".to_string(),
            }
        );
    }

    #[test]
    fn missing_file_returns_supplied_default() {
        let temp = tempdir().unwrap();
        let fallback = default_config("proj", "owner@example.com");
        assert_eq!(
            load_config(temp.path(), fallback.clone()).unwrap(),
            fallback
        );
    }

    #[test]
    fn save_matches_python_pretty_json_shape() {
        let temp = tempdir().unwrap();
        let config = BeadConfigWire {
            issue_prefix: "proj".to_string(),
            next_counter: 5,
            owner: String::new(),
        };
        save_config(temp.path(), &config).unwrap();
        let raw =
            std::fs::read_to_string(temp.path().join("config.json")).unwrap();
        assert!(raw.ends_with('\n'));
        assert!(raw.contains("  \"issue_prefix\": \"proj\""));
    }
}
