//! Agent-name template primitives.
//!
//! A template contains exactly one `@` marker. Rendering replaces that
//! marker with a token from the shared auto-name sequence:
//! `0, 1, ..., 9, a, ..., z, 00, 01, ...`.

use std::cmp::Ordering;

use thiserror::Error;

pub const AGENT_NAME_TEMPLATE_MARKER: char = '@';
pub const AGENT_NAME_TEMPLATE_ALPHABET: &str =
    "0123456789abcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNameTemplate {
    pub template: String,
    pub prefix: String,
    pub suffix: String,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AgentNameTemplateError {
    #[error(
        "Invalid agent name template '{template}': expected exactly one '@' marker"
    )]
    InvalidMarkerCount { template: String },

    #[error(
        "Invalid agent name template token '{token}': token must be non-empty and contain only 0-9 or a-z"
    )]
    InvalidToken { token: String },
}

impl AgentNameTemplate {
    pub fn parse(template: &str) -> Result<Self, AgentNameTemplateError> {
        if template.matches(AGENT_NAME_TEMPLATE_MARKER).count() != 1 {
            return Err(AgentNameTemplateError::InvalidMarkerCount {
                template: template.to_string(),
            });
        }

        let marker_idx = template
            .find(AGENT_NAME_TEMPLATE_MARKER)
            .expect("marker count was checked before locating the marker");
        let marker_len = AGENT_NAME_TEMPLATE_MARKER.len_utf8();
        Ok(Self {
            template: template.to_string(),
            prefix: template[..marker_idx].to_string(),
            suffix: template[marker_idx + marker_len..].to_string(),
        })
    }

    pub fn render(
        &self,
        token: &str,
    ) -> Result<String, AgentNameTemplateError> {
        validate_agent_name_template_token(token)?;
        Ok(format!("{}{}{}", self.prefix, token, self.suffix))
    }

    pub fn match_token(
        &self,
        concrete: &str,
    ) -> Result<Option<String>, AgentNameTemplateError> {
        if !concrete.starts_with(&self.prefix)
            || !concrete.ends_with(&self.suffix)
            || concrete.len() < self.prefix.len() + self.suffix.len()
        {
            return Ok(None);
        }

        let token_start = self.prefix.len();
        let token_end = concrete.len() - self.suffix.len();
        let token = &concrete[token_start..token_end];
        if is_valid_agent_name_template_token(token) {
            Ok(Some(token.to_string()))
        } else {
            Ok(None)
        }
    }
}

pub fn is_agent_name_template(value: &str) -> bool {
    value.matches(AGENT_NAME_TEMPLATE_MARKER).count() == 1
}

pub fn parse_agent_name_template(
    template: &str,
) -> Result<AgentNameTemplate, AgentNameTemplateError> {
    AgentNameTemplate::parse(template)
}

pub fn render_agent_name_template(
    template: &str,
    token: &str,
) -> Result<String, AgentNameTemplateError> {
    parse_agent_name_template(template)?.render(token)
}

pub fn match_agent_name_template(
    template: &str,
    concrete: &str,
) -> Result<Option<String>, AgentNameTemplateError> {
    parse_agent_name_template(template)?.match_token(concrete)
}

pub fn validate_agent_name_template_token(
    token: &str,
) -> Result<(), AgentNameTemplateError> {
    if is_valid_agent_name_template_token(token) {
        Ok(())
    } else {
        Err(AgentNameTemplateError::InvalidToken {
            token: token.to_string(),
        })
    }
}

pub fn is_valid_agent_name_template_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
}

pub fn compare_agent_name_template_tokens(
    left: &str,
    right: &str,
) -> Result<Ordering, AgentNameTemplateError> {
    validate_agent_name_template_token(left)?;
    validate_agent_name_template_token(right)?;
    Ok(left.len().cmp(&right.len()).then_with(|| left.cmp(right)))
}

pub fn next_agent_name_template_token(
    after: Option<&str>,
) -> Result<String, AgentNameTemplateError> {
    let Some(after) = after else {
        return Ok("0".to_string());
    };
    validate_agent_name_template_token(after)?;

    let mut indices: Vec<usize> = after
        .bytes()
        .map(|byte| {
            AGENT_NAME_TEMPLATE_ALPHABET
                .as_bytes()
                .iter()
                .position(|candidate| *candidate == byte)
                .expect("token validation guarantees alphabet membership")
        })
        .collect();

    for idx in (0..indices.len()).rev() {
        if indices[idx] + 1 < AGENT_NAME_TEMPLATE_ALPHABET.len() {
            indices[idx] += 1;
            for trailing_idx in indices.iter_mut().skip(idx + 1) {
                *trailing_idx = 0;
            }
            return Ok(indices_to_token(&indices));
        }
    }

    Ok("0".repeat(indices.len() + 1))
}

pub fn agent_name_template_tokens_after(
    after: Option<&str>,
    count: usize,
) -> Result<Vec<String>, AgentNameTemplateError> {
    let mut tokens = Vec::with_capacity(count);
    let mut previous = after.map(str::to_string);
    for _ in 0..count {
        let token = next_agent_name_template_token(previous.as_deref())?;
        previous = Some(token.clone());
        tokens.push(token);
    }
    Ok(tokens)
}

fn indices_to_token(indices: &[usize]) -> String {
    let alphabet = AGENT_NAME_TEMPLATE_ALPHABET.as_bytes();
    indices.iter().map(|idx| alphabet[*idx] as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exactly_one_marker() {
        let parsed = parse_agent_name_template("research.@.final").unwrap();
        assert_eq!(parsed.prefix, "research.");
        assert_eq!(parsed.suffix, ".final");

        assert!(parse_agent_name_template("plain").is_err());
        assert!(parse_agent_name_template("too@many@markers").is_err());
    }

    #[test]
    fn renders_template_shapes() {
        assert_eq!(render_agent_name_template("@", "0").unwrap(), "0");
        assert_eq!(
            render_agent_name_template("build-@", "1").unwrap(),
            "build-1"
        );
        assert_eq!(render_agent_name_template("@.cld", "a").unwrap(), "a.cld");
        assert_eq!(
            render_agent_name_template("research.@.final", "00").unwrap(),
            "research.00.final"
        );
    }

    #[test]
    fn matches_template_tokens() {
        assert_eq!(
            match_agent_name_template("build-@", "build-1").unwrap(),
            Some("1".to_string())
        );
        assert_eq!(
            match_agent_name_template("build-@", "build-a").unwrap(),
            Some("a".to_string())
        );
        assert_eq!(
            match_agent_name_template("build-@", "other-1").unwrap(),
            None
        );
        assert_eq!(
            match_agent_name_template("@.cld", "00.cld").unwrap(),
            Some("00".to_string())
        );
        assert_eq!(match_agent_name_template("@", "not.auto").unwrap(), None);
    }

    #[test]
    fn compares_by_auto_sequence_order() {
        assert_eq!(
            compare_agent_name_template_tokens("9", "a").unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_agent_name_template_tokens("z", "00").unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_agent_name_template_tokens("09", "0a").unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_agent_name_template_tokens("10", "0z").unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn generates_shortlex_tokens() {
        assert_eq!(
            agent_name_template_tokens_after(None, 12).unwrap(),
            vec!["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b"]
        );
        assert_eq!(
            agent_name_template_tokens_after(Some("z"), 3).unwrap(),
            vec!["00", "01", "02"]
        );
        assert_eq!(next_agent_name_template_token(Some("09")).unwrap(), "0a");
        assert_eq!(next_agent_name_template_token(Some("0z")).unwrap(), "10");
    }
}
