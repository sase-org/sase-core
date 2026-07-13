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
        let separator = if self.requires_auto_id_separator(token) {
            "-"
        } else {
            ""
        };
        Ok(format!(
            "{}{}{}{}",
            self.prefix, separator, token, self.suffix
        ))
    }

    pub fn namespace_template(&self) -> String {
        match self.suffix.find('.') {
            Some(dot_idx) => format!(
                "{}{}{}",
                self.prefix,
                AGENT_NAME_TEMPLATE_MARKER,
                &self.suffix[..dot_idx]
            ),
            None => self.template.clone(),
        }
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

        let mut token_start = self.prefix.len();
        let token_end = concrete.len() - self.suffix.len();
        let rendered_token = &concrete[token_start..token_end];
        if self.prefix_requires_separator_protection() {
            if let Some(token) = rendered_token.strip_prefix('-') {
                if !token.starts_with(|character: char| {
                    character.is_ascii_lowercase()
                }) {
                    return Ok(None);
                }
                token_start += 1;
            } else if !rendered_token
                .starts_with(|character: char| character.is_ascii_digit())
            {
                return Ok(None);
            }
        }

        let token = &concrete[token_start..token_end];
        Ok(
            is_valid_agent_name_template_token(token)
                .then(|| token.to_string()),
        )
    }

    fn requires_auto_id_separator(&self, token: &str) -> bool {
        self.prefix_requires_separator_protection()
            && token
                .starts_with(|character: char| character.is_ascii_lowercase())
    }

    fn prefix_requires_separator_protection(&self) -> bool {
        self.prefix
            .chars()
            .next_back()
            .is_some_and(|character| !matches!(character, '-' | '.'))
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

pub fn agent_name_template_namespace_template(
    template: &str,
) -> Result<String, AgentNameTemplateError> {
    Ok(parse_agent_name_template(template)?.namespace_template())
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
        let cases = [
            ("@", "0", "0"),
            ("@", "a", "a"),
            ("@.cld", "a", "a.cld"),
            ("foo.f@", "0", "foo.f0"),
            ("foo.f@", "a", "foo.f-a"),
            ("foo.f@", "0a", "foo.f0a"),
            ("foo.f@", "a0", "foo.f-a0"),
            ("foo-@", "a", "foo-a"),
            ("foo.@", "a", "foo.a"),
            ("research.@.final", "00", "research.00.final"),
        ];

        for (template, token, concrete) in cases {
            assert_eq!(
                render_agent_name_template(template, token).unwrap(),
                concrete
            );
        }
    }

    #[test]
    fn derives_namespace_template_shapes() {
        assert_eq!(agent_name_template_namespace_template("@").unwrap(), "@");
        assert_eq!(
            agent_name_template_namespace_template("@.cld").unwrap(),
            "@"
        );
        assert_eq!(
            agent_name_template_namespace_template("foo-@").unwrap(),
            "foo-@"
        );
        assert_eq!(
            agent_name_template_namespace_template("foo.@.bar").unwrap(),
            "foo.@"
        );
        assert_eq!(
            agent_name_template_namespace_template("foo.@x.bar").unwrap(),
            "foo.@x"
        );
    }

    #[test]
    fn matches_template_tokens() {
        let matching_cases = [
            ("@", "0", "0"),
            ("@", "a", "a"),
            ("@.cld", "00", "00.cld"),
            ("@.cld", "a", "a.cld"),
            ("foo.f@", "0", "foo.f0"),
            ("foo.f@", "a", "foo.f-a"),
            ("foo.f@", "0a", "foo.f0a"),
            ("foo.f@", "a0", "foo.f-a0"),
            ("foo-@", "a", "foo-a"),
            ("foo.@", "a", "foo.a"),
        ];

        for (template, token, concrete) in matching_cases {
            assert_eq!(
                match_agent_name_template(template, concrete).unwrap(),
                Some(token.to_string())
            );
        }

        let rejected_cases = [
            ("foo.f@", "foo.fa"),
            ("foo.f@", "foo.f-0"),
            ("foo.f@", "foo.f--a"),
            ("foo.f@", "other.f0"),
            ("foo-@", "foo--a"),
            ("foo.@", "foo.-a"),
            ("@", "-a"),
            ("@", "not.auto"),
        ];

        for (template, concrete) in rejected_cases {
            assert_eq!(
                match_agent_name_template(template, concrete).unwrap(),
                None
            );
        }
    }

    #[test]
    fn render_and_match_are_exact_inverses() {
        let templates = ["@", "@.cld", "foo.f@", "foo-@", "foo.@"];
        let tokens = ["0", "9", "a", "z", "0a", "a0", "00"];

        for template in templates {
            for token in tokens {
                let concrete =
                    render_agent_name_template(template, token).unwrap();
                assert_eq!(
                    match_agent_name_template(template, &concrete).unwrap(),
                    Some(token.to_string())
                );
            }
        }
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
