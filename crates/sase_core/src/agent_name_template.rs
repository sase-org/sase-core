//! Agent-name template primitives.
//!
//! A template contains exactly one bare `@` or keyed `{@<id>}` marker.
//! Rendering replaces that marker with a token from the shared auto-name sequence:
//! `0, 1, ..., 9, a, ..., z, 00, 01, ...`.

use std::cmp::Ordering;

use thiserror::Error;

pub const AGENT_NAME_TEMPLATE_MARKER: char = '@';
pub const AGENT_NAME_TEMPLATE_ALPHABET: &str =
    "0123456789abcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNameTemplateKey {
    pub id: String,
    pub qualified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNameTemplateMarker {
    pub start: usize,
    pub end: usize,
    pub id: Option<String>,
    pub qualified: bool,
    pub braced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNameTemplate {
    pub template: String,
    pub prefix: String,
    pub suffix: String,
    pub marker: String,
    pub key: Option<AgentNameTemplateKey>,
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
        let markers = iter_agent_name_key_markers(template);
        if markers.len() != 1 {
            return Err(AgentNameTemplateError::InvalidMarkerCount {
                template: template.to_string(),
            });
        }

        let matched = &markers[0];
        let marker = template[matched.start..matched.end].to_string();
        let key = matched.id.as_ref().map(|id| AgentNameTemplateKey {
            id: id.clone(),
            qualified: matched.qualified,
        });
        Ok(Self {
            template: template.to_string(),
            prefix: template[..matched.start].to_string(),
            suffix: template[matched.end..].to_string(),
            marker,
            key,
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
                self.marker,
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
    iter_agent_name_key_markers(value).len() == 1
}

pub fn iter_agent_name_key_markers(text: &str) -> Vec<AgentNameTemplateMarker> {
    let bytes = text.as_bytes();
    let mut markers = Vec::new();
    let mut cursor = 0;
    let mut brace_depth = 0_usize;

    while cursor < bytes.len() {
        if bytes[cursor] == b'{' && bytes.get(cursor + 1) == Some(&b'@') {
            if let Some((end, id, qualified)) =
                parse_braced_marker(bytes, cursor)
            {
                markers.push(AgentNameTemplateMarker {
                    start: cursor,
                    end,
                    id: Some(id),
                    qualified,
                    braced: true,
                });
                cursor = end;
            } else {
                // An invalid braced form is ordinary text, including any `@`
                // before its closing brace.
                brace_depth += 1;
                cursor += 2;
            }
            continue;
        }

        if bytes[cursor] == b'{' {
            brace_depth += 1;
        } else if bytes[cursor] == b'}' {
            brace_depth = brace_depth.saturating_sub(1);
        } else if bytes[cursor] == AGENT_NAME_TEMPLATE_MARKER as u8
            && brace_depth == 0
        {
            markers.push(AgentNameTemplateMarker {
                start: cursor,
                end: cursor + AGENT_NAME_TEMPLATE_MARKER.len_utf8(),
                id: None,
                qualified: false,
                braced: false,
            });
        }
        cursor += 1;
    }

    markers
}

pub fn agent_name_template_key(
    template: &str,
) -> Result<Option<AgentNameTemplateKey>, AgentNameTemplateError> {
    Ok(parse_agent_name_template(template)?.key)
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

fn parse_braced_marker(
    bytes: &[u8],
    start: usize,
) -> Option<(usize, String, bool)> {
    let id_start = start + 2;
    let mut cursor = id_start;

    if !bytes.get(cursor).is_some_and(u8::is_ascii_alphanumeric) {
        return None;
    }

    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_alphanumeric) {
            cursor += 1;
        }
        if bytes.get(cursor) != Some(&b'.') {
            break;
        }
        cursor += 1;
        if !bytes.get(cursor).is_some_and(u8::is_ascii_alphanumeric) {
            return None;
        }
    }

    let id_end = cursor;
    let qualified = bytes.get(cursor) == Some(&b'!');
    if qualified {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'}') {
        return None;
    }

    let id = std::str::from_utf8(&bytes[id_start..id_end])
        .expect("agent-name key ids contain only ASCII")
        .to_string();
    Some((cursor + 1, id, qualified))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exactly_one_marker() {
        let parsed = parse_agent_name_template("research.@.final").unwrap();
        assert_eq!(parsed.prefix, "research.");
        assert_eq!(parsed.suffix, ".final");
        assert_eq!(parsed.marker, "@");
        assert_eq!(parsed.key, None);

        assert!(parse_agent_name_template("plain").is_err());
        assert!(parse_agent_name_template("too@many@markers").is_err());
    }

    #[test]
    fn parses_keyed_markers() {
        let cases = [
            ("research.{@1}.cdx", "research.", ".cdx", "1", false),
            ("{@a}", "", "", "a", false),
            ("foo.{@lead.a}", "foo.", "", "lead.a", false),
            ("research.{@x!}", "research.", "", "x", true),
        ];

        for (template, prefix, suffix, id, qualified) in cases {
            let parsed = parse_agent_name_template(template).unwrap();
            assert_eq!(parsed.prefix, prefix);
            assert_eq!(parsed.suffix, suffix);
            assert_eq!(
                parsed.marker,
                template[prefix.len()..template.len() - suffix.len()]
            );
            assert_eq!(
                parsed.key,
                Some(AgentNameTemplateKey {
                    id: id.to_string(),
                    qualified,
                })
            );
            assert_eq!(agent_name_template_key(template).unwrap(), parsed.key);
        }
    }

    #[test]
    fn rejects_invalid_or_multiple_markers() {
        for template in ["{@1}.{@2}", "a@b{@1}", "{@}", "{@-bad}"] {
            assert_eq!(
                parse_agent_name_template(template),
                Err(AgentNameTemplateError::InvalidMarkerCount {
                    template: template.to_string(),
                })
            );
        }
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
        assert_eq!(
            agent_name_template_namespace_template("research.{@1}.cdx")
                .unwrap(),
            "research.{@1}"
        );
        assert_eq!(
            agent_name_template_namespace_template("foo.{@1!}x.bar").unwrap(),
            "foo.{@1!}x"
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
        let templates = [
            "@",
            "@.cld",
            "foo.f@",
            "foo-@",
            "foo.@",
            "{@1}",
            "{@1}.cld",
            "foo.f{@1}",
            "foo-{@1}",
            "foo.{@1}",
            "foo.{@qualified!}.cld",
        ];
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
    fn scans_markers_with_round_trip_byte_spans() {
        let text = "α prose `research.{@lead.a}.cdx`, then @ and {@shared!}.";
        let markers = iter_agent_name_key_markers(text);
        let slices: Vec<&str> = markers
            .iter()
            .map(|marker| &text[marker.start..marker.end])
            .collect();
        assert_eq!(slices, ["{@lead.a}", "@", "{@shared!}"]);
        assert_eq!(
            markers,
            vec![
                AgentNameTemplateMarker {
                    start: text.find("{@lead.a}").unwrap(),
                    end: text.find("{@lead.a}").unwrap() + "{@lead.a}".len(),
                    id: Some("lead.a".to_string()),
                    qualified: false,
                    braced: true,
                },
                AgentNameTemplateMarker {
                    start: text.find("then @").unwrap() + "then ".len(),
                    end: text.find("then @").unwrap() + "then @".len(),
                    id: None,
                    qualified: false,
                    braced: false,
                },
                AgentNameTemplateMarker {
                    start: text.find("{@shared!}").unwrap(),
                    end: text.find("{@shared!}").unwrap() + "{@shared!}".len(),
                    id: Some("shared".to_string()),
                    qualified: true,
                    braced: true,
                },
            ]
        );
    }

    #[test]
    fn scanner_ignores_invalid_braced_and_jinja_forms() {
        let text = "{@} { @1 } {@-bad} {{ prompt }}";
        assert!(iter_agent_name_key_markers(text).is_empty());
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
