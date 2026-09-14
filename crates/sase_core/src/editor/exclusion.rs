use crate::prompt_literal_zone_ranges;

/// Return byte ranges where editor typing helpers must stay inert.
///
/// This layers prompt launch literal zones with editor-definition regions that
/// are not launch syntax themselves: prompt frontmatter and Jinja tags.
pub(crate) fn excluded_literal_and_definition_ranges(
    text: &str,
) -> Vec<(usize, usize)> {
    let mut ranges = prompt_literal_zone_ranges(text);
    ranges.extend(jinja_tag_ranges(text));
    if let Some(end) = frontmatter_block_len(text) {
        ranges.push((0, end));
    }
    ranges
}

pub(crate) fn position_in_ranges(
    pos: usize,
    ranges: &[(usize, usize)],
) -> bool {
    ranges
        .iter()
        .any(|(start, end)| *start <= pos && pos < *end)
}

fn jinja_tag_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < text.len() {
        let Some((relative_start, close)) = next_jinja_tag(text, offset) else {
            break;
        };
        let start = offset + relative_start;
        let close_start = start + 2;
        let end = text
            .get(close_start..)
            .and_then(|tail| {
                tail.find(close).map(|index| close_start + index + 2)
            })
            .unwrap_or(text.len());
        ranges.push((start, end));
        offset = end.max(start + 2);
    }
    ranges
}

fn next_jinja_tag(text: &str, offset: usize) -> Option<(usize, &'static str)> {
    let tail = text.get(offset..)?;
    [("{{", "}}"), ("{%", "%}"), ("{#", "#}")]
        .into_iter()
        .filter_map(|(open, close)| tail.find(open).map(|start| (start, close)))
        .min_by_key(|(start, _)| *start)
}

fn frontmatter_block_len(text: &str) -> Option<usize> {
    let mut lines = text.split_inclusive('\n');
    let first = lines.next()?;
    if first.trim() != "---" {
        return None;
    }
    let mut consumed = first.len();
    for line in lines {
        consumed += line.len();
        if line.trim() == "---" {
            return Some(consumed);
        }
    }
    None
}
