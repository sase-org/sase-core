//! Goal renderers: markdown card and one-line citation.

mod markdown;

pub use markdown::{
    goal_card_markdown, goal_citation_line, GOAL_CITATION_LINE_MAX,
};
