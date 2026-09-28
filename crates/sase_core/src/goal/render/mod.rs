//! Goal renderers: markdown card, one-line citation, and terminal output.
//!
//! The markdown renderer produces the card and citation line; the
//! terminal renderer consumes the presentation-neutral view models
//! from [`super::view`](crate::goal::view) for the `sase goal` CLI
//! (rows for the list, the card for `show`). Slow (`argparse`) and
//! fast (`entry.py` early dispatch) paths call the same terminal
//! renderer through the `goal_render_*` bindings, so their output is
//! byte-identical by construction.
//!
//! Status is always text plus glyph, never color alone. Color is gated
//! on the caller's `color` flag; Python passes
//! `sase.core.term_color.should_colorize`.

mod markdown;
mod terminal;

pub use markdown::{
    goal_card_markdown, goal_citation_line, GOAL_CITATION_LINE_MAX,
};
pub use terminal::{
    render_goal_card, render_goal_list, GoalRenderCardRequestWire,
    GoalRenderListRequestWire, GoalRenderTextWire,
};
