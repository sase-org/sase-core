//! Shared question copy metadata contract.

mod normalize;
mod wire;

pub use normalize::{display_question_copy, normalize_question_copy};
pub use wire::{
    QuestionCopyDisplayWire, QuestionCopyError, QuestionCopyItemWire,
    QUESTION_COPY_CODE_INVALID, QUESTION_COPY_MAX_ENTRIES,
    QUESTION_COPY_MAX_LABEL_CHARS, QUESTION_COPY_MAX_TEXT_BYTES,
};

#[cfg(test)]
mod tests;
