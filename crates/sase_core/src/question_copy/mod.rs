//! Shared question copy metadata contract.

mod detect;
mod normalize;
mod wire;

pub use detect::detect_question_copy_candidates;
pub use normalize::{display_question_copy, normalize_question_copy};
pub use wire::{
    QuestionCopyCandidateKind, QuestionCopyCandidateWire,
    QuestionCopyDisplayWire, QuestionCopyError, QuestionCopyItemWire,
    QUESTION_COPY_CODE_INVALID, QUESTION_COPY_MAX_ENTRIES,
    QUESTION_COPY_MAX_LABEL_CHARS, QUESTION_COPY_MAX_TEXT_BYTES,
};

#[cfg(test)]
mod tests;
