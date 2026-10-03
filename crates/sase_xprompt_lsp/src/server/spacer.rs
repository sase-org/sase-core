//! Completion-owned xprompt spacer acceptance for the language server.
//!
//! The widget tracks completion-owned trailing spaces locally; the language
//! server must do the same through standard LSP messages only (completion
//! item commands, document sync, on-type formatting, and completion). This
//! module owns the small per-document acceptance records plus the pure
//! text-validation helpers behind them. No catalog, filesystem, or provider
//! work happens on the typing path.

use lsp_types::{
    Command, CompletionResponse, CompletionTextEdit, CompletionTriggerKind,
    Position, TextEdit, Uri,
};
use sase_core::{
    editor_map_normalized_range_to_actual,
    editor_normalize_xprompt_spacer_transition,
    editor_plan_xprompt_completion_spacer_to_parentheses_edit,
    DocumentSnapshot, EditorPosition, EditorRange, XpromptCompletionSpacerWire,
};
use serde::{Deserialize, Serialize};
use tower_lsp_server::UriExt;

use super::state::{
    ConfirmedSpacer, MacroLspServer, OpenDocument, ServedSpacer,
};
use crate::lsp_convert::{to_editor_position, to_lsp_range};

/// Server-owned acceptance command attached to eligible xprompt items.
pub(super) const ACCEPT_COMMAND: &str = "sase.xpromptLsp.acceptCompletion";
/// Legacy macro spelling of the acceptance command.
pub(super) const ACCEPT_COMMAND_MACRO: &str = "sase.macroLsp.acceptCompletion";

/// Arguments echoed back through `workspace/executeCommand`.
///
/// The client executes this command after inserting the completion; its only
/// effect is to record acceptance. The later on-type response performs the
/// edit. Standard command execution runs after insertion, unlike
/// completion-item resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct AcceptArgs {
    pub(super) uri: String,
    pub(super) reference_text: String,
    pub(super) reference_start: EditorPosition,
    pub(super) spacer_start: EditorPosition,
    pub(super) has_optional_inputs: bool,
}

impl AcceptArgs {
    pub(super) fn matches_served(&self, served: &ServedSpacer) -> bool {
        self.reference_text == served.reference_text
            && self.reference_start == served.reference_start
            && self.spacer_start == served.spacer_start
            && self.has_optional_inputs == served.has_optional_inputs
    }

    pub(super) fn to_confirmed(&self) -> ConfirmedSpacer {
        ConfirmedSpacer {
            reference_text: self.reference_text.clone(),
            reference_start: self.reference_start,
            spacer_start: self.spacer_start,
            has_optional_inputs: self.has_optional_inputs,
        }
    }
}

/// Convert recorded positions to byte offsets and validate the exact
/// reference plus a single ASCII space.
///
/// Returns `(reference_byte, spacer_byte)` on success. Rejects zero-input
/// records, non-`#` references, whitespace-containing references, mismatched
/// positions, non-space spacers, and out-of-bounds positions. Excluded
/// literal/definition regions are enforced by the shared core planner at
/// on-type time, not here, so transition views stay cheap.
pub(super) fn spacer_bytes_in_text(
    text: &str,
    reference_text: &str,
    reference_start: EditorPosition,
    spacer_start: EditorPosition,
    has_optional_inputs: bool,
) -> Option<(usize, usize)> {
    if !has_optional_inputs {
        return None;
    }
    if !reference_text.starts_with('#') || reference_text.is_empty() {
        return None;
    }
    if reference_text.chars().any(char::is_whitespace) {
        return None;
    }
    let document = DocumentSnapshot::new(text);
    let reference_byte = document.position_to_byte_offset(reference_start)?;
    let spacer_byte = document.position_to_byte_offset(spacer_start)?;
    let reference_end = reference_byte.checked_add(reference_text.len())?;
    if spacer_byte != reference_end {
        return None;
    }
    if text.get(reference_byte..reference_end)? != reference_text {
        return None;
    }
    if text.as_bytes().get(spacer_byte)? != &b' ' {
        return None;
    }
    Some((reference_byte, spacer_byte))
}

/// Return the spacer byte when `text` still contains the owned space.
pub(super) fn owned_spacer_byte(
    text: &str,
    confirmed: &ConfirmedSpacer,
) -> Option<usize> {
    let (_, spacer) = spacer_bytes_in_text(
        text,
        &confirmed.reference_text,
        confirmed.reference_start,
        confirmed.spacer_start,
        confirmed.has_optional_inputs,
    )?;
    Some(spacer)
}

/// Return true when `text` is the normalized form: the owned space is gone
/// and an opener follows the reference directly.
///
/// This is the `didChange` the client sends after applying the on-type
/// deletion. The opener, closer, and suffix are preserved by the client;
/// this branch never inserts them.
pub(super) fn is_normalized_after_deletion(
    text: &str,
    confirmed: &ConfirmedSpacer,
) -> bool {
    let document = DocumentSnapshot::new(text);
    let Ok(reference_byte) = document
        .position_to_byte_offset(confirmed.reference_start)
        .ok_or(())
        .and_then(|byte| {
            let end =
                byte.checked_add(confirmed.reference_text.len()).ok_or(())?;
            if text.get(byte..end).ok_or(())?
                != confirmed.reference_text.as_str()
            {
                return Err(());
            }
            Ok(end)
        })
    else {
        return false;
    };
    // Reference is intact; the owned space must be gone and `(` must follow
    // directly. Any other shape (space still present, different punctuation,
    // missing opener) is not the deletion acknowledgement.
    match text.as_bytes().get(reference_byte) {
        Some(b'(') => true,
        _ => false,
    }
}

/// UTF-16 length of an ASCII-heavy reference (references are ASCII names).
pub(super) fn utf16_len(text: &str) -> u32 {
    text.chars().map(|ch| ch.len_utf16() as u32).sum()
}

/// Return the spacer byte when `text` is a confirmed pending transition:
/// the owned space is still present and an opener follows it.
///
/// Covers `#optional (` and `#optional ()` (plus any suffix). The opener,
/// closer, and suffix are preserved by the caller; this only detects the
/// shape. Returns `None` for ordinary prose that merely resembles the
/// transition without a confirmed acceptance.
pub(super) fn transition_spacer_byte(
    text: &str,
    confirmed: &ConfirmedSpacer,
) -> Option<usize> {
    let (_, spacer) = spacer_bytes_in_text(
        text,
        &confirmed.reference_text,
        confirmed.reference_start,
        confirmed.spacer_start,
        confirmed.has_optional_inputs,
    )?;
    // Opener must follow the owned space directly. Any suffix after the
    // opener (closer, arguments, prose) is preserved, not validated here.
    if text.as_bytes().get(spacer.checked_add(1)?)? != &b'(' {
        return None;
    }
    Some(spacer)
}

impl MacroLspServer {
    /// Attach acceptance commands to eligible xprompt snippet items.
    ///
    /// Eligible items insert a trailing spacer for optional-only entries
    /// (`#optional `). Zero-input and non-spacer items never receive a
    /// command. Records the served set (replacing any previous completion)
    /// so a later `executeCommand` can prove acceptance. Text-only helpers
    /// bypass this path and never manufacture acceptance.
    pub(super) async fn attach_spacer_acceptance(
        &self,
        uri: &Uri,
        document: &OpenDocument,
        response: &mut CompletionResponse,
    ) {
        let items = match response {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => &mut list.items,
        };
        if items.is_empty() {
            return;
        }
        // Only xprompt snippet completions with a trailing spacer are
        // eligible. Other surfaces (argument names, paths, agents) pass
        // through untouched.
        let mut eligible_indices = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let Some(CompletionTextEdit::Edit(edit)) = item.text_edit.as_ref()
            else {
                continue;
            };
            if !edit.new_text.ends_with(' ') {
                continue;
            }
            if !item.label.starts_with('#') {
                continue;
            }
            // Exact spacer shape: label plus one ASCII space. Colon,
            // double-colon, and `($0)` skeletons never qualify.
            if edit.new_text != format!("{} ", item.label) {
                continue;
            }
            eligible_indices.push(index);
        }
        if eligible_indices.is_empty() {
            return;
        }
        let config = self.current_config();
        let entries = self.entries_for_completion(&config).await;
        let mut served = Vec::new();
        for index in eligible_indices {
            let label = items[index].label.clone();
            let edit_range = match items[index].text_edit.as_ref() {
                Some(CompletionTextEdit::Edit(edit)) => edit.range,
                _ => continue,
            };
            let Some(entry) =
                entries.iter().find(|entry| entry.insertion == label)
            else {
                continue;
            };
            let required =
                entry.inputs.iter().filter(|input| input.required).count();
            if required != 0 || entry.inputs.is_empty() {
                continue;
            }
            let reference_start = to_editor_position(edit_range.start);
            let replacement_start_line = edit_range.start.line;
            let replacement_end_line = edit_range.end.line;
            if replacement_start_line != replacement_end_line {
                continue;
            }
            let reference_len = utf16_len(&entry.insertion);
            let Some(spacer_character) =
                reference_start.character.checked_add(reference_len)
            else {
                continue;
            };
            let spacer_start = EditorPosition {
                line: reference_start.line,
                character: spacer_character,
            };
            let args = AcceptArgs {
                uri: uri.to_string(),
                reference_text: entry.insertion.clone(),
                reference_start,
                spacer_start,
                has_optional_inputs: true,
            };
            let Ok(value) = serde_json::to_value(&args) else {
                continue;
            };
            items[index].command = Some(Command {
                title: "Accept xprompt completion".to_string(),
                command: ACCEPT_COMMAND.to_string(),
                arguments: Some(vec![value]),
            });
            served.push(ServedSpacer {
                reference_text: entry.insertion.clone(),
                reference_start,
                spacer_start,
                has_optional_inputs: true,
                served_generation: document.generation,
            });
        }
        if served.is_empty() {
            return;
        }
        if let Ok(mut documents) = self.documents.write() {
            if let Some(stored) = documents.get_mut(&uri.to_string()) {
                stored.served_spacers = served;
            }
        }
    }

    /// Derive argument completion for a confirmed `#optional (` transition.
    ///
    /// Runs the ordinary completion route on a temporary normalized document
    /// (owned space removed), maps candidate ranges back to actual
    /// coordinates, and adds a nonoverlapping spacer deletion as an
    /// additional edit so accepting an argument before formatting still
    /// removes the space. After the deletion `didChange`, the ordinary route
    /// runs with no extra deletion. Never uses normalized coordinates against
    /// the unnormalized buffer.
    pub(super) async fn transition_argument_completion(
        &self,
        uri: &Uri,
        document: &OpenDocument,
        confirmed: &ConfirmedSpacer,
        spacer_byte: usize,
        position: Position,
        trigger_kind: Option<CompletionTriggerKind>,
        trigger_character: Option<String>,
    ) -> Option<CompletionResponse> {
        let actual_doc = DocumentSnapshot::new(document.text.clone());
        let actual_byte =
            actual_doc.position_to_byte_offset(to_editor_position(position))?;
        // Caret must sit after the owned spacer (at or after the opener).
        // Anything at or before the spacer is not the transition.
        if actual_byte <= spacer_byte {
            return None;
        }
        let normalized_text = editor_normalize_xprompt_spacer_transition(
            &document.text,
            spacer_byte,
        )?;
        let normalized_byte = actual_byte.checked_sub(1)?;
        let normalized_doc = DocumentSnapshot::new(normalized_text.clone());
        if !normalized_doc.text().is_char_boundary(normalized_byte) {
            return None;
        }
        let normalized_position =
            normalized_doc.byte_offset_to_position(normalized_byte)?;
        let normalized_lsp = Position {
            line: normalized_position.line,
            character: normalized_position.character,
        };
        let source_path = uri.to_file_path().map(|path| path.into_owned());
        let response = self
            .completion_for_document(
                normalized_text,
                normalized_lsp,
                source_path,
                &document.language_id,
                trigger_kind,
                trigger_character,
            )
            .await?;
        let items = match response {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        };
        if items.is_empty() {
            return None;
        }
        // Spacer deletion in actual coordinates, shared by every candidate.
        let spacer_end = EditorPosition {
            line: confirmed.spacer_start.line,
            character: confirmed.spacer_start.character.checked_add(1)?,
        };
        let spacer_range = EditorRange {
            start: confirmed.spacer_start,
            end: spacer_end,
        };
        let spacer_edit = TextEdit {
            range: to_lsp_range(spacer_range),
            new_text: String::new(),
        };
        let mut mapped = Vec::with_capacity(items.len());
        for mut item in items {
            // Map the primary edit back; skip candidates that cannot be
            // represented in actual coordinates rather than leaking
            // normalized positions.
            if let Some(CompletionTextEdit::Edit(edit)) =
                item.text_edit.as_mut()
            {
                let normalized_range = EditorRange {
                    start: to_editor_position(edit.range.start),
                    end: to_editor_position(edit.range.end),
                };
                let Some(actual_range) = editor_map_normalized_range_to_actual(
                    &normalized_doc,
                    &actual_doc,
                    spacer_byte,
                    normalized_range,
                ) else {
                    continue;
                };
                edit.range = to_lsp_range(actual_range);
            }
            if let Some(edits) = item.additional_text_edits.as_mut() {
                let mut ok = true;
                for edit in edits.iter_mut() {
                    let normalized_range = EditorRange {
                        start: to_editor_position(edit.range.start),
                        end: to_editor_position(edit.range.end),
                    };
                    match editor_map_normalized_range_to_actual(
                        &normalized_doc,
                        &actual_doc,
                        spacer_byte,
                        normalized_range,
                    ) {
                        Some(actual_range) => {
                            edit.range = to_lsp_range(actual_range);
                        }
                        None => {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    continue;
                }
            }
            // Standard, nonoverlapping additional edit: deleting the owned
            // spacer sits before the paren while the primary argument edit
            // sits inside it.
            match item.additional_text_edits.as_mut() {
                Some(edits) => edits.push(spacer_edit.clone()),
                None => {
                    item.additional_text_edits =
                        Some(vec![spacer_edit.clone()]);
                }
            }
            // Argument candidates never carry acceptance commands.
            item.command = None;
            mapped.push(item);
        }
        if mapped.is_empty() {
            return None;
        }
        Some(CompletionResponse::Array(mapped))
    }

    /// Record acceptance from `workspace/executeCommand`.
    ///
    /// The command's only effect is to record acceptance; the later on-type
    /// response performs the edit. Validates against a served item and the
    /// expected document transition. Accounts for command delivery before or
    /// after the acceptance `didChange`, and for a coalesced acceptance plus
    /// `(`/`()` change. Rejects stale commands after unrelated changes or
    /// document close/reopen. Never infers acceptance from merely seeing
    /// `#optional ` in the buffer.
    pub(super) fn handle_accept_command(
        &self,
        value: &serde_json::Value,
    ) -> bool {
        let Ok(args) = serde_json::from_value::<AcceptArgs>(value.clone())
        else {
            return false;
        };
        if args.uri.is_empty() {
            return false;
        }
        let uri_string = args.uri.clone();
        let Some(document) = self
            .documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri_string).cloned())
        else {
            return false;
        };
        let Some(served) = document
            .served_spacers
            .iter()
            .find(|served| args.matches_served(served))
            .cloned()
        else {
            return false;
        };
        // Stale after unrelated changes: only the serving generation (command
        // before `didChange`) or the next one (command after acceptance
        // `didChange`, possibly coalesced with `(`/`()`) is admissible.
        let current = document.generation;
        if current != served.served_generation
            && current != served.served_generation.wrapping_add(1)
        {
            return false;
        }
        let confirmed = args.to_confirmed();
        // Command after `didChange` (including coalesced acceptance plus
        // opener): the current text already contains the owned spacer, so
        // confirm immediately. Command before `didChange`: the current text
        // is still pre-acceptance, so park as pending until the change
        // arrives and `changed_document` promotes it.
        if owned_spacer_byte(&document.text, &confirmed).is_some() {
            if let Ok(mut documents) = self.documents.write() {
                if let Some(stored) = documents.get_mut(&uri_string) {
                    // Re-check generation under the write lock; another
                    // change may have landed between the read above and now.
                    if stored.generation != current {
                        return false;
                    }
                    stored.confirmed_spacer = Some(confirmed);
                    stored.pending_spacer = None;
                    return true;
                }
            }
            return false;
        }
        // Before-change: only park when we are still on the serving snapshot.
        // A later generation means an unrelated change already invalidated it.
        if current != served.served_generation {
            return false;
        }
        if let Ok(mut documents) = self.documents.write() {
            if let Some(stored) = documents.get_mut(&uri_string) {
                if stored.generation != current {
                    return false;
                }
                // Only one pending acceptance is tracked; the latest command
                // wins and earlier ones are superseded.
                stored.pending_spacer = Some(served);
                return true;
            }
        }
        false
    }

    /// On-type deletion for a confirmed owned spacer before `(`.
    ///
    /// Uses the shared core planner against the pre-insertion document so
    /// excluded literal/definition regions, UTF-16 positions, and stale text
    /// all reject. Preserves the typed opener, any editor-inserted closer,
    /// and every suffix character; this branch never inserts a closer.
    /// Repeated requests on one unchanged snapshot return consistent
    /// results; the state is discarded only by the normalizing `didChange`.
    pub(super) fn spacer_on_type(
        &self,
        document: &OpenDocument,
        position: Position,
        ch: &str,
    ) -> Option<Vec<TextEdit>> {
        if ch != "(" {
            return None;
        }
        let confirmed = document.confirmed_spacer.clone()?;
        let text = document.text.as_str();
        let actual_doc = DocumentSnapshot::new(text);
        let cursor =
            actual_doc.position_to_byte_offset(to_editor_position(position))?;
        let (opener_idx, after_opener_idx) =
            if text.as_bytes().get(cursor) == Some(&b'(') {
                (cursor, cursor.checked_add(1)?)
            } else {
                let opener_idx = cursor.checked_sub(1)?;
                if text.as_bytes().get(opener_idx) != Some(&b'(') {
                    return None;
                }
                (opener_idx, cursor)
            };
        let mut pre_insert_text = String::with_capacity(text.len());
        pre_insert_text.push_str(text.get(..opener_idx)?);
        pre_insert_text.push_str(text.get(after_opener_idx..)?);
        let pre_insert_doc = DocumentSnapshot::new(pre_insert_text);
        let pre_insert_position =
            pre_insert_doc.byte_offset_to_position(opener_idx)?;
        let record = XpromptCompletionSpacerWire {
            reference_text: confirmed.reference_text.clone(),
            reference_start: confirmed.reference_start,
            spacer_start: confirmed.spacer_start,
            has_optional_inputs: confirmed.has_optional_inputs,
        };
        let edit = editor_plan_xprompt_completion_spacer_to_parentheses_edit(
            &pre_insert_doc,
            pre_insert_position,
            &record,
        )?;
        Some(vec![TextEdit {
            range: to_lsp_range(edit.range),
            new_text: edit.new_text,
        }])
    }
}
