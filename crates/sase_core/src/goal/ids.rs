//! Goal and event ids.
//!
//! Goal ids are 5 Crockford base32 characters, minted from `OsRng`.
//! Parsing folds uppercase to lowercase and rejects anything else.
//! Event ids are 26-character ULID-shaped lowercase Crockford strings:
//! a 48-bit millisecond timestamp plus 80 random bits, monotonic
//! within a process: equal milliseconds increment the random part.

use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use thiserror::Error;

/// Crockford base32 lowercase alphabet, shared with proc ids.
pub const GOAL_ID_ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";

/// Length of a goal id in characters.
pub const GOAL_ID_LEN: usize = 5;

/// Length of an event id in characters.
pub const GOAL_EVENT_ID_LEN: usize = 26;

/// Timestamp characters at the head of an event id.
pub const GOAL_EVENT_ID_TIMESTAMP_LEN: usize = 10;

/// Random characters trailing the timestamp in an event id.
pub const GOAL_EVENT_ID_RANDOM_LEN: usize = 16;

/// Errors from goal-id and event-id parsing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GoalIdError {
    /// The value has the wrong length.
    #[error("invalid goal id length {found}: want {want}")]
    InvalidLength {
        /// Characters found in the value.
        found: usize,
        /// Characters the id kind requires.
        want: usize,
    },
    /// The value holds a character outside the alphabet.
    #[error("invalid character {character:?} in id")]
    InvalidChar {
        /// The offending character.
        character: char,
    },
    /// The value is empty.
    #[error("id is empty")]
    Empty,
}

/// Mint a random goal id from `OsRng`.
pub fn mint_goal_id() -> String {
    let mut bytes = [0u8; GOAL_ID_LEN];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| {
            GOAL_ID_ALPHABET.as_bytes()[(usize::from(*byte) * 32) / 256] as char
        })
        .collect()
}

/// Parse and normalize a goal id.
///
/// Uppercase folds to lowercase; anything outside the alphabet is
/// rejected.
pub fn parse_goal_id(value: &str) -> Result<String, GoalIdError> {
    let folded: String = value
        .chars()
        .flat_map(|char| {
            if char.is_ascii_uppercase() {
                char.to_lowercase().collect::<Vec<_>>()
            } else {
                vec![char]
            }
        })
        .collect();
    if folded.is_empty() {
        return Err(GoalIdError::Empty);
    }
    if folded.chars().count() != GOAL_ID_LEN {
        return Err(GoalIdError::InvalidLength {
            found: folded.chars().count(),
            want: GOAL_ID_LEN,
        });
    }
    for char in folded.chars() {
        if !GOAL_ID_ALPHABET.contains(char) {
            return Err(GoalIdError::InvalidChar { character: char });
        }
    }
    Ok(folded)
}

/// Parse and normalize an event id.
pub fn parse_event_id(value: &str) -> Result<String, GoalIdError> {
    let folded = value.to_ascii_lowercase();
    if folded.is_empty() {
        return Err(GoalIdError::Empty);
    }
    if folded.chars().count() != GOAL_EVENT_ID_LEN {
        return Err(GoalIdError::InvalidLength {
            found: folded.chars().count(),
            want: GOAL_EVENT_ID_LEN,
        });
    }
    for char in folded.chars() {
        if !GOAL_ID_ALPHABET.contains(char) {
            return Err(GoalIdError::InvalidChar { character: char });
        }
    }
    Ok(folded)
}

/// Read the millisecond timestamp carried by an event id.
pub fn event_id_timestamp_ms(event_id: &str) -> Option<u64> {
    if event_id.len() != GOAL_EVENT_ID_LEN {
        return None;
    }
    let mut timestamp: u64 = 0;
    for char in event_id.chars().take(GOAL_EVENT_ID_TIMESTAMP_LEN) {
        let digit = GOAL_ID_ALPHABET.find(char)? as u64;
        timestamp = timestamp * 32 + digit;
    }
    Some(timestamp)
}

fn encode_crockford(value: u64, width: usize) -> String {
    let mut digits = vec!['0'; width];
    let mut rest = value;
    for slot in (0..width).rev() {
        digits[slot] =
            GOAL_ID_ALPHABET.as_bytes()[(rest & 31) as usize] as char;
        rest >>= 5;
    }
    digits.into_iter().collect()
}

fn encode_random(random: &[u8; 10]) -> String {
    let mut out = String::with_capacity(GOAL_EVENT_ID_RANDOM_LEN);
    let mut carry: u32 = 0;
    let mut bits: u32 = 0;
    for byte in random {
        carry = (carry << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(
                GOAL_ID_ALPHABET.as_bytes()[((carry >> bits) & 31) as usize]
                    as char,
            );
        }
    }
    if bits > 0 {
        out.push(
            GOAL_ID_ALPHABET.as_bytes()[((carry << (5 - bits)) & 31) as usize]
                as char,
        );
    }
    out
}

/// Mint an event id for an explicit timestamp and random part.
///
/// This pure helper backs [`mint_event_id`] and lets tests build
/// deterministic fixtures.
pub fn mint_event_id_with(timestamp_ms: u64, random: &[u8; 10]) -> String {
    format!(
        "{}{}",
        encode_crockford(timestamp_ms, GOAL_EVENT_ID_TIMESTAMP_LEN),
        encode_random(random)
    )
}

fn monotonic_state() -> &'static Mutex<(u64, [u8; 10])> {
    static STATE: OnceLock<Mutex<(u64, [u8; 10])>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new((0, [0u8; 10])))
}

fn increment_random(random: &mut [u8; 10]) {
    for byte in random.iter_mut().rev() {
        let (next, overflow) = byte.overflowing_add(1);
        *byte = next;
        if !overflow {
            return;
        }
    }
}

/// Mint a ULID-shaped event id, monotonic within this process.
///
/// Equal milliseconds increment the random part so ids minted in
/// the same millisecond still sort after earlier ones.
pub fn mint_event_id() -> String {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let mut guard = monotonic_state()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let (last_ms, last_random) = &mut *guard;
    if timestamp_ms > *last_ms {
        let mut random = [0u8; 10];
        OsRng.fill_bytes(&mut random);
        *last_ms = timestamp_ms;
        *last_random = random;
    } else {
        increment_random(last_random);
    }
    mint_event_id_with(*last_ms, last_random)
}

/// Test helper: render a fixed event id without randomness.
#[cfg(test)]
pub fn test_event_id(timestamp_ms: u64, seed: u8) -> String {
    let mut random = [0u8; 10];
    for (index, byte) in random.iter_mut().enumerate() {
        *byte = seed.wrapping_add(index as u8);
    }
    mint_event_id_with(timestamp_ms, &random)
}
