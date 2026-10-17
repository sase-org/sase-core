//! Owner-only mobile pairing challenges.
//!
//! Challenges are issued through a local, filesystem-protected store and
//! redeemed later over HTTP. The store never exposes raw codes, and HTTP
//! callers cannot create challenges.

mod crypto;
mod error;
mod fs;
mod store;
mod trust;
mod wire;

#[cfg(test)]
mod tests;

pub use error::MobilePairingError;
pub use wire::{
    MobilePairingConsumeOutcome, MobilePairingConsumeRequestWire,
    MobilePairingIssueRequestWire, MobilePairingIssueResponseWire,
    MOBILE_PAIRING_ISSUANCE, MOBILE_PAIRING_TTL,
    MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
};

use chrono::{DateTime, Utc};

use error::MobilePairingError as Error;
use store::PairingStore;
use wire::MobilePairingConsumeOutcome as Outcome;

/// Issue one short-lived pairing challenge into the owner-protected store.
pub fn issue_mobile_pairing_challenge(
    request: &MobilePairingIssueRequestWire,
    now: DateTime<Utc>,
) -> Result<MobilePairingIssueResponseWire, Error> {
    validate_schema(request.schema_version)?;
    let store = PairingStore::open(request.state_root.as_deref(), true)?;
    store.issue(request.host_label.as_deref(), now)
}

/// Issue a pairing challenge using the current UTC time.
pub fn issue_mobile_pairing_challenge_now(
    request: &MobilePairingIssueRequestWire,
) -> Result<MobilePairingIssueResponseWire, Error> {
    issue_mobile_pairing_challenge(request, Utc::now())
}

/// Atomically consume a pairing challenge by ID.
///
/// A matching ID is removed even when the code is wrong or the challenge has
/// expired. Missing store files yield [`Outcome::Rejected`], not an empty
/// writable store.
pub fn consume_mobile_pairing_challenge(
    request: &MobilePairingConsumeRequestWire,
    now: DateTime<Utc>,
) -> Result<Outcome, Error> {
    validate_schema(request.schema_version)?;
    if request.pairing_id.is_empty() || request.code.is_empty() {
        return Ok(Outcome::Rejected);
    }
    let store = match PairingStore::open(request.state_root.as_deref(), false) {
        Ok(store) => store,
        Err(Error::MissingStore) => return Ok(Outcome::Rejected),
        Err(error) => return Err(error),
    };
    store.consume(&request.pairing_id, &request.code, now)
}

/// Consume a pairing challenge using the current UTC time.
pub fn consume_mobile_pairing_challenge_now(
    request: &MobilePairingConsumeRequestWire,
) -> Result<Outcome, Error> {
    consume_mobile_pairing_challenge(request, Utc::now())
}

fn validate_schema(found: u32) -> Result<(), Error> {
    if found == MOBILE_PAIRING_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(Error::SchemaVersion {
            found,
            expected: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
        })
    }
}
