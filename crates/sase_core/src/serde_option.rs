//! Shared serde helpers for "present but nullable" wire fields.
//!
//! Several wire structs use a nested `Option<Option<T>>` so callers can
//! distinguish an omitted field (`None`, "leave unchanged") from an explicit
//! JSON `null` (`Some(None)`, "clear the value"). Serde's derived
//! deserializer collapses JSON `null` onto the outer `None` by default,
//! which makes that shape asymmetric: `Some(None)` serializes to
//! `"field": null`, but decoding `"field": null` yields `None` instead of
//! `Some(None)`. Pairing `deserialize_with = "deserialize_present_option"`
//! with `skip_serializing_if = "Option::is_none"` closes that gap so the
//! encoding round-trips: omitted stays omitted, explicit `null` stays
//! explicit `null`.

use serde::{Deserialize, Deserializer};

pub fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}
