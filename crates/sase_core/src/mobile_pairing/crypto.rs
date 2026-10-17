use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256};

const ID_DOMAIN: &[u8] = b"sase.mobile.pairing.v1.id";
const SECRET_DOMAIN: &[u8] = b"sase.mobile.pairing.v1.secret";
const PAIRING_ID_BYTES: usize = 16;

pub(crate) fn generate_pairing_id() -> String {
    let mut bytes = [0_u8; PAIRING_ID_BYTES];
    OsRng.fill_bytes(&mut bytes);
    format!("pair_{}", hex::encode(bytes))
}

pub(crate) fn generate_pairing_code() -> String {
    loop {
        let mut bytes = [0_u8; 4];
        OsRng.fill_bytes(&mut bytes);
        let value = u32::from_le_bytes(bytes);
        const LIMIT: u32 = 1_000_000 * (u32::MAX / 1_000_000);
        if value < LIMIT {
            return format!("{:06}", value % 1_000_000);
        }
    }
}

pub(crate) fn id_hash(pairing_id: &str) -> String {
    domain_hash(ID_DOMAIN, &[pairing_id.as_bytes()])
}

pub(crate) fn secret_hash(pairing_id: &str, code: &str) -> String {
    domain_hash(SECRET_DOMAIN, &[pairing_id.as_bytes(), code.as_bytes()])
}

pub(crate) fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0_u8]);
    for part in parts {
        hasher.update(
            u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes(),
        );
        hasher.update(part);
        hasher.update([0_u8]);
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_id_has_at_least_128_bits() {
        let id = generate_pairing_id();
        assert!(id.starts_with("pair_"));
        let hex_part = id.strip_prefix("pair_").unwrap();
        assert_eq!(hex_part.len(), PAIRING_ID_BYTES * 2);
        assert!(hex_part.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn pairing_code_is_six_digits() {
        let code = generate_pairing_code();
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|ch| ch.is_ascii_digit()));
    }

    #[test]
    fn secret_hash_is_domain_separated_from_id_hash() {
        let id = "pair_abc";
        let code = "123456";
        assert_ne!(id_hash(id), secret_hash(id, code));
        assert_ne!(secret_hash(id, code), secret_hash(id, "000000"));
    }
}
