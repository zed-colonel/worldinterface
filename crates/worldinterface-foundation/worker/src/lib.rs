//! Inactive WI-owned worker boundary. Validation of these declarations grants no authority.
pub mod artifacts;
pub mod controls;
pub mod credential;
pub mod evidence;
pub mod lifecycle;
pub mod mapping;
pub mod ports;
pub mod process_state;
pub mod profile;

use serde::{de::DeserializeOwned, Serialize};
use worldinterface_protocol::{canonical_collection, strict_collection};

pub type Result<T> = std::result::Result<T, String>;
pub const MAX_ARTIFACT_BYTES: usize = 1024 * 1024;

pub fn decode<T: DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err("worker artifact exceeds bound".into());
    }
    let value = strict_collection(bytes)?;
    let decoded: T = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    if serde_json::to_value(&decoded).map_err(|e| e.to_string())? != value {
        return Err("non-schema worker artifact".into());
    }
    Ok(decoded)
}
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = canonical_collection(value)?;
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err("worker artifact exceeds bound".into());
    }
    Ok(bytes)
}

pub fn nonempty(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err("invalid opaque reference".into())
    } else {
        Ok(())
    }
}
