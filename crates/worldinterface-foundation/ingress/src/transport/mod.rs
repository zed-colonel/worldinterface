//! Offline validators only: no sockets, sessions from an activated runtime, or provider calls.
pub mod http;
pub mod streaming;
use crate::{error, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use worldinterface_domain::ingress::{
    provider_artifact_scope, validate_attachments, validate_event_references, validate_profile,
};
use worldinterface_protocol::*;

/// Trusted configuration supplied by the inactive composition root. Never deserialize keys
/// or provider profiles from a request. No Debug implementation exposes key material.
pub struct ProviderCredentials {
    pub(crate) profile: ProviderProfile,
    pub(crate) key: Vec<u8>,
}
impl ProviderCredentials {
    pub fn new(profile: ProviderProfile, key: Vec<u8>) -> Result<Self> {
        validate_profile(&profile).map_err(error)?;
        if !(32..=128).contains(&key.len()) {
            return Err(error("authentication key bound"));
        }
        Ok(Self { profile, key })
    }
    pub fn profile(&self) -> &ProviderProfile {
        &self.profile
    }
}
/// Can only be created by authenticated transport validation; deliberately not Deserialize.
pub struct ValidatedProviderEvent {
    pub(crate) event: ProviderEvent,
    pub(crate) profile: ProviderProfile,
    pub(crate) provenance: TransportProvenance,
}
impl ValidatedProviderEvent {
    pub fn profile(&self) -> &ProviderProfile {
        &self.profile
    }
}
fn verify(key: &[u8], bytes: &[u8], signature: &[u8]) -> Result<()> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| error("invalid key"))?;
    mac.update(bytes);
    mac.verify_slice(signature).map_err(|_| error("authentication failed"))
}
fn parse(bytes: &[u8], profile: &ProviderProfile, now: u64) -> Result<ProviderEvent> {
    if bytes.len() > profile.max_body_bytes as usize {
        return Err(error("body exceeds bound"));
    }
    let event: ProviderEvent = serde_json::from_value(strict_json(bytes).map_err(error)?)?;
    if event.scope != profile.scope
        || event.attachments.len() > profile.max_attachments as usize
        || event.asserted_audience.len() > 64
        || event.produced_at_ms > now
        || now - event.produced_at_ms > profile.replay_window_ms
        || event.content.scope != provider_artifact_scope(&profile.scope).map_err(error)?
        || event.attachments.iter().any(|v| v.artifact.scope != event.content.scope)
    {
        return Err(error("source, tenant, replay or attachment validation failed"));
    }
    match profile.identity {
        IdentityStrategy::StableId if event.event_id.is_none() => {
            return Err(error("missing event id"))
        }
        IdentityStrategy::StreamPosition if event.offset.is_none() => {
            return Err(error("missing position"))
        }
        _ => {}
    }
    if event.offset.is_some_and(|v| v < profile.first_offset || v >= 9_007_199_254_740_991) {
        return Err(error("offset outside profile"));
    }
    validate_attachments(profile, &event).map_err(error)?;
    validate_event_references(profile, &event).map_err(error)?;
    Ok(event)
}
