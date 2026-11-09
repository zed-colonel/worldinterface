use super::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionHandshake {
    pub session: OpaqueRef,
    pub scope: ProviderScope,
    pub reconnect_from: u64,
    pub expires_at_ms: u64,
}
/// Authenticated session provenance, privately constructed. Frames are individually signed
/// over (session ID, exact frame bytes) so a frame cannot be moved to another session.
pub struct StreamSession {
    handshake: SessionHandshake,
    credentials: ProviderCredentials,
}
impl StreamSession {
    pub fn profile(&self) -> &ProviderProfile {
        &self.credentials.profile
    }
    pub fn authenticate(
        bytes: &[u8],
        signature: &[u8],
        credentials: ProviderCredentials,
        now: u64,
    ) -> Result<Self> {
        if bytes.len() > 4096 || signature.len() != 32 {
            return Err(error("session metadata bound"));
        }
        verify(&credentials.key, bytes, signature)?;
        let handshake: SessionHandshake =
            serde_json::from_value(strict_json(bytes).map_err(error)?)?;
        if handshake.scope != credentials.profile.scope
            || handshake.expires_at_ms <= now
            || handshake.expires_at_ms - now > credentials.profile.replay_window_ms
            || handshake.reconnect_from < credentials.profile.first_offset
            || handshake.reconnect_from >= 9_007_199_254_740_991
        {
            return Err(error("invalid session provenance"));
        }
        Ok(Self { handshake, credentials })
    }
    pub fn validate_stream(
        &self,
        bytes: &[u8],
        signature: &[u8],
        now: u64,
    ) -> Result<ValidatedProviderEvent> {
        if now >= self.handshake.expires_at_ms
            || bytes.len() > self.credentials.profile.max_body_bytes as usize
            || signature.len() != 32
        {
            return Err(error("expired session or frame bounds"));
        }
        let signed =
            canonical_collection(&(self.handshake.session.clone(), bytes)).map_err(error)?;
        verify(&self.credentials.key, &signed, signature)?;
        let event = parse(bytes, &self.credentials.profile, now)?;
        if event.offset.is_none() {
            return Err(error("stream frame needs offset"));
        }
        Ok(ValidatedProviderEvent {
            event,
            profile: self.credentials.profile.clone(),
            provenance: TransportProvenance {
                transport: "stream-hmac-sha256".to_string().try_into().map_err(error)?,
                authenticated_peer: self.credentials.profile.scope.account.clone(),
                assurance: "authenticated-session-and-frame"
                    .to_string()
                    .try_into()
                    .map_err(error)?,
                exact_bytes_digest: ContentHash::of(bytes),
                session: Some(self.handshake.session.clone()),
                reconnect_from: Some(self.handshake.reconnect_from),
            },
        })
    }
}
