use super::*;
/// HMAC-SHA256 over the exact request body, with a fixed-size signature header.
/// Provider/source/time fields are inside those bytes, never taken from unverified headers.
pub fn validate_http(
    body: &[u8],
    signature: &[u8],
    credentials: &ProviderCredentials,
    now: u64,
) -> Result<ValidatedProviderEvent> {
    if signature.len() != 32 || body.len() > credentials.profile.max_body_bytes as usize {
        return Err(error("HTTP input bounds"));
    }
    verify(&credentials.key, body, signature)?;
    let event = parse(body, &credentials.profile, now)?;
    Ok(ValidatedProviderEvent {
        event,
        profile: credentials.profile.clone(),
        provenance: TransportProvenance {
            transport: "http-hmac-sha256".to_string().try_into().map_err(error)?,
            authenticated_peer: credentials.profile.scope.account.clone(),
            assurance: "exact-byte-hmac".to_string().try_into().map_err(error)?,
            exact_bytes_digest: ContentHash::of(body),
            session: None,
            reconnect_from: None,
        },
    })
}
