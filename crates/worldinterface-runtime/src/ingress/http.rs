//! HTTP body adapter for a host route with trusted provider credentials.
use super::{read_bounded, IngressRuntime};
use crate::Result;
use std::io::Read;
use worldinterface_domain::ingress::IngressResult;
use worldinterface_ingress::transport::ProviderCredentials;

/// Read at most the configured limit plus one byte. The host can return success only
/// after this method has committed the observation, receipt, fan-out, and AQ obligations.
pub fn accept(
    runtime: &IngressRuntime<'_>,
    credentials: &ProviderCredentials,
    body: impl Read,
    signature: &[u8],
    now: u64,
) -> Result<IngressResult> {
    let exact = read_bounded(body, credentials.profile().max_body_bytes)?;
    runtime.accept_http(credentials, &exact, signature, now)
}
