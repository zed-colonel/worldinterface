//! Signed frame adapter for a host-managed authenticated streaming session.
use super::{read_bounded, IngressRuntime};
use crate::Result;
use std::io::Read;
use worldinterface_domain::ingress::IngressResult;
use worldinterface_ingress::transport::streaming::StreamSession;

/// Returns the committed contiguous offset in `IngressResult::Admitted`. The host
/// must acknowledge that prefix only after this method returns successfully.
pub fn accept_frame(
    runtime: &IngressRuntime<'_>,
    session: &StreamSession,
    frame: impl Read,
    signature: &[u8],
    now: u64,
) -> Result<IngressResult> {
    let exact = read_bounded(frame, session.profile().max_body_bytes)?;
    runtime.accept_frame(session, &exact, signature, now)
}
