use crate::{mapping::WorkerRequestV1, Result};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Acknowledgment {
    Accepted,
    Uncertain,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub provider: String,
    pub run: String,
    pub event: String,
    pub sequence: u64,
    pub kind: ObservationKind,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    Provisioned,
    Running,
    Exited,
    ArtifactPublished,
    Released,
    Quarantined,
}
/// Implementations in this slice live only in tests. Handles are opaque.
pub trait WorkerPort: Send + Sync {
    fn submit<'a>(&'a self, request: &'a WorkerRequestV1) -> PortFuture<'a, Acknowledgment>;
    fn observe<'a>(&'a self, provider: &'a str, run: &'a str) -> PortFuture<'a, Vec<Observation>>;
    fn read_artifact<'a>(&'a self, reference: &'a str) -> PortFuture<'a, Vec<u8>>;
}
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub fn frame(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err("frame exceeds bound".into());
    }
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(1);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}
pub fn unframe(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 5 || bytes[0] != 1 {
        return Err("unsupported frame".into());
    }
    let len = u32::from_be_bytes(bytes[1..5].try_into().map_err(|_| "short frame")?) as usize;
    if len > MAX_FRAME_BYTES || bytes.len() != len + 5 {
        return Err("invalid frame length".into());
    }
    Ok(&bytes[5..])
}
