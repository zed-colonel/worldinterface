//! WI-FABRIC-2 G1 production composition. AQ remains the sole queue authority.
//! Dispatch/admission/wake adapters belong to G2–G4, not this boundary.
pub mod aq_adapter;
pub mod artifact;
pub mod storage;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[path = "../tests/support/crash.rs"]
mod crash;
