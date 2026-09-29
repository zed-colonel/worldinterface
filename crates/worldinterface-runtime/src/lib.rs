//! WI-FABRIC-2 production composition. AQ remains the sole queue authority.
pub mod aq_adapter;
pub mod artifact;
pub mod ingress;
pub mod storage;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[path = "../tests/support/crash.rs"]
mod crash;
