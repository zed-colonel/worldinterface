//! WI-FABRIC-2 production composition. AQ remains the sole queue authority.
pub mod admission;
pub mod aq_adapter;
pub mod artifact;
pub mod authority;
pub mod completion;
pub mod egress;
pub mod ingress;
pub mod inspection;
pub mod resolution;
pub mod run_discovery;
pub mod storage;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[path = "../tests/support/crash.rs"]
mod crash;

pub mod connector;

pub mod reconciliation;

pub mod flow;
mod flow_coordinator;
pub mod flow_node;

pub mod continuation;
mod flow_wait;

#[cfg(target_os = "linux")]
pub mod worker_process;

#[cfg(target_os = "linux")]
pub mod worker;

#[cfg(target_os = "linux")]
pub mod embedded;
