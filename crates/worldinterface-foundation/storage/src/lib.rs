//! Fresh local repositories. This package authenticates nobody and dispatches nothing.
//!
//! All public APIs are trusted-host persistence interfaces. Host composition must
//! authenticate and authorize tenant, principal, artifact and evidence access
//! before using them; tenant arguments alone confer no authority. In particular,
//! snapshot, enumeration, journal append and artifact APIs must never be exposed
//! directly to remote callers. ReceiptAccess is a trusted authorizer assertion.

#[cfg(all(feature = "crash-points", not(debug_assertions)))]
const CRASH_POINTS_REQUIRE_DEBUG: () =
    assert!(false, "crash-points is restricted to debug verification builds");
pub mod contextstore;
mod repository;
mod root;
pub use repository::*;
pub use root::*;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(crate) fn invalid(s: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, s.into()).into()
}

mod ingress;

#[cfg(feature = "crash-points")]
mod crashpoint;
mod effect;
mod proof;
mod publication;
mod receipt;

#[cfg(test)]
#[path = "../tests/support/crash.rs"]
mod crash;

pub use effect::{
    is_effect_contention, DispatchFault, EffectExecutionTimes, EffectJournalConflict,
    EffectReceiptCommand, EffectReceiptSource,
};

mod reconciliation;

pub mod flow;

mod continuation;
pub use continuation::continuation_signal_admitted;
