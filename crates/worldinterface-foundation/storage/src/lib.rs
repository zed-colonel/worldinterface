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

pub(crate) fn decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(worldinterface_protocol::strict_json(bytes).map_err(invalid)?)?)
}
pub(crate) fn decode_collection<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(worldinterface_protocol::strict_collection(bytes).map_err(invalid)?)?)
}

/// Temporary measurement only; removed after the baseline profile.
pub mod wsp_profile {
    pub fn now() -> u128 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    }
    pub struct Span(String, u128);
    impl Span {
        pub fn new(name: impl Into<String>) -> Self {
            Self(name.into(), now())
        }
    }
    impl Drop for Span {
        fn drop(&mut self) {
            eprintln!(
                "WSP_PROFILE {}",
                serde_json::json!({"phase":self.0,"start":self.1.to_string(),"end":now().to_string()})
            );
        }
    }
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

mod worker;
pub use worker::worker_boundary;

mod worker_collection;

pub mod control_plane;
