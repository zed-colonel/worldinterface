//! Fresh local repositories. This package authenticates nobody and dispatches nothing.
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

#[cfg(test)]
#[path = "../tests/support/crash.rs"]
mod crash;
