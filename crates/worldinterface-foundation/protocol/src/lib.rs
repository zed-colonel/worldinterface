//! Data contracts only. Deserialization never grants executable authority.
mod canonical;
mod identity;
mod records;
pub use canonical::*;
pub use identity::*;
pub use records::*;
pub mod evidence;
pub mod receipt;
pub type Result<T> = std::result::Result<T, String>;

mod ingress;
pub use ingress::*;

pub mod connector;
pub use connector::*;

pub mod control_plane;
pub mod plan;
