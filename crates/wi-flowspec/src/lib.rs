//! FlowSpec parser, validator, and compiler to ActionQueue DAG.
//!
//! This crate takes a declarative `FlowSpec` (from `wi-core`) and compiles it
//! into ActionQueue `TaskSpec` hierarchies with dependency declarations.

pub mod compile;
pub mod config;
pub mod error;
pub mod id;
pub mod payload;

pub use compile::{compile, compile_with_config, CompilationResult};
pub use config::CompilerConfig;
pub use error::CompilationError;
pub use id::{derive_coordinator_task_id, derive_step_run_id, derive_task_id};
pub use payload::{CoordinatorPayload, StepPayload, TaskType};
