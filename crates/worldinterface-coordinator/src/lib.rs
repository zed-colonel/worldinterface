//! ActionQueue handler for WorldInterface FlowRun orchestration.
//!
//! The Coordinator is an ActionQueue `ExecutorHandler` that manages a FlowRun's
//! lifecycle: it submits Step tasks via `TaskSubmissionPort`, tracks their
//! completion through `ChildrenSnapshot`, and reports the overall flow result.

pub mod branch_eval;
pub mod coordinator;
pub mod error;
pub mod handler;
pub mod resolve;
pub mod step;

pub use error::{BranchEvalError, CoordinatorError, ResolveError, StepError};
pub use handler::FlowHandler;
pub use resolve::resolve_params;
pub use step::BranchResult;
