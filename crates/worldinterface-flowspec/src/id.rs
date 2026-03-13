//! Deterministic ID derivation for compilation.
//!
//! Uses UUID v5 (SHA-1 namespace-based) to derive stable, deterministic IDs
//! from a FlowRunId + NodeId pair. This ensures that recompiling the same
//! FlowSpec with the same FlowRunId always produces identical TaskIds and
//! StepRunIds.

use actionqueue_core::ids::TaskId;
use uuid::Uuid;
use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

/// Derives a deterministic StepRunId from a FlowRunId and NodeId.
///
/// Uses UUID v5 with the FlowRunId as namespace and the NodeId bytes
/// prefixed with "step_run:" as name. This guarantees:
/// - Same (flow_run_id, node_id) pair always produces the same StepRunId
/// - Different pairs produce different StepRunIds (with cryptographic probability)
/// - No external state or coordination required
pub fn derive_step_run_id(flow_run_id: FlowRunId, node_id: NodeId) -> StepRunId {
    let namespace = *flow_run_id.as_ref();
    let mut name = b"step_run:".to_vec();
    name.extend_from_slice(node_id.as_ref().as_bytes());
    let derived = Uuid::new_v5(&namespace, &name);
    StepRunId::from(derived)
}

/// Derives a deterministic TaskId for a step from a FlowRunId and NodeId.
///
/// Uses UUID v5 with the FlowRunId as namespace and the NodeId bytes
/// prefixed with "task:" as name. The prefix distinguishes this derivation
/// from `derive_step_run_id` to avoid collisions.
pub fn derive_task_id(flow_run_id: FlowRunId, node_id: NodeId) -> TaskId {
    let namespace = *flow_run_id.as_ref();
    let mut name = b"task:".to_vec();
    name.extend_from_slice(node_id.as_ref().as_bytes());
    let derived = Uuid::new_v5(&namespace, &name);
    TaskId::from_uuid(derived)
}

/// Derives a deterministic TaskId for the Coordinator from a FlowRunId.
///
/// Uses UUID v5 with the FlowRunId as namespace and `b"coordinator"` as name.
/// This cannot collide with step TaskIds because NodeId bytes are 16-byte UUIDs,
/// not ASCII strings.
pub fn derive_coordinator_task_id(flow_run_id: FlowRunId) -> TaskId {
    let namespace = *flow_run_id.as_ref();
    TaskId::from_uuid(Uuid::new_v5(&namespace, b"coordinator"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_run_id_deterministic() {
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let a = derive_step_run_id(fr, n);
        let b = derive_step_run_id(fr, n);
        assert_eq!(a, b);
    }

    #[test]
    fn step_run_id_unique_per_node() {
        let fr = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        assert_ne!(derive_step_run_id(fr, n1), derive_step_run_id(fr, n2));
    }

    #[test]
    fn step_run_id_unique_per_flow_run() {
        let fr1 = FlowRunId::new();
        let fr2 = FlowRunId::new();
        let n = NodeId::new();
        assert_ne!(derive_step_run_id(fr1, n), derive_step_run_id(fr2, n));
    }

    #[test]
    fn step_run_id_not_nil() {
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let id = derive_step_run_id(fr, n);
        let uuid: &Uuid = id.as_ref();
        assert!(!uuid.is_nil());
    }

    #[test]
    fn task_id_deterministic() {
        let fr = FlowRunId::new();
        let n = NodeId::new();
        assert_eq!(derive_task_id(fr, n), derive_task_id(fr, n));
    }

    #[test]
    fn task_id_unique_per_node() {
        let fr = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        assert_ne!(derive_task_id(fr, n1), derive_task_id(fr, n2));
    }

    #[test]
    fn coordinator_task_id_deterministic() {
        let fr = FlowRunId::new();
        assert_eq!(derive_coordinator_task_id(fr), derive_coordinator_task_id(fr));
    }

    #[test]
    fn coordinator_task_id_not_nil() {
        let fr = FlowRunId::new();
        let id = derive_coordinator_task_id(fr);
        assert!(!id.is_nil());
    }

    #[test]
    fn coordinator_and_step_task_ids_differ() {
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let coord_id = derive_coordinator_task_id(fr);
        let step_id = derive_task_id(fr, n);
        assert_ne!(coord_id, step_id);
    }

    #[test]
    fn step_run_id_and_task_id_differ() {
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let step_run = derive_step_run_id(fr, n);
        let task = derive_task_id(fr, n);
        assert_ne!(step_run.as_ref(), task.as_uuid());
    }
}
