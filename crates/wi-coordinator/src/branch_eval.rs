//! Branch condition evaluation.
//!
//! Evaluates `BranchCondition` variants against runtime values from
//! ContextStore and flow parameters.

use serde_json::Value;
use wi_connector::transform::resolve_path;
use wi_contextstore::ContextStore;
use wi_core::flowspec::branch::{BranchCondition, ParamRef};
use wi_core::id::FlowRunId;

use crate::error::{BranchEvalError, ResolveError};

/// Evaluate a branch condition, returning true/false.
pub fn evaluate_branch(
    condition: &BranchCondition,
    flow_run_id: FlowRunId,
    flow_params: Option<&Value>,
    store: &dyn ContextStore,
) -> Result<bool, BranchEvalError> {
    match condition {
        BranchCondition::Expression(_) => Err(BranchEvalError::ExpressionNotImplemented),
        BranchCondition::Equals { left, right } => {
            let left_value = resolve_param_ref(left, flow_run_id, flow_params, store)?;
            Ok(left_value == *right)
        }
        BranchCondition::Exists(param_ref) => {
            let value = resolve_param_ref(param_ref, flow_run_id, flow_params, store)?;
            Ok(!value.is_null())
        }
    }
}

/// Resolve a ParamRef to a concrete Value.
fn resolve_param_ref(
    param_ref: &ParamRef,
    flow_run_id: FlowRunId,
    flow_params: Option<&Value>,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    match param_ref {
        ParamRef::NodeOutput { node_id, path } => {
            let output = store
                .get(flow_run_id, *node_id)?
                .ok_or(ResolveError::NodeOutputNotFound { node_id: *node_id })?;

            if path.is_empty() {
                return Ok(output);
            }

            resolve_path(&output, path)
                .cloned()
                .ok_or_else(|| ResolveError::PathNotFound { node_id: *node_id, path: path.clone() })
        }
        ParamRef::FlowParam { path } => {
            let params = flow_params
                .ok_or_else(|| ResolveError::FlowParamNotFound { path: path.clone() })?;

            resolve_path(params, path)
                .cloned()
                .ok_or_else(|| ResolveError::FlowParamNotFound { path: path.clone() })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wi_contextstore::SqliteContextStore;
    use wi_core::id::NodeId;

    use super::*;

    fn make_store() -> SqliteContextStore {
        SqliteContextStore::in_memory().unwrap()
    }

    // T-4: Branch Evaluation

    #[test]
    fn exists_true_when_value_present() {
        let store = make_store();
        let fr = FlowRunId::new();
        let params = json!({"flag": "yes"});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() });
        assert!(evaluate_branch(&condition, fr, Some(&params), &store).unwrap());
    }

    #[test]
    fn exists_false_when_value_null() {
        let store = make_store();
        let fr = FlowRunId::new();
        let params = json!({"flag": null});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() });
        assert!(!evaluate_branch(&condition, fr, Some(&params), &store).unwrap());
    }

    #[test]
    fn exists_false_when_value_missing() {
        let store = make_store();
        let fr = FlowRunId::new();
        let params = json!({"other": "value"});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() });
        let result = evaluate_branch(&condition, fr, Some(&params), &store);
        assert!(result.is_err());
    }

    #[test]
    fn equals_true_when_values_match() {
        let store = make_store();
        let fr = FlowRunId::new();
        let params = json!({"status": "active"});
        let condition = BranchCondition::Equals {
            left: ParamRef::FlowParam { path: "status".into() },
            right: json!("active"),
        };
        assert!(evaluate_branch(&condition, fr, Some(&params), &store).unwrap());
    }

    #[test]
    fn equals_false_when_values_differ() {
        let store = make_store();
        let fr = FlowRunId::new();
        let params = json!({"status": "inactive"});
        let condition = BranchCondition::Equals {
            left: ParamRef::FlowParam { path: "status".into() },
            right: json!("active"),
        };
        assert!(!evaluate_branch(&condition, fr, Some(&params), &store).unwrap());
    }

    #[test]
    fn exists_with_node_output() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"result": "data"})).unwrap();

        let condition =
            BranchCondition::Exists(ParamRef::NodeOutput { node_id, path: "result".into() });
        assert!(evaluate_branch(&condition, fr, None, &store).unwrap());
    }

    #[test]
    fn equals_with_node_output() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"count": 42})).unwrap();

        let condition = BranchCondition::Equals {
            left: ParamRef::NodeOutput { node_id, path: "count".into() },
            right: json!(42),
        };
        assert!(evaluate_branch(&condition, fr, None, &store).unwrap());
    }

    #[test]
    fn expression_not_implemented() {
        let store = make_store();
        let condition = BranchCondition::Expression("some expr".into());
        let result = evaluate_branch(&condition, FlowRunId::new(), None, &store);
        assert!(matches!(result, Err(BranchEvalError::ExpressionNotImplemented)));
    }

    #[test]
    fn exists_true_for_zero() {
        let store = make_store();
        let params = json!({"val": 0});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "val".into() });
        assert!(evaluate_branch(&condition, FlowRunId::new(), Some(&params), &store).unwrap());
    }

    #[test]
    fn exists_true_for_empty_string() {
        let store = make_store();
        let params = json!({"val": ""});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "val".into() });
        assert!(evaluate_branch(&condition, FlowRunId::new(), Some(&params), &store).unwrap());
    }

    #[test]
    fn exists_true_for_false_value() {
        let store = make_store();
        let params = json!({"val": false});
        let condition = BranchCondition::Exists(ParamRef::FlowParam { path: "val".into() });
        assert!(evaluate_branch(&condition, FlowRunId::new(), Some(&params), &store).unwrap());
    }
}
