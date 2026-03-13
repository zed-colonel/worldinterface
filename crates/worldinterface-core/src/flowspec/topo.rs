//! Topological sort for FlowSpec graphs.
//!
//! Provides a shared topological ordering utility used by both the validator
//! (cycle detection) and the compiler (step ordering).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use crate::flowspec::FlowSpec;
use crate::id::NodeId;

/// Returns nodes in deterministic topological order, or the unsorted
/// remainder if cycles exist.
///
/// Uses Kahn's algorithm with a min-heap (ordered by NodeId UUID) to
/// guarantee a stable, deterministic ordering for any DAG. For every
/// edge (A -> B), A appears before B in the result.
///
/// # Returns
/// - `Ok(sorted)` — all nodes in topological order
/// - `Err(remaining)` — cycle detected; `remaining` contains the NodeIds
///   that could not be sorted (they participate in cycles)
pub fn topological_sort(spec: &FlowSpec) -> Result<Vec<NodeId>, Vec<NodeId>> {
    let node_ids: Vec<NodeId> = spec.nodes.iter().map(|n| n.id).collect();

    // Build adjacency structures
    let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
    let mut successors: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for &id in &node_ids {
        in_degree.entry(id).or_insert(0);
        successors.entry(id).or_default();
    }

    let node_set: std::collections::HashSet<NodeId> = node_ids.iter().copied().collect();
    for edge in &spec.edges {
        if node_set.contains(&edge.from) && node_set.contains(&edge.to) {
            *in_degree.entry(edge.to).or_insert(0) += 1;
            successors.entry(edge.from).or_default().push(edge.to);
        }
    }

    // Kahn's algorithm with min-heap for deterministic ordering
    let mut heap: BinaryHeap<Reverse<NodeId>> =
        in_degree.iter().filter(|(_, &deg)| deg == 0).map(|(&id, _)| Reverse(id)).collect();

    let mut sorted = Vec::with_capacity(node_ids.len());

    while let Some(Reverse(node_id)) = heap.pop() {
        sorted.push(node_id);
        if let Some(succs) = successors.get(&node_id) {
            for &succ in succs {
                if let Some(deg) = in_degree.get_mut(&succ) {
                    *deg -= 1;
                    if *deg == 0 {
                        heap.push(Reverse(succ));
                    }
                }
            }
        }
    }

    if sorted.len() == node_ids.len() {
        Ok(sorted)
    } else {
        // Collect nodes that couldn't be sorted (in cycles)
        let sorted_set: std::collections::HashSet<NodeId> = sorted.into_iter().collect();
        let remaining: Vec<NodeId> =
            node_ids.into_iter().filter(|id| !sorted_set.contains(id)).collect();
        Err(remaining)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::flowspec::*;

    fn connector_node(id: NodeId, name: &str) -> Node {
        Node {
            id,
            label: None,
            node_type: NodeType::Connector(ConnectorNode {
                connector: name.into(),
                params: json!({}),
                idempotency_config: None,
            }),
        }
    }

    fn edge(from: NodeId, to: NodeId) -> Edge {
        Edge { from, to, condition: None }
    }

    fn make_ids(n: usize) -> Vec<NodeId> {
        (0..n).map(|_| NodeId::new()).collect()
    }

    fn make_spec(nodes: Vec<Node>, edges: Vec<Edge>) -> FlowSpec {
        FlowSpec { id: None, name: None, nodes, edges, params: None }
    }

    #[test]
    fn linear_flow_sort_order() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let sorted = topological_sort(&spec).unwrap();
        assert_eq!(sorted, vec![ids[0], ids[1], ids[2]]);
    }

    #[test]
    fn diamond_flow_sort_order() {
        let ids = make_ids(4);
        // A -> B, A -> C, B -> D, C -> D
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
                connector_node(ids[3], "d"),
            ],
            vec![
                edge(ids[0], ids[1]),
                edge(ids[0], ids[2]),
                edge(ids[1], ids[3]),
                edge(ids[2], ids[3]),
            ],
        );
        let sorted = topological_sort(&spec).unwrap();
        // A must be first, D must be last
        assert_eq!(sorted[0], ids[0]);
        assert_eq!(sorted[3], ids[3]);
        // B and C must both come before D
        let pos_b = sorted.iter().position(|&id| id == ids[1]).unwrap();
        let pos_c = sorted.iter().position(|&id| id == ids[2]).unwrap();
        let pos_d = sorted.iter().position(|&id| id == ids[3]).unwrap();
        assert!(pos_b < pos_d);
        assert!(pos_c < pos_d);
    }

    #[test]
    fn single_node() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![]);
        let sorted = topological_sort(&spec).unwrap();
        assert_eq!(sorted, vec![id]);
    }

    #[test]
    fn cycle_detection() {
        let ids = make_ids(2);
        let spec = make_spec(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[0])],
        );
        let err = topological_sort(&spec).unwrap_err();
        assert_eq!(err.len(), 2);
    }

    #[test]
    fn determinism() {
        let ids = make_ids(4);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
                connector_node(ids[3], "d"),
            ],
            vec![
                edge(ids[0], ids[1]),
                edge(ids[0], ids[2]),
                edge(ids[1], ids[3]),
                edge(ids[2], ids[3]),
            ],
        );
        let r1 = topological_sort(&spec).unwrap();
        let r2 = topological_sort(&spec).unwrap();
        assert_eq!(r1, r2);
    }
}
