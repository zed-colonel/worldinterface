# FlowSpec Format Reference

FlowSpecs are declarative workflow graphs that define how WorldInterface executes
boundary crossings. They can be submitted as JSON or YAML.

## Structure

```yaml
name: my-flow            # Optional human-readable name
id: <uuid>               # Optional; system-assigned if omitted
params:                   # Optional flow-level parameters
  key: value
nodes:                    # Required: list of nodes
  - id: <uuid>
    label: "step name"   # Optional label
    node_type: ...       # Required: connector, transform, or branch
edges:                    # Required: directed edges between nodes
  - from: <uuid>
    to: <uuid>
    condition: ...       # Optional: branch_true or branch_false
```

## Node Types

### Connector

Performs a boundary crossing via a registered connector.

```yaml
node_type: !connector
  connector: delay       # Connector name (must be registered)
  params:                # Connector-specific parameters
    duration_ms: 100
```

Available connectors: `delay`, `fs.read`, `fs.write`, `http.request`.

### Transform

Pure data transformation with no side effects.

```yaml
node_type: !transform
  transform: identity    # Transform type
  input:                 # Input data or template
    key: value
```

Transform types: `identity`, `field_mapping`.

Parameter templates use `{{nodes.<node_id>.output}}` and `{{trigger.body}}` syntax.

### Branch

Conditional routing based on flow parameters or node outputs.

```yaml
node_type: !branch
  condition: !exists
    flow_param:
      path: flag
  then_edge: <uuid>      # Target if condition is true
  else_edge: <uuid>      # Optional: target if false
```

Branch conditions:
- `exists`: True if the referenced value is non-null
- `equals`: True if left equals right

## Edges

Edges define execution order. For branch nodes, edges must specify a condition:

```yaml
edges:
  - from: <branch-id>
    to: <then-target>
    condition: branch_true
  - from: <branch-id>
    to: <else-target>
    condition: branch_false
```

## Parameter Resolution

Parameters support template expressions:
- `{{trigger.body}}` — webhook trigger payload
- `{{trigger.body.field}}` — nested field from trigger
- `{{nodes.<node_id>.output}}` — output from a completed node

## YAML Tag Syntax

The YAML format uses serde_yaml 0.9 tags for enum variants:

```yaml
node_type: !connector    # Not node_type: connector
  connector: delay
```

## JSON Format

JSON uses the standard serde externally-tagged representation:

```json
{
  "node_type": {
    "connector": {
      "connector": "delay",
      "params": {"duration_ms": 100}
    }
  }
}
```

## Validation

FlowSpecs are validated on submission:
- All node IDs must be unique
- Edge endpoints must reference existing nodes
- Branch then_edge/else_edge must reference existing nodes
- At least one node is required
- No cycles (DAG constraint)

## Examples

See `examples/golden-path/flow.yaml` and `examples/golden-path/webhook-flow.yaml`.
