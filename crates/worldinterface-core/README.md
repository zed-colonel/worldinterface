# worldinterface-core

Core domain types for WorldInterface: FlowSpec, IDs, Descriptor, Receipt.

## Overview

This crate defines the foundational types used throughout WorldInterface with no ActionQueue dependency and no I/O:

- **id** -- Strongly-typed UUID identifiers (FlowId, FlowRunId, NodeId, StepRunId)
- **flowspec** -- Declarative workflow graph model (FlowSpec, Node, NodeType, Edge)
- **descriptor** -- Connector self-description with input/output schemas
- **receipt** -- Immutable content-addressed evidence of boundary crossings
- **metrics** -- MetricsRecorder trait for cross-crate instrumentation

## Node Types

```
ConnectorNode  -- External boundary crossing (HTTP, filesystem, etc.)
TransformNode  -- Pure data transformation (identity, field mapping)
BranchNode     -- Conditional routing (equals, extensible)
```

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

Apache-2.0
