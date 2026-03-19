# worldinterface-flowspec

FlowSpec parser, validator, and compiler to ActionQueue DAG.

## Overview

This crate compiles declarative FlowSpec graphs into ActionQueue task hierarchies:

- **compile** -- Converts a FlowSpec into a Coordinator task + Step tasks with DAG dependencies
- **payload** -- CoordinatorPayload and StepPayload types carried through ActionQueue
- **id** -- Deterministic v5 UUID derivation for TaskId and StepRunId from flow/node identity
- **config** -- CompilerConfig for timeout and scheduling tunables

The compiler produces a `CompilationResult` containing the full AQ task hierarchy, dependency graph, and node-to-task mapping.

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

Apache-2.0
