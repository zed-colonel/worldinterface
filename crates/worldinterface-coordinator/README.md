# worldinterface-coordinator

ActionQueue handler for WorldInterface FlowRun orchestration.

## Overview

This crate implements the ActionQueue `ExecutorHandler` that drives flow execution:

- **FlowHandler** -- Multiplexed handler that routes AQ tasks to Coordinator or Step logic
- **coordinator** -- Coordinator task logic: eligibility checks, step promotion, completion detection
- **step** -- Step execution: connector invocation, transform execution, branch evaluation
- **resolve** -- Template parameter resolution from ContextStore node outputs
- **branch_eval** -- Branch condition evaluation for conditional routing

The Coordinator derives step eligibility from AQ state + ContextStore only (Sacred Invariant #5). Step completion writes to ContextStore before reporting success to AQ (Sacred Invariant #2).

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

Apache-2.0
