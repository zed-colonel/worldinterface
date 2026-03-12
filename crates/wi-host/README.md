# wi-host

Embedded Host trait for WorldInterface (Agent integration surface).

## Overview

This crate provides the primary programmatic API for embedding WorldInterface:

- **EmbeddedHost** -- Start a host, submit flows, query status, discover capabilities
- **FlowRunStatus / StepStatus** -- Derived flow and step execution state
- **FlowPhase / StepPhase** -- Lifecycle phases (Pending, Running, Completed, Failed, etc.)
- **HostConfig** -- Configuration for AQ data directory, ContextStore path, tick interval, and metrics

EmbeddedHost owns the ActionQueue runtime and ContextStore, and exposes a high-level async API for flow submission, status inspection, and connector discovery.

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

MIT
