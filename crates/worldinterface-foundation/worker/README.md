# worldinterface-worker

Worker layer for WorldInterface durable embedded workflows. Version 0.2.0-alpha.1; Rust 1.89.0, Apache-2.0.

This pre-release carries the WI embedded worker proof. See the repository
`docs/release/0.2.0-alpha.1.md` for scope, limitations and consumer instructions.

`supervisor` is a separate public instance-lifecycle protocol. Consumers implement
`SupervisorHandler` in an operator-owned service with durable, authoritative
stable-key inspect, outside the managed guest. Its closed operations are `stop`,
`replace` and `recover`; it adds no operation or authority to `WorkerPort`.
The server derives tenant/principal from its authenticated channel, never from
request fields. Authorized envelopes have private fields and cannot deserialize.
An accepted command is pending work, not a completed lifecycle transition.
