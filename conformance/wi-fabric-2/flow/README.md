# WI-07 pure domain profile: execution-plan-1

WIP-06 implements `wi/WI-07/domain` and WI-ADR-014 in the AQ-free foundation.
`coverage.json` is the authoritative obligation matrix. Its exact, non-ignored,
unambiguous foundation tests are checked by `proof::validate_flow`. This scope
owns no acceptance-case IDs and makes no runtime-conformance claim. The 55-case
registry and WIP-07's AB-WI-005/010/013 remain unchanged.

Protocol's `PlanDocument`, `FlowDefinition`, and `PlanEnvelope` are untrusted data.
Domain's `ExecutionPlan` has private state and no deserializer, unchecked
constructor, or mutable accessor. `compile_direct`, `compile_flow`, and
`restore_plan` all pass through the same validator. `compile_flow_version` also
returns a sealed, immutable `FlowVersion`; revision returns a new validated record
and plan. The persistence layer must enforce durable version uniqueness,
predecessor existence and activation in WIP-07.

The exact plan hash input is:

```
SHA-256("WI-FABRIC-2\0wi-json-1\0execution-plan-1\0" || canonical envelope)
```

The envelope pins schema/plan version 1, record kind, definition identity/version/
predecessor/digest, static causal metadata, every node's stable definition-local
ID, dependencies, guards, inputs/outputs, resources, policies, structural ceilings,
program/runtime identities, schema definitions/digests and the registry digest.
Unordered declarations and finite sets are normalized; expression operand order
is preserved. Recovery requires exact normalized canonical bytes and hash,
revalidates all semantics, and compares the supplied immutable context snapshots.
A recomputed attacker hash does not bypass validation.

Direct submissions select a connector operation explicitly and produce one
boundary node with ID `direct`. Execution-relevant artifact, resource, effect,
budget, deadline, receipt-profile and expected-outcome semantics are pinned.
`direct_submission()` preserves the *entire* original submission separately,
including client identity, principals, attribution, purpose, causal references,
requested authority, attestations, information-use and delegation. These admission
fields are not plan identity. Recovery returns no admission; WIP-07 must persist
and restore it separately, never infer it from the plan. A one-node Flow uses the
same operation contract; its definition provenance changes its plan hash.

The initial closed profile uses these limits:

| Item | Bound |
|---|---|
| Expanded nodes / canonical plan envelope | 64 / 65,536 bytes |
| Per-node dependencies, guards, inputs and outputs | 32 each |
| Definition outputs / static causal metadata | 32 each |
| Submitting/run/definition/node authority alternatives | 64 each |
| Resource possibilities per output or boundary selector | 64 |
| Schemas / connector registrations | 64 / 32 |
| Expression depth / evaluation steps | 16 / 256 |
| Pure materialized value / pure referenced input artifact | 16 KiB |
| Boundary artifact (local publication ceiling) | 16 MiB, further limited by its schema |
| Schema depth / schema nodes | 16 / 256 |
| Object properties / array items / string bytes | 32 / 32 / 4,096 |
| Boundary attempts / timeout | 32 / 86,400,000 ms |

Fan-out is represented by explicit expanded nodes with unique stable IDs. Dynamic
fan-out rejects. No staging, arbitrary callback or loop is supported. Dependency
paths may exceed eight edges: AQ's child hierarchy depth is a different limit.
The profile is a WI restriction, not a claim about the maximum complete AQ workflow.
All bounds compose with the 64 KiB envelope limit; 64 large nodes may exceed it.

Expression schemas form a closed tagged language: Boolean, canonical integer,
bounded string,
fixed object (every property required, no extra properties), bounded homogeneous
array, and resource selector. No null, floating point, coercion, unions, arbitrary
JSON Schema keyword or fetch is supported. Schema definitions are pinned by
`SHA-256(canonical(["wi-schema-1", schema]))`. Literal expression arrays must be
nonempty and have an inferable homogeneous type; declared empty input arrays are
accepted by their pinned array schema. Artifact inputs retain only their
scope/hash/size reference; compilation cannot claim unseen bytes were validated.
Boundary ports may instead pin `Artifact { schema_hash, max_bytes }`: an immutable
external connector schema contract for arbitrary bytes, including non-JSON inputs.
Its positive byte bound cannot exceed the local publication ceiling of 16 MiB
(the separate 64 MiB worker export-manifest ceiling does not apply here). It cannot
be nested in expression schemas or used by pure programs, even as an unused input.
Artifact contracts must match across edges, including their schema hash and bound.
Boundary references using closed schemas also allow up to 16 MiB; the 16 KiB
materialization limit applies only to pure programs. `verify_input_artifact` checks
pinned boundary bytes against their size/hash and validates closed JSON schemas
without expression size limits. For an external artifact schema it returns the
pinned contract, whose `schema_hash` **still requires connector schema validation**
before dispatch. `validate_inputs`/`validate_outputs` reject opaque contracts as
JSON values; they cannot substitute for connector validation. WIP-07 must check
artifact scope/access, integrity and the applicable operation/artifact limits,
validate the pinned connector schema, and perform exact authorization immediately
before dispatch. Compilation and byte verification grant no authority.
Resource artifacts are unsupported because they lack finite declared possibilities.
A direct operation whose output schema is `resource` requires a Flow with explicit
output possibilities in this initial profile.

`wi-expr-1` supports declared input references, literals, object projection,
equality, AND, OR and NOT. It pins its runtime hash and expression implementation
hash. Any access request or unknown evaluator rejects. Evaluation validates inputs,
preserves pinned literals, checks outputs and finite resource possibilities, and
returns value/input hash/output hash/branch outcome. Trace input hashes are
`SHA-256(canonical(["wi-eval-input-1", inputs]))`; output hashes are
`SHA-256(canonical(["wi-eval-output-1", value]))`, using wi-json-1 canonical bytes.
It returns no boundary receipt or authorization. WIP-07 must verify artifact content hashes, materialize
inputs from the actual selected checkpoint producers, and record these results in
workflow traces. `validate_inputs` and `validate_outputs` expose the same pinned
contracts for boundary dispatch/results.

A branch evaluates a Boolean into two distinct declared labels. Node guards are
conjunctions of ancestor branch outcomes. Ordinary references require their
producer's guards to hold at the consumer. Every dependency must also have its
guards included by the dependent node; contradictory guards reject as unreachable,
and missing guards reject as unavailable. The sole exception is a direct producer
named by a validated exclusive selection on the dependent node. Joins may
explicitly select both
exclusive, exhaustive producer alternatives; unsupported conditional availability
rejects. Dependencies require settlement: successful execution or a guard skip
recorded **after all of that node's dependencies settle**. A skipped dependency
never satisfies success for an ordinary consumer. Only validated exhaustive joins
can await guarded alternatives; they wait for all dependencies to settle and read
only the selected producer. Failures remain failures, not guard skips. These rules
preserve ordering through skipped alternatives and underpin wait serialization.
Resource outputs declare finite possibilities. An ordinary resource
selector names exactly one producer; multiple producers require an explicit
exclusive selection. Resource transforms require a provable literal or input
passthrough; arbitrary resource computation rejects.

`AuthorityUpperBound` contains complete (connector, operation, effect, resource
selector) alternatives. Definition ceilings must fit both submitting and run
ceilings; node ceilings must fit the definition; every possible boundary resource
and effect must fit the node ceiling and descriptor namespace. Nothing forms a
cross-product, silently filters forbidden alternatives, or creates agency
`Authority`, grants, authorization envelopes or `Decision::Allow`. Pure programs,
branches and waits have no boundary authority. WIP-07 must still resolve resources,
materialize exact submissions and use the existing exact-submission authorization
path immediately before dispatch.

A signal wait declares exact tenant, namespace, kind and correlation. Opaque
references exclude wildcard syntax. Both early and future waits use exact routing.
A repeated route requires `AfterHandled { wait }`, naming an available ancestor
signal wait on the same route, or a fresh generation label. Handled-cursor chains
must cover every prior nonexclusive wait on that route. An absolute
`AfterSequence` is only an initial cursor: increasing a declared number does not
prove advancement past an earlier event's actual handled sequence. A generation
label is part of exact routing; `AfterHandled` inherits its referenced wait's
generation. Reusing a generation without handled-cursor advancement rejects.
WIP-08 must bind the actual handled sequence or encode the exact fresh generation
when lowering. A handled-cursor reference to a wait that may continue after
timeout rejects because that path has no handled signal. Potentially concurrent waits/timers reject unless
mutually exclusive guards prove that only one can run. Sequential waits are
permitted. Timers and optional signal deadlines have inclusive absolute UTC
second precision, epoch-or-later, and a required fail/continue action. Nanoseconds
are rejected rather than rounded. No clock is read, timer executed, or signal sent.
AQ lowering, checkpointing, event outboxes and race/recovery semantics remain WIP-08.

`generate-vectors.py` independently constructs the fixture context, canonical
timer-only, direct boundary, and branch/transform/boundary plans and their
domain-separated digests with Python's standard library. Run it from any
working directory to reproduce `context.json` and the `plan`, `direct`, and
`branch` `.canonical.json` / `.sha256` pairs; it does not call Rust or consume compiler output. Compile-fail
Rust documentation tests establish executable-plan construction restrictions.
