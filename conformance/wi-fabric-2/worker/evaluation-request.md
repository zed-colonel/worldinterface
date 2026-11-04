# WIP-10 revised packet approval request

This packet requests evaluation only, for this candidate's verification and review
through 2026-11-10T19:00:00Z, within the maximum seven days from explicit operator
acceptance. It does not authorize ordinary
worker use, production credentials, hostile code, same-user isolation claims,
publication, integration or gate acceptance.

`evaluation-manifest.json` binds the exact source bytes, build/check recipes,
lockfiles, Rust 1.89.0, AQ 0.3.0 graph, worker profile, fixture data and harness.
The packet digest is SHA-256 of the manifest's canonical JSON, printed by
`scripts/refresh-wi-worker-evaluation.py`. The manifest does not hash itself.
`evaluation-environment.json` states restrictions and measured system support
programs. `supervisor-disposition.json` proposes the two changed exact-source
supervisor pins, retaining the original five-file exception and generic
continuation ban. Explicit acceptance must include that exact-source disposition.

The permitted cases are WP-017–WP-026, AB-WI-021 and the source-bound WIP-09
adapter regressions. Operations are provision, start, inspect, terminate,
collect-artifacts and release. Command/checkpoint/suspend/resume are rejection
cases, never authorized operations. Typed tasks are Probe, Coding, Churn(512),
Sleep(1/5000/30000 ms), Transform(xor 0/7), Flood(1048576 bytes), and Tree with
(early_exit, change_session, resist_term) equal to (false,false,false),
(false,true,true) or (true,true,true). Their exact canonical digests are in the
manifest. Wall-time enforcement is at most 15000 ms and retained output/exports
at most 65536 bytes. Negative tests may construct invalid requests to prove
rejection without launch. System support programs and self test subprocesses are
limited to the exact source-bound regressions; they confer no worker capability.

The build recipe compiles all test targets without prebuilding binaries and runs
only the two non-launching build regressions. It verifies every packet source
binding before and after validation, and records actual fixture/helper digests,
byte sizes, compiler identity and packet digest in scratch verification evidence.

The source-bound test-host builder (`tests/support/worker_build.rs`) verifies the
packet, runs a fixed locked/offline Cargo build of the reviewed binary targets,
then rechecks sources and reads only Cargo's reported outputs under its separate
external cache. The compiler environment allows tool/cache lookup and scratch
paths, pins Rust 1.89.0, and excludes ambient wrappers, flags and profile overrides.
The builder retains the resulting bytes and digests and creates private artifact
copies. TrustedWorkerBuild compares the independently built digests and packet
identity against launch measurements; measuring an arbitrary pathname or ELF
never grants build provenance. Paths and digests are not supplied by requests.
Builder success grants no worker operation or evaluation approval. The WI
approval/admission checks remain required before any consequential operation.
Sealed executable capabilities fence subsequent path replacement.

There is no compile-time include_bytes! dependency on unfinished Cargo binaries,
no build script, and no source-generator policy exception. Both executable
artifacts must fit the existing 16 MiB artifact limit. Debug information is
disabled for dev/test builds; debug assertions, overflow checks and native crash
seams remain enabled. Crash parents only warm the compilation cache before their
child-observation deadlines; this neither launches workers nor grants approval.

Approval must come from actual explicit operator acceptance of the completed
canonical manifest digest. Its decision identity, operator, scope and finite
expiry are provisioned into the separate `evaluation-approval.json` receipt and
trusted test-host anchor. These two trust records are intentionally outside the
source manifest to avoid recursive hashes; they may only record that actual
acceptance, never widen it. Repository assertions cannot authenticate themselves.
Missing acceptance, drafts, drift, expiry, revocation, unknown decisions or scope
expansion reject. Covered changes require a new packet and renewed acceptance.

After acceptance, positive lifecycle/crash/concurrency tests and every adopted
check must pass on the committed candidate. Qualifying CraftingTable review and
integration remain required for G3 acceptance; no positive passage is asserted
by this request or by the source-to-test mappings.


## Changes requiring renewed acceptance

The receipt and test-host anchor record only the actual acceptance of packet
`4602f6c489562f1c73424786ed3180b091b79cfc93e6f51dc242fbddf8aacb53`
at `747d4f5`, expiring 2026-11-10T19:00:00Z. That packet failed evaluation at
`abd7a36`; its receipt does not authorize this revision. The current WIP-10
remediation request authorizes these fixes and preparation of the revised packet.
Renewed exact-digest acceptance is required before positive live evaluation.

This revision addresses review findings F-001 through F-004. Every worker success
path publishes a deterministic request acknowledgement matching the declared
output artifact, plus authoritative lifecycle state as evidence and the boundary
record. The executor's publication checks remain intact. A non-launching regression
passes these publications through the domain's outcome validator for all six
operations and advancing lifecycle states, and rejects missing boundary records
and mismatched manifest outputs. Only the path-replacement test's attack copy is
made writable; private build artifacts and sealed capabilities retain their
protections. The crash matrix emits a completion line after each of its 27 cuts,
including cancel and cleanup, so a subsequent authorized run can establish full
coverage. No crash passage is asserted before that run.

The lifecycle coverage mapping records the prior evaluated failure and points to
`docs/work-items/WIP-10.md#remediation-evaluation` for the current result. This
keeps changing result metadata separate from the exact source packet; all test
and case bindings in the coverage mapping remain hashed. Approval provenance
continues to be provisioned only through the separate receipt and trusted anchor.
The result note grants no executable authority.

The supervisor sources and exact dispositions are unchanged from the last
accepted packet. The five-file exception, generic continuation ban, cases,
tasks, operations, environment and evaluation limits retain their scope.
The receipt must not be retargeted without actual explicit acceptance of the new
canonical manifest digest and finite expiry. All adopted live verification and
qualifying review/integration remain due after that acceptance.
