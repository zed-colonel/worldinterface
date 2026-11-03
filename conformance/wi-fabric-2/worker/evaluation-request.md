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

The prior receipt and test-host anchor still record only acceptance of packet
`fcf4f9936abd712244a78f3499c4b76eb7acceb068385e1eda43e84169261ffc`
at `70caa54`, expiring 2026-11-10T19:00:00Z. They do not authorize this revision.
The 2026-11-03 follow-up permits preparation and non-live validation of the revised
packet only. No positive live test has run on this revised source.

This revision addresses the executable artifact size, canonical journal nulls
(including an absent cleanup exit status), and clean Cargo test build ordering.
The new non-launching regressions exercise real artifact publication, trusted
build digest/path drift rejection, and intent/ownership/cleanup journal replay
with missing/false/true exit status and corrupt/truncated evidence rejection.
The journal test observes only its own process; it launches and signals nothing.

`supervisor-disposition.json` proposes the updated exact `supervision.rs` pin for
the optional exit-status serialization attribute. Acceptance of this packet must
include that pin. The five-file exception, other supervisor source pins, generic
continuation ban, cases, tasks, operations and evaluation limits retain their scope.
The existing receipt must not be retargeted without a new explicit acceptance of
the canonical manifest digest and a finite expiry. All adopted live verification
and qualifying review/integration remain due after that acceptance.
