# WIP-11 collection reconciliation exact-packet evaluation request

Explicit operator acceptance is requested for the canonical SHA-256 printed by
`scripts/refresh-wi-worker-evaluation.py`, including the exact supervisor
source disposition in `supervisor-disposition.json`. This is a candidate request,
not an approval. The existing receipt and independent trusted test-host anchor
authorize only the previously accepted WIP-11 restart remediation packet
`1d13a48427d0f745268c7acdf2fcd58d7c0e34a08df25b7bee9b0124422bf298`, accepted in
`9dbc0c4` and expiring 2026-11-11T00:00:00Z.

That packet completed full live evaluation and all ten adopted checks before this
change. Nine adopted checks passed. The direct evaluation (continued across all
targets) and adopted runtime-test both failed only WP-028's new crash-continuation
assertion. An isolated exact-name reproduction also failed. Before a durable
receipt, the executor correctly recovers a Running effect as Uncertain without
reinvocation. The test incorrectly expected immediate Succeeded on resubmission.

The follow-up asserts Uncertain for interrupted pre-receipt cuts and Succeeded
only after a committed receipt. It confirms the interrupted collection using the
real AQ reconciliation handler, preserving the original receipt, before checking
completion through Release/rebuild/reopen. It also verifies that the original
flow outcome remains unchanged and exact acknowledged collection reconciliation
still reports Committed after resource quarantine. The connector now keys this
last check to the terminal Published collection instead of allocation quarantine.
Unpublished/quarantined collections still report Unknown. The initial F-001
terminal collection, completion and scoped-read fixes remain; F-002 records the
actual accepted evaluation and failure in coverage and programme notes.
Supervisor source/pin and task/operation/bound scope are unchanged. These changed
source/test/status bytes have compilation/non-live evidence only until renewed
exact acceptance.

The request adds WP-027, WP-028 and WP-031 to WP-017–026, AB-WI-021 and the
source-bound WIP-09 regressions. It covers scoped artifact imports/exports, durable
collection snapshots, staged WI publication, quarantine/recovery, descriptor race
checks, and a WI-owned asynchronous fixture labelled **seam simulation**. The seam
fixture exchanges bounded messages and no path, PID, workspace or store handle.
It does not qualify a remote backend or demonstrate isolation.

All sources, test compositions, contract data, lockfiles and build recipes remain
bound in `evaluation-manifest.json`. Rust is 1.89.0 and AQ is crates.io 0.3.0 under
the recorded WIP-07 migration. Tasks, operations and environment retain the prior
limits: six operations, the existing typed task set, at most 15000 ms wall time
and 65536 bytes of retained output/exports per evaluation run. Profile bounds are
one input (1 MiB), four exports (4 MiB aggregate); evaluation controls further
restrict outputs. Collection manifests remain limited to 64 KiB.

The process supervisor's changed `mod.rs` source is proposed for exact-source
review; the five-file exception and generic continuation ban remain unchanged.
The trusted seam provenance is its reviewed source, explicitly carrying no
process helper. The legacy realization manifest's executable slot carries this
source artifact only in the simulated composition. It is not claimed as a process
binary, external backend, isolation proof or ordinary-use qualification.

Complete non-live compilation, Clippy, pure collection/security/replay/crash checks
and the source-bound build recipe before acceptance. That recipe builds all test
targets and measures actual fixture/helper binaries without launching either.
System support subprocesses remain confined to reviewed regression code. The separate approval receipt/host anchor and the two derived inventory files
containing the anchor's hash are outside the packet to avoid recursive acceptance.
Static inventory dispositions are bound, and contract checks still validate both
derived inventories. Missing or drifting bound sources, invalid approval, expiry
or revocation reject positive use.
No test creates, widens or retargets an approval. Non-ignored live tests are not
silently skipped; the full adopted runtime test awaits actual acceptance.

The request grants no ordinary worker use, production credentials, hostile-code
containment, publication, integration or gate closure. Acceptance must name the
exact digest, actual operator/decision provenance, scope and a finite expiry no
more than seven days after acceptance. Record that actual decision only in the
separate receipt and trusted test-host anchor. Covered source changes require
renewed exact-digest acceptance. The candidate accepted-supervisor-pin update is
included in this packet for review; activation and positive evaluation still
require acceptance of these exact bytes. All ten adopted committed-head checks,
focused security review and CraftingTable integration remain due.
