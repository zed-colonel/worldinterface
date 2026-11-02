# WIP-10 completed packet approval request

This packet requests evaluation only, for this candidate's verification and review
for seven days from explicit operator acceptance. It does not authorize ordinary
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

The build recipe compiles binaries and test targets without running fixtures,
verifies every packet source binding, and records measured fixture/helper digests,
compiler identity and packet digest in the run scratch verification directory.
Cargo's binary outputs are embedded in the test host at compile time; their
digests are independent of runtime pathname measurement. The approved source/recipe
and that trusted build output establish provenance; a measured ELF hash alone grants no authority. Sealed executable
capabilities prevent later path replacement from substituting bytes.

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
