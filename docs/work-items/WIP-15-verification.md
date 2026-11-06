# WIP-15 accepted remediation verification

This is the execution addendum to [WIP-15](WIP-15.md). The packet-bound item
note and ledger preserve the request and earlier evaluation history. This
addendum records the subsequent operator acceptance and measured cold result
without changing the accepted packet's source bytes.

On 2026-11-06 the operator accepted evaluation-only packet
`289f6efacf3378ba4e955e2164d6ea0f3bc80f8e1b76b07293d3bef6d1cd07d6`
through **2026-11-11T00:00:00Z**, under the unchanged limits in
[evaluation-request.md](../../conformance/wi-fabric-2/worker/evaluation-request.md).
Run `e73756a8-8e69-4f9a-acb7-ba3074a3dbba` supplied that actual decision.
Commit `555a8b09478ed96c0d16c1d8c2ed971c8ee48e44` records it in the separate
approval receipt and trusted anchor. All 315 packet-bound files still match.
No ordinary use, publication, integration or gate closure is authorized.

## Cold timing and F-001

The uncontended cold run used this worktree's supplied external
`CARGO_TARGET_DIR`. `cargo clean` removed the build artifacts, then returned
EBUSY at the protected `.git` mount; `debug`, `release` and `tmp` were verified
absent before execution. Protected mounts were left intact. Dependencies were
already fetched; Cargo ran locked and offline. No other campaign or check ran
concurrently. The complete runtime test command had a hard 1800-second timeout.

Measured on clean commit `555a8b09478ed96c0d16c1d8c2ed971c8ee48e44`, from
2026-11-06T23:08:12Z to 23:30:42Z:

| Phase | Seconds |
|---|---:|
| Initial runtime test build | 124.00 |
| Nested tool build | 9.61 |
| Campaign discovery, remaining builds and listings | 135.84 |
| Registered campaign tests (224) | 623.49 |
| Additional runtime suite tests (146) | 386.27 |
| Receipt and handoff validation | 68.62 |
| Campaign total, including its generation checks | 1215.31 |
| Complete Cargo invocation, including four doctests | **1349.90** |
| Headroom under unchanged 1800-second check limit | **450.10** |

The campaign discovered once, passed **224/224 registered tests**, retained and
passed **146/146 additional runtime tests**, and passed **55/55 cases**. Cargo's
four compile-fail doctests also passed. Receipt generation validated the unchanged
clean generation, exact logs and executable pins without another discovery.
The 1500-second campaign timeout plus 15-second kill grace fits within the
adopted check's unchanged 1800-second limit. This provides cold evidence for
F-001's cost remediation; it does not replace the declared adopted checks.

Receipt SHA-256:
`939d9d15aafce99f95fc9d7892256c34edd04f5f36ae3432a02cfbc96a782e26`.
The receipt binds the actual Linux/OS and execution identity, all six crates.io
AQ 0.3.0 checksums, applied and observed controls/limits, cleanup, individual test
outcomes and all 55 cases. Unknown seam controls remain null; the asynchronous
fixture remains explicitly a seam simulation.

## Review findings and retained work

- **F-001:** sole runtime test ownership, one discovery and the bounded campaign
  are implemented in `e60cbd7`/`c52c170`; measured cold headroom is recorded above.
- **F-002:** the packet-bound item note and active ledger record the prior
  `4dcbe731…` acceptance, its passing campaign receipt and timeout/retry history.
  This addendum records the accepted replacement and its cold execution.
- **F-003:** the generated handoff regression passed in the campaign. It rejects
  substituted producer pins, fixture/observation bytes, inflated claims and
  missing embedded evidence using the payload validator shared by handoff checks.

Eight gates remain closed. `WI-WORKER-G4` and
`WI-EMBEDDED-WORKER-PROOF-1` remain open for WIP-16, which retains consumer
instructions, final handoff publication and closure. WP-033 permits only the
partial milestone; no consumer acceptance, parent acceptance, G5, remote parity,
release, ordinary use or strong isolation is asserted.

## Reproduction and evidence

Reproduce the runtime command from the repository root, with the supplied
external scratch and build-cache environment:

```sh
timeout --kill-after=20s 1800s env PROPTEST_DISABLE_FAILURE_PERSISTENCE=1 \
  cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml
```

The durable external evidence is under this run's `scratch/verification/`:
`cold-runtime-timing.json`, `cold-runtime-test.log`, and
`wi-worker-campaign-5RtFqj/{timing.json,receipt.json,handoff-candidate.json}` plus
individual logs and observations. `run-cold-runtime.py` records the invocation
and timing. The final committed-head declared checks are recorded separately in
`adopted-checks.json` and the ten named check logs. Each declared check is invoked
with `ct-check --declared ID`; supplemental execution is not counted as adoption.
Scratch execution artifacts are not committed source.
