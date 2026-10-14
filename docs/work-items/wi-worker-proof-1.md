# WI-WORKER-PROOF-1 active ledger

Imported plan version `9212604c-4873-4cd5-b492-1ee3141b8b0b`, accepted and started
2026-10-13. Integration destination: `wi-worker-proof-1`. Baseline
`wi-standalone-base` and import tip: `6d5cbd7d117ece6664c8cbc634fcf02c5e7a5af7`.
Retired map definition `0ebcb7cf-9686-4bc1-9b9d-bb85a1d0b4c9`, binding 4, digest
`51563f70c37bd7ca696f22833ec81d37acfa4918ba380dec679198c0a4f36cff`.

The design recovery recorded successful Rust 1.89.0 baseline checks on that tip:
foundation 123 tests, runtime 30 tests, contract tooling 64 tests, no failures or
ignored tests. These are historical baseline results, not candidate or merged-head
passage. WIP-01 reruns all adopted checks on its committed head. CraftingTable
must establish them on the reviewed merge. Current run evidence is summarized in
[wip-01.md](wip-01.md); logs live outside the checkout.

Repository policy settings version 1 was observed 2026-10-13. Managed integration
requires qualifying review and operator approval or recorded integration policy;
only CraftingTable merges. `main` remains locally frozen at `f48df82e...`.
Remote publication/protection and final promotion retain their operator-owned
milestones. The legacy root workspace stays historical and
`contracts/milestones.json` stays at `WI-01`, so legacy deletion gates remain
inactive. This plan enables an embedded Rust proof only. Daemon, CLI, Python,
remote/two-host execution, hostile-code and VM/Kata qualification are deferred.

The map's STACK-PLAN-ACCEPTED requirement is met by this operator import/start.
AQ-BASELINE-ACCEPTED retains historical submission `7b060900` (2026-09-21); the
current baseline is crates.io AQ 0.2.0 plus its published checksums and adopted
runtime graph check. Per-slice verification and parent review move to the named
WIP scopes, reviewed integration and adopted checks. Gate records preserve the
export's verbatim criteria and requirements. Historical submissions never become
current passage automatically.

The approved operator override makes WIP-01 `check_adoption_at_merge: true`,
replacing the plan bundle's original false value. Add `contract-test` and retain
its three Cargo definition paths. Current adoption governs until merge.
`runtime-aq-graph`'s definition script is also strengthened by WIP-01. The operator
explicitly approved adopting it together with `contract-test` at WIP-01 merge in
the 2026-10-13 remediation guidance (see [WIP-01](wip-01.md)). No source matrix or read-only plan
bundle is changed.

### 3.2 Inherited baseline ledger

WIP-02 implementation verifies the inherited WI-04 admission boundary on crates.io
AQ 0.2.0, registers AB-WI-002/003/004 and WP-009–WP-014 with domain/runtime tests,
and resolves WP-A02 at runtime admission. The WI-04 source-test and ADR-004/005/006/007
assessment, fixed findings and candidate evidence are in [WIP-02](WIP-02.md).
Independent CraftingTable review and merged-head verification remain due. Both
G2 gate records stay open for WIP-04; the historical ledger below is unchanged.

Every `wi-fabric-2` merge, with the evidence the retired roadmap recorded for it. "Verified" means a slice-verification scope receipt on the merged commit, where an independent agent reviewed it and ran checks. WIP-01 commits this ledger to `docs/work-items/wi-worker-proof-1.md` so that finalization reviewers can see which inherited code was independently verified.

| Merge | Source scope | Diff | Retired-roadmap evidence |
|---|---|---|---|
| 2ecb5c3, 288bb63, 8f27839 | wi/WI-01/implementation | +42.4k (frozen evidence, inventory, contracts, checker) | verified ×3; parent WI-01 accepted at 8f27839 |
| f364bad | wi/WI-10/domain | +9.6k | verified (WP-007 definition-level) |
| 9cbdb13, d8e1a49 | wi/WI-02/domain (+ re-verification) | +9.3k | verified at 9cbdb13; re-verified at d8e1a49 |
| c4a2df2, fce7def | wi/WI-03/domain (+ owning-slice repair) | +5.5k / +0.2k | verified at both |
| 03370fd | wi/WI-02/integration | +4.6k | verified; parent WI-02 accepted (03370fd, again at d8e1a49) |
| d5c4ad1 | wi/WI-09/domain | +3.5k | verified (WP-001/003/004/005/006/008) |
| 9e92972 | wi/WI-04/domain | +6.4k | verified (WP-009–WP-014) |
| 6f71472 | wi/WI-03/integration | +3.6k | verified, but WI-03 parent review then requested changes (WI03P-F001, §6.1) |
| bd884be | wi/WI-04/integration | +3.5k | **merged, never verified** |
| afa130d | wi/WI-05/domain | +3.4k | verified (WP-015/016) |

Checkpoint evidence accepted in the retired roadmap:

- **ADRs.** WI-ADR-001 to 020 all accepted. The repository records only 001–010, 012, 013, 016 and 017. WIP-01 imports 011, 014, 015 and 018 from the supporting file `worldinterface-worker-proof-1-accepted-adr-records.md`.
- **AQ-BASELINE-ACCEPTED** accepted.
- **WI-AQ-G1** accepted with AB-WI-001/015/016 (submission df7bc4a3).
- **WI-AQ-G2** accepted (7cdecc06, 7ed4b60e).
- **WI-AQ-G3** accepted with AB-WI-006/007/009/022 (300d033f). This was before WI03P-F001 was found.
- **WI-WORKER-G1** accepted with WP-001–WP-008 (264d791e, b0f3e948).
- **Never passed:** WI-AQ-G4, WI-WORKER-G2–G4 and WI-EMBEDDED-CORE-1.

These records are historical provenance. This plan re-establishes each gate in the repository (§5); it does not inherit an attestation.

### 3.3 Required work computed from the retired map

The transitive closure of `WI-EMBEDDED-WORKER-PROOF-1` over the map's checkpoint `requires` and slice `start_requires`/`merge_requires`/`verify_requires` contains:

- **Checkpoints:** STACK-PLAN-ACCEPTED; the 18 ADR checkpoints WI-ADR-001–018 (019/020 are not required); AQ-BASELINE-ACCEPTED; WI-AQ-G1–G4; WI-EMBEDDED-CORE-1; WI-WORKER-G1–G4.
- **Parent acceptances:** WI-01–WI-05.
- **Slices:** the 22 WI slices below.

| Required slice | Retired-roadmap state | In `wi-fabric-2`? | Class | Item |
|---|---|---|---|---|
| wi/WI-01/implementation | verified; parent accepted | yes | DONE | — |
| wi/WI-02/domain | verified (re-verified d8e1a49) | yes | DONE | WIP-01 re-registers WP-002 |
| wi/WI-02/integration | verified; parent accepted | yes | DONE | WIP-01 reruns AB-WI-001/015/016 on crates.io AQ |
| wi/WI-03/domain | verified (c4a2df2, fce7def) | yes | DONE | — |
| wi/WI-03/integration | verified at 6f71472, then reopened by parent finding WI03P-F001; repair cycle 808910cb stopped at review | original yes; repair **no** (26114a7, +23 commits, +4.2k) | PARTIAL | WIP-03 |
| wi/WI-04/domain | verified | yes | DONE | — |
| wi/WI-04/integration | merged bd884be (cycle 3f48cc88 completed); no verification run | yes | MERGED-UNVERIFIED | WIP-02 |
| wi/WI-05/domain | verified | yes | DONE | — |
| wi/WI-05/integration | cycle dd3d1012 paused at implement; candidate never reviewed | **no** (c507031, +4 commits, +2.9k) | PARTIAL | WIP-04 |
| wi/WI-06/domain | — | no | NOT STARTED | WIP-05 |
| wi/WI-06/integration | — | no | NOT STARTED | WIP-05 |
| wi/WI-07/domain | — | no | NOT STARTED | WIP-06 |
| wi/WI-07/integration | — | no | NOT STARTED | WIP-07 |
| wi/WI-08/integration | — | no | NOT STARTED | WIP-08 |
| wi/WI-09/domain | verified | yes | DONE | — |
| wi/WI-09/process-provider | — | no | NOT STARTED | WIP-09, WIP-10, WIP-11 |
| wi/WI-10/domain | verified | yes | DONE | WIP-01 re-registers WP-007 |
| wi/WI-10/process-catalog | — | no | NOT STARTED | WIP-12 |
| wi/WI-11/domain | — | no | NOT STARTED | WIP-13 |
| wi/WI-11/embedded-runtime | — | no | NOT STARTED | WIP-14 |
| wi/WI-13/worker-conformance | — | no | NOT STARTED | WIP-15 |
| wi/WI-14/worker-handoff | — | no | NOT STARTED | WIP-16 |

Parent state:

- WI-01 and WI-02 were accepted.
- WI-03 parent acceptance (cycle a1f7972e) stopped with "changes requested" on WI03P-F001.
- WI-04 and WI-05 never reached parent review.

The map needs WI-03/04/05 accepted before the embedded runtime can start. In this plan, WIP-02, WIP-03 and WIP-04 each close the whole source exit gate of their parent on the target runtime.

**Interpretation of embedded-proof scopes.** The map's WI-06/07/08 integration slices are `partial_scope: embedded-proof`, class `producer-core`: "prove the source generic engine/continuation semantics … Core verification is not full parent acceptance". This plan therefore implements the following, with the WI-06/07/08 source tests that exercise them:

- the generic connector SPI, executor, uncertainty and reconciliation;
- the Flow plan, compiler and coordinator;
- the wait/signal lowering.

The full-parent obligations that do not serve the generic engine are deferred, with the reason recorded (§11). These are the reference filesystem/HTTP/communication connectors, WASM adapter alignment and legacy deletions.


## Standing work-item steps

WIP-03 takes over the 23 parked WI-03 repair commits through `26114a7`, adds the
exact WI03P-F001 expiry regression and admitted-task companion, and registers
AB-WI-006/007/009/022. [WIP-03](WIP-03.md) maps every WI-03 source-required test,
records ADR-012/013 and the retained admission mapping, and documents the required
security review. WI03I-F010 remains the permitted host-loop follow-up: retain one
`IngressRuntime` per tenant across polls. Independent candidate review and
CraftingTable integration remain due; WI-AQ-G3 stays open for WIP-08.

Register cases against non-ignored exact tests, preserve scope coverage and source
pins, run `hashes refresh`, then all adopted checks on the committed head. Record
review decisions and retained work in the item note. Structural inventory changes
need explicit review. Approval records and closure drafts become effective only
through their authorized reviewed merge. Security review obligations and deferred
full-parent work remain as specified by the imported plan.
