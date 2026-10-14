# Architectural decision queue

The operator owns every decision in queue.json. Queued is not accepted. Preserve the source subjects and deadlines; WI-ADR-016 definition clauses precede dependent provider admission/evidence work. Each accepted ADR must record rationale, alternatives, invariant impact, verification, decision owner and approval record. Recommendations alone cannot accept an ADR.

WI-ADR-008 and WI-ADR-010 have full accepted decisions and operator approvals mirrored from `craftingtable-scope-evidence.json` under `checkpoints[WI-WORKER-G1].decisions`. Their immutable records are in `approvals/`, pinned by SHA-256 in `queue.json` and the contract checker. WI-05 must implement and verify receipt and publication behavior; WI-06 must implement and verify connector and recovery behavior; WI-12 retains optional signing-provider and key-lifecycle work. The approvals do not establish implementation, provider qualification, WI-WORKER-G1 acceptance, runtime evidence, or parent acceptance.

WI-ADR-017 and WI-ADR-016 are fully accepted under recorded operator approvals.
ADR-016's earlier clause approval remains separately pinned; the full approval is a
new record, not a rewrite of its historical coverage. The checker pins both
records independently. These decisions provide design authority only, never
implementation, runtime qualification, or test success.

WI-02 additionally imports exact supplied approvals under `approvals/`: ADR-001/002/003
are accepted in full. The per-decision ADR-016 record preserves the same clause
approval as `approvals.json`; both forms are pinned and checked. The shared queue now records the full approval; later implementation and qualification obligations remain.

WIP-01 imports full accepted WI-ADR-011/014/015/018 approvals from the supplied
2026-09-29 operator records. `approvals/` preserves their exact proposal, rationale,
retained obligations and decision packet identities. Both the queue and independent
checker pins bind their original bytes. WI-ADR-019/020 remain queued because WI-12
is outside this plan. In this plan, retained map-era integration/verification
obligations are carried by the covering WIP scopes, reviewed merge and adopted
checks; historical approval is not current passage. See
[WI-WORKER-PROOF-1](../work-items/wi-worker-proof-1.md).
