# Architectural decision queue

The operator owns every decision in queue.json. Queued is not accepted. Preserve the source subjects and deadlines; WI-ADR-016 definition clauses precede dependent provider admission/evidence work. Each accepted ADR must record rationale, alternatives, invariant impact, verification, decision owner and approval record. Recommendations alone cannot accept an ADR.

`controller-accepted-reference-artifact-pending` means the supplied controller ledger references an accepted checkpoint submission, but this repository has not received the approved decision body. It is neither a new approval nor clause-review evidence. WI-ADR-008 and WI-ADR-010 have this status for WI-WORKER-G1; their owning WI-05 and WI-06 scopes must mirror and reconcile the immutable decisions before their clauses can be independently reviewed. Do not infer decision coverage or runtime authority from the reference alone.

WI-ADR-017 and WI-ADR-016 are fully accepted under recorded operator approvals.
ADR-016's earlier clause approval remains separately pinned; the full approval is a
new record, not a rewrite of its historical coverage. The checker pins both
records independently. These decisions provide design authority only, never
implementation, runtime qualification, or test success.

WI-02 additionally imports exact supplied approvals under `approvals/`: ADR-001/002/003
are accepted in full. The per-decision ADR-016 record preserves the same clause
approval as `approvals.json`; both forms are pinned and checked. The shared queue now records the full approval; later implementation and qualification obligations remain.
