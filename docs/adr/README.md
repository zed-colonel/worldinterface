# Architectural decision queue

The operator owns every decision in queue.json. Queued is not accepted. Preserve the source subjects and deadlines; WI-ADR-016 definition clauses precede dependent provider admission/evidence work. Each accepted ADR must record rationale, alternatives, invariant impact, verification, decision owner and approval record. Recommendations alone cannot accept an ADR.

WI-ADR-017 is fully accepted under the recorded operator approval. WI-ADR-016 is
`accepted-clauses` only for its early representation/admission definitions and
listed consumers/phases; its full checkpoint remains pending. `approvals.json`
retains exact supplied decisions, consumer applicability, retained obligations,
approval IDs and packet digests. The checker pins that evidence independently of
queue status. Adding acceptance requires a new operator record and a reviewed
pin update; editing a status cannot manufacture controller acceptance. These
records establish design authority only, never implementation or test success.

WI-02 additionally imports exact supplied approvals under `approvals/`: ADR-001/002/003
are accepted in full. The per-decision ADR-016 record preserves the same clause
approval as `approvals.json`; both forms are pinned and checked. The shared queue
uses `accepted-clauses` for ADR-016, retaining all later obligations.
