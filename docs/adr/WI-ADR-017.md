# WI-ADR-017: Capability admission checklist and connector quarantine

Status: accepted

Owner: operator

Due: WI-10

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals.json), approval `01f46a00-49ae-4244-ae29-ea95cc6413c3`, 2026-09-19T22:50:50.725Z. Decision packet SHA-256: `0ed0ac6f51edbcd7f66f5c2f2affaff97ad7000aefb3e761639e550e380d3f4a`.

Coverage: full.

Adopt the admission, evaluation-only, invalidation, quarantine and removal-readiness rules in the attached WI-10/domain proposal. The domain slice builds an offline typed inventory/admission/removal-readiness checker, never a live provider registration or deletion path. Require a justified admission criterion, mediation-cost assessment and complete descriptor/evidence contract. Ordinary use requires applicable qualification. Evaluation-only use requires explicit, exact fixture approval and cannot waive controls or self-promote a provider. Qualification binds the exact profile, adapter, executable/dependencies, configuration, environment, fixtures, results and reviewer evidence; relevant change or revocation invalidates it. Quarantine blocks new starts while retaining separately authorized inspection, termination and reconciliation. Removal remains blocked until exact replacement revisions, qualification, fixtures and applicable WI-06/WI-09 acceptance are evidenced. WP-007 remains synthetic definition-level verification with runtime_pass_asserted false. Current target catalog entries remain pending actual admission, and process profiles gain no VM/Kata or hostile-code qualification from naming or context. This approves the governance rules, not production admission, runtime removal, parent acceptance or test completion.

Rationale: On open question #1, as the operator, I accept the rules as they are proposed.

On open question #2, I accept the narrow prerequisite amendment above. Full evidence will be provided by other slice work.

Retained obligations: Production admission, runtime removal, independent verification, exact merge-target checks and parent acceptance remain required.

Consumer applicability: []

Alternatives: ordinary admission from declarations alone, self-promotion after evaluation, fabricated process image identity and unbounded metadata were rejected by these rules.

Invariant impact: exact resource/authority and evidence distinctions are preserved; no runtime or controller gate is activated.

Verification: offline mutation and WP-007 definition tests are specified in `contracts/wi10-obligations.json`. Approval is operator guidance, not a test result. Full runtime qualification requires independent future evidence.
