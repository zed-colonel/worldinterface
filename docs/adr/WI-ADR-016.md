# WI-ADR-016: Agent Host transport, run credential, authoritative process-managed-proof-1 profile, executable/image representation, supported controls, artifact exchange and external-provider seam

Status: accepted-clauses

Owner: operator

Due: WI-09; definition clauses before dependent provider admission/evidence work

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals.json), approval `2e15ca00-a081-4f2c-8430-bc81b2f27ec9`, 2026-09-19T22:56:11.805Z. Decision packet SHA-256: `cafcb369dfeb14acbd6407ffd740da96d1d4de8f642243a1704128b05c89acc4`.

Coverage: clauses.

Approve only the early process identity/provenance representation and admission-evidence definition clauses required by WI-02/domain (WP-002) and WI-10/domain (WP-007). Use the existing worker resource selector and EffectInputRef for requested profile/inputs; bind selected profile, adapter, executable/dependencies and configuration through ResolvedEffect and reviewed typed immutable artifact manifests. Record applied/observed controls through bounded versioned artifacts referenced by EvidenceRef, keeping requested, advertised, applied and observed values distinct. Represent image applicability explicitly as Known, NotApplicable or Unknown; process fixtures use NotApplicable without fabricated image digests. Enclosing-instance evidence is separate and does not establish worker isolation. Portable identity uses opaque provider/run/resource references, never host paths or PIDs. Bind through existing receipt/content references; no free-form metadata or new public worker protocol family. Implement and fixture-review this mapping; a demonstrated wire-compatibility gap requires an explicit narrow amendment. This staged decision supplies the shared definition prerequisite, without requiring the whole WI-02 parent before WI-10/domain design. It does not claim WP-002/WP-007 verification or any live runtime proof.

Rationale: I approve the narrow decisions to unblock current roadmap slices. Later slices will amend to record their requirements.

Retained obligations: The full WI-ADR-016 remains pending for WI-09/domain, WI-09/integration, WI-09/process-provider and WI-11/supervisor-boundary. Retain the complete transport, credential, supported-control, artifact-exchange and external-provider seam decisions and any clauses outside this early representation/admission-definition scope. Runtime realization, exact mapping conformance, qualification, worker evidence gates and all parent acceptance obligations remain independently required. No production provider, sandbox, Kata environment or public protocol extension is authorized by this staged approval.

Consumer applicability: [{"sliceId": "wi/WI-02/domain", "phase": "merge", "replacesFullCheckpoint": true}, {"sliceId": "wi/WI-10/domain", "phase": "start", "replacesFullCheckpoint": false}]

Alternatives: ordinary admission from declarations alone, self-promotion after evaluation, fabricated process image identity and unbounded metadata were rejected by these rules.

Invariant impact: exact resource/authority and evidence distinctions are preserved; no runtime or controller gate is activated.

Verification: offline mutation and WP-007 definition tests are specified in `contracts/wi10-obligations.json`. Approval is operator guidance, not a test result. Full runtime qualification requires independent future evidence.
