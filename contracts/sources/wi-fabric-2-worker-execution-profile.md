# WI-FABRIC-2 worker execution: initial process provider and external-resource seam

**Profile:** `WI-WORKER-EXECUTION-1`  
**Concrete provider profile:** `process-managed-proof-1`  
**Owner:** WorldInterface / Agent Host  
**Plan:** `WI-PLAN-5`  
**Status:** Normative planning supplement; no implementation or runtime qualification asserted  
**Date:** September 12, 2026

## 1. Decision, scope, and precedence

WorldInterface supplies the authoritative definition of the first embedded worker provider. It is a **bounded, reviewed-process provider for functional delegation proof**, not a hostile-code sandbox. Exoskeleton selects this profile through ordinary WI authorization and supervises work through the generic worker contract. Its Kata/containerd instance target does not require WI to embed containerd, acquire engine sockets, or become a platform execution service.

The profile refines implementation and conformance over `WI-FABRIC-2`; it does not replace the architecture, frozen stack contract, AQ-CONT-1, or developmental evidence profile. The [machine-readable profile](wi-fabric-2-worker-execution-profile.yaml) and [acceptance plan](wi-fabric-2-worker-execution-acceptance-matrix.yaml) accompany this document. None is a runnable deployment manifest, a new work breakdown, or a runtime qualification result. Any inconsistency among normative definitions blocks qualification until reviewed; implementations must not select the weakest interpretation.

The original fourteen parent exit gates, AQ gates, and release order remain. The embedded proof does not satisfy a broad parent obligation for stronger isolation, remote parity, hybrid operation with sensitive effects, or full platform services. Those obligations remain open. A first published release with reduced scope requires a separate release-scope decision.

## 2. Three independent descriptions

Do not combine these dimensions into a single `sandboxed` flag:

1. **Enclosing instance environment:** for the EXO reference consumer, one dedicated Kata VM controlled by an operator. This is external deployment provenance, not a WI worker guarantee.
2. **Worker execution profile:** initially reviewed processes, usually within that same VM. A separate process lifecycle is not an independent worker VM or an assurance that a compromised binary cannot access resources available to its OS identity.
3. **Mediation mode:** fully mediated, sandboxed-native, or hybrid describes how declared operations cross the fabric. It does not establish OS isolation by itself.

A malicious-text fixture may be admitted while arbitrary hostile executable code is prohibited. Neither the outer VM nor a sanitized environment makes shared-instance unreviewed executable trials safe. Initial worker fixtures and permitted inputs must be non-sensitive. Sensitive production data, production credentials, and newly generated unreviewed executable candidates are excluded from this profile. The restriction covers the approved executable and its dependencies/inputs, not merely its display name.

A native developer machine may run controlled provider tests. Only separate EXO deployment tests can establish the qualified Kata instance. The WI provider contract therefore has no mandatory Kata, containerd, hypervisor, or host-path dependency.

## 3. Ownership

| Responsibility | Owner |
|---|---|
| Work purpose, delegation, progress interpretation, verification, and acceptance | Exoskeleton or another application |
| Exact request authorization, admitted provider profile, receipts and artifacts | WI |
| Executable mapping, local process lifecycle and resource mechanics | Agent Host adapter behind WI |
| Durable tasks, waits, signals and continuation recovery | AQ-CONT-1 through existing adapters |
| Vessel VM start/stop/recovery and protected host access | Operator-owned external supervisor |
| Future organizational execution policy and placement | Later DigiCorp composition; not required now |

Exoskeleton may impose stricter consumer restrictions. It cannot weaken WI's profile under the same identifier. EXO-ADR-038 references WI-ADR-016 and the exact profile definition rather than publishing a competing provider definition. The provider harness is owned by WI and must not depend on a functioning EXO binary. EXO conformance later proves consumption and independent acceptance, not the provider's right to claim its own acceptance result.

## 4. Admission: authority and enforceability

Resolve an ordinary request to a canonical run/workspace/provider resource. Check its purpose, principal, acting capacity, information-use rules, approval requirements and grant. Then check that the exact qualified profile and adapter can enforce every mandatory requirement. Both checks must pass before launch.

The binding includes the profile definition digest, approved executable/dependency and configuration identity, provider adapter, policy/grant, requested controls, and the input/export scope. Caller-supplied profile and assurance fields are requests or assertions, never authoritative evidence. The resolver selects an admitted provider realization; requests do not supply raw host commands or arbitrary runtime flags.

Unknown mandatory controls, unsupported independent isolation, or unavailable operations produce explicit denial/unavailability without side effects. There is no silent fallback from external/VM requirements to the local process provider. An operator approval can authorize a request within this profile but cannot make an unenforced property true or waive the profile's exclusions; different assurances require a different qualified profile.

Bind the authorization to the effective provider/configuration. Provider replacement or materially changed controls require re-resolution and authorization before invocation. Reusing a request ID with changed canonical content conflicts under the existing idempotency contract. Profile discovery may reveal supported operations but cannot mint a grant.

### 4.1 Reviewed field mapping, not an invented wire contract

WI-ADR-016 must resolve how process executable/configuration provenance is represented using the existing worker, resource, descriptor, evidence and artifact seams. The older `SandboxSpecification` image/mount sketch is illustrative; do not fabricate a VM image for a process or mistake `not applicable` for `unknown`. Image identity is meaningful for backends that actually execute an image. Local paths/PIDs stay provider-private and are not portable domain identifiers.

This revision does not change frozen wire schemas or require a new public protocol family. If implementation cannot faithfully encode the agreed semantics, record a narrow versioned contract amendment before freezing the adapter. Do not add an unbounded metadata map or silently alter canonical serialization to avoid that review.

### 4.2 Producing qualification evidence without circular admission

A new adapter cannot have a completed conformance result before its first conformance run. The WI-owned test harness therefore uses an explicitly operator-approved **evaluation-only registration** in a non-sensitive test environment. It may run only reviewed fixture cases to produce evidence; ordinary consumer admission stays disabled. All profile exclusions, exact authorization, resource bounds and required-control checks still apply. Evaluation status cannot waive missing mandatory enforcement, authorize arbitrary code, or self-promote the adapter to qualified. A separate reviewer admits ordinary use after the applicable evidence is complete. The planning package itself authorizes no test execution.

## 5. Minimum process realization

The provider exposes provision, start, inspect, terminate, collect-artifacts, and release. Here provision allocates an authorized workspace/run, not a separate VM. Command/steer, checkpoint, suspend, and resume remain unavailable until explicitly supported and tested. An unsupported suspension must not secretly kill a process; application-artifact resume must not be called process-memory restore.

Admission selects reviewed executables and constructs typed bounded arguments. Launch with allowlisted environment, no deliberately inherited privileged credentials or sensitive descriptors, per-run workspace and explicit artifact roots. Enforce bounded output and wall time, and stop/clean the validated process tree with bounded escalation. Any advertised CPU, memory, process-count or network control requires actual enforcement evidence. Workdir selection does not establish filesystem or network confinement.

Credential/descriptor exclusion is a launch control, not a claim that a compromised same-identity process cannot discover other guest resources. Keep fixture inputs non-sensitive, avoid ambient secrets in the proof environment, and document residual co-residency risk. No production secret is intentionally injected. Test-only credentials, where unavoidable, are narrow and run scoped.

### 5.1 Durable lifecycle and ambiguity

Use stable provider/run resource references, a durable start intent and a recoverable run registry. Preserve request/digest identity across retries. Record enough validated ownership information to distinguish a managed process from a recycled PID after restart. A PID alone is not ownership proof.

Exercise the gap between intent commit, process creation, registry persistence and acknowledgement. Reconcile a verified owned process, or conservatively quarantine/stop the ambiguous run before replacement. Never adopt or kill an unrelated process and never fix uncertainty by blind duplicate spawning. This is not a claim of transactional exactly-once OS process creation.

Distinguish accepted start from observed running, requested stop from confirmed exit, and completed run from successful cleanup. Report orphan/cleanup uncertainty explicitly. A worker's exit code or completion report remains execution evidence, not accepted application work. Resource cleanup must not erase required lifecycle evidence.

Worker events become authoritative WI records before AQ wake publication. Test duplicate, late, reordered and cancelled events, timeout races and restart recovery. AQ retains generic execution semantics; no provider retry loop replaces its durability model.

## 6. Evidence and artifacts

Retain requested, advertised, applied and observed controls separately. Bind evidence to the actual profile/adapter, executable/configuration, principal/run, policy/grant, artifacts and AQ/WI pins. Unknown enforcement remains unknown and blocks claims that require it.

Record enclosing-instance provenance separately and name its source and trust. Host-side Kata evidence can be referenced or imported through an authenticated path, but a worker report does not attest the host. An enclosing Kata VM never implies an independently VM-isolated child. Higher layers can classify environment differences as developmental confounds without WI interpreting scores or improvement.

Outputs cross the artifact boundary using scoped references and digests, not a consumer's direct access to a local workspace. Validate run ownership, export paths/links, manifest bounds and content integrity. Reject or quarantine unsafe exports. Preserve publication ordering and uncertainty. Artifact scanning and schema checks support admission; neither WI nor the worker becomes the application acceptance authority.

## 7. Future workers are externally supplied resources

The same control/event/artifact contract must permit a later independent VM, remote service or platform-provisioned worker. Preserve opaque provider/run identity, asynchronous operations, attenuated run identity, explicit supported controls and artifact exchange. Logical delegation parentage does not require nested containers or shared storage.

A WI-owned asynchronous test provider that exposes no PID or shared path proves the consumer seam. It does not claim a remote service has been implemented, qualified, or secured. No cloud provider abstraction, fleet scheduler, distributed budget system, platform WI/AQ or DigiCorp is required for the first proof.

## 8. Separate bounded host-lifecycle adapter

When EXO tests approved instance replacement or recovery, WI may mediate a narrowly authenticated call to an operator-owned supervisor. This is a different resource/capability from `worker.start`; a local worker grant cannot authorize instance replacement.

The supervisor—not the active Vessel or worker—holds runtime-management access and protected recovery material. It accepts only bounded instance-specific operations and approved target references, never arbitrary shell commands, engine flags, devices, mounts or host paths. WI core does not embed a containerd client. Neither guest nor worker receives a host engine socket.

The supervisor persists command identity and outcomes outside the target guest's lifetime. Accepted command is not completed transition. After a stop/restart, the recovering WI imports or queries the result and reconciles its original intent idempotently. Operator recovery remains possible when the guest cannot issue a request. Test candidate/worker credentials cannot activate their own output or access the sole acceptance suite/rollback material.

`WI-WORKER-G5` proves this boundary contract with appropriate fixtures and real adapter evidence where claimed. Actual Kata VM stop/recreate, storage durability and image compatibility remain EXO instance qualification. G5 is not required to prove the narrower process provider.

## 9. Staging and checkpoint semantics

`WI-EMBEDDED-WORKER-PROOF-1` requires G1–G4 from the machine-readable profile. G1 approves definition/mapping; G2 proves admission and evidence; G3 proves real process lifecycle with real embedded AQ/WI; G4 proves artifacts, embedded composition and portable handoff using a WI-owned test consumer.

The harness runs one reviewed coding fixture and one non-coding fixture and covers failures, not just happy-path launches. Real AQ integration requires the unchanged WI-AQ-G1–G4 evidence on exact pins. No real integration adapter may merge merely because a mock passed. A full published WI release retains its G5 and publication gates.

Checkpoint evidence can be produced in explicit partial slices of owning PRs before full parent acceptance. Owner IDs attribute work; they are not dependencies requiring those same parents to be complete. In particular, the embedded composition portion of WI-11 and profile-specific evidence/handoff portions of WI-13/WI-14 can be used without claiming remote/platform or published-release completion. The broader PR dependency graph and exit gates remain the acceptance rules for the full parents.

Do not make WI's proof depend on EXO: WI provider proof precedes EXO consumption proof. Host-lifecycle G5 can be developed independently of the basic worker proof and is consumed by the EXO lifecycle qualification. The next concurrency revision must encode these as evidence milestones/slices, retaining upstream gates and rejecting cycles. This package does not update or activate that concurrency map.

## 10. Full conformance and claim discipline

The existing assertion that a worker cannot escape mount/network/secret/resource policy remains a requirement for the profiles claiming those properties. For the limited process proof, test only the declared controls and record the explicitly unsupported properties. Mark unavailable capabilities as unavailable, not passed by a weaker test, and leave broader parent/profile obligations open. A process proof is not the full WI-09 isolation exit gate.

Enable a profile for ordinary use only with version-bound producer evidence. The planning YAML always says unqualified/planned. Real qualification records live outside this planning definition and bind source, binaries, environment, fixture digests, test results and reviewers. Change or revocation invalidates affected admissions. Quarantine blocks new starts while allowing separately authorized inspect, stop and reconciliation required for safe cleanup.

No generic benchmark, security label, prior planning validator or EXO receipt can replace runtime evidence. No candidate, model or provider can self-certify a higher assurance than the controls actually qualified.

## 11. Handoff and compatibility

Publish the provider definition/profile/acceptance digests and, when implemented, exact adapter and conformance revisions, supported operation set, evidence source, test harness and limitations. Include the unchanged AQ gate bindings. The EXO package pins these planning definitions and later verifies implementation evidence independently.

The importable work breakdown remains one `document`/`pull_requests` file with all original required fields. Supplement YAMLs are typed planning inputs, not additional work lists. Consumers that do not yet understand the supplement may store it as supporting material; they may not infer that importing it enables a runtime.

## 12. Sources and boundaries

This is a project design decision derived from the supplied WI-PLAN-3 architecture/implementation package and EXO-V3-EXECUTION-1, not a new claim about a container product. Source hashes and retained semantics are recorded under `provenance/`. Relevant original sections are WI architecture 10 and 15.5–15.6; WI implementation WI-04/05/06/08/09/10/11/13/14 and WI-ADR-016; EXO execution sections 2, 5–9. No host, provider or target Rust implementation was executed to prepare this document.
