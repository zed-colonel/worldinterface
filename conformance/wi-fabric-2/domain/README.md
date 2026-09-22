# WI-02 domain foundation

This independently buildable workspace implements wi/WI-02/domain only. The
legacy workspace remains historical implementation during staged replacement;
none of its readers or types is reachable from this target graph. There is no
composition root, production AQ adapter, authentication endpoint, executable
authorization handle, provider, or lifecycle supervisor here. Public records are
data. The internal domain package owns reducers and the three named queue ports.

## Canonical profile wi-json-1

`Envelope` contains contract `WI-FABRIC-2`, canonical profile `wi-json-1`, schema
integer 1, and a tagged `record` containing `kind` and typed `body`. Every variant
of `Record` is an explicit hash input; every field in its schema participates.
Entity UUIDv4 IDs are generated outside reducers and are not derived hashes, so
those IDs participate. Records have no self-derived digest or signature field.
Later receipt/signature envelopes must explicitly preserve this separation.
Unknown fields, duplicate keys (including nested keys), unsupported versions,
null, floating-point and exponent notation, malformed UUIDs/hashes/references,
and invalid timestamps are rejected before hashing. Optional absence is omitted.
No current schema permits null. Decimal application data belongs in immutable
artifacts; this profile introduces no decimal-string field.

Object keys sort by UTF-8 byte order (equivalent to Unicode scalar order); arrays
retain order and strings retain Unicode without normalization. No whitespace or
BOM is emitted. Quotes and backslashes escape; backspace, formfeed, newline,
carriage return and tab use the JSON short escapes; remaining U+0000..U+001F use
lowercase `\u00xx`. Slash and non-ASCII text are unescaped. Integers are decimal,
bounded to ±(2^53−1), without leading zeroes. Timestamp strings are calendar-valid
UTC `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`, without leap-second representations.
Each temporal point separately preserves source, assurance and uncertainty;
occurred/observed/received/admitted/dispatched/committed/reconciled times remain
distinct. Authorization and receipt records pin boundary dependency versions.
Envelopes are at most 65536 bytes. Opaque references are 1..256 ASCII bytes from
letters, digits, dash, underscore, colon and dot; they are not file paths/grants.

Record digest is SHA-256 of the exact prefix
`WI-FABRIC-2\0wi-json-1\0record\0` followed by canonical envelope bytes. Content
hashes are SHA-256 of exact artifact bytes, with no prefix. Golden values were
computed independently with Python JSON key sorting and hashlib, and are checked
by Rust, including recursive field sensitivity and malformed input tests.
Serialization explicitly sorts keys even if downstream serde_json features change.

## Pure transitions

`transitions.json` is the separately represented reference model. Tests compare
all 6 × 30 × 31 machine/phase/action combinations, then 256 generated sequences
(up to 99 events), plus replay, entity, clock and revision checks. Reducers take
identity and timestamp as inputs and perform no I/O. Failed transitions leave
state untouched. Reducer revision counts events; repository revision counts
atomic batches, allowing immutable provenance records without artificial events.

Observation quarantine does not imply truth or application acceptance. Delivery
acknowledgment precedes separate application acceptance/rejection. Unknown effect
commit cannot transition to dispatch/retry. Coarse effect `Reconciled` is paired
with a separate detailed reconciliation record: confirmed committed, confirmed
not committed, still uncertain or escalated. Only confirmed-not-committed can
record an explicit retry permission; it does not itself reexecute an effect.
Proof publication records publication, not truth. These are the local foundation
historical reducers. Guarded ADR-007 semantics now live in the [WI-04 egress journal](../egress/README.md); ADR-008 signing remains separate.

## Local store profile

`StoreRoot` requires an empty directory for creation. Root and per-database
manifests bind contract, schema 1, instance, creation time, feature profile
`local-egress-1` and canonical profile (WI-04 supersedes development profiles
`local-domain-1` and `local-ingress-1`, both rejected unchanged). Fabric, Flow and Context databases are
required; Platform is optional. All manifests and expected members are checked
before SQLite opens. Unsupported, pre-contract and incomplete roots reject
without migration or modification. Initialization publishes the root completion
manifest only after databases and per-store manifests are synced. Each database
contains one immutable canonical copy of its complete store manifest, binding
instance, store kind, contract, schema and profiles. Open and subsequent repository
connections compare these bytes with the validated manifest before exposing data.
Missing identities and swapped databases reject without repair or migration;
backup/restore applies the same validation before publishing a destination.

Each database has append-only batch history, immutable canonical records,
rebuildable projections, atomic scoped request mappings and source-owned
outboxes. `BEGIN IMMEDIATE`, unique keys and expected revisions serialize
concurrent writers. Admission namespace is deployment/tenant + authenticated
principal + inferred submission class + ClientRequestId. Claimed identity stays
in canonical request content. Identical retries recover the original ID;
conflicting content changes nothing. This API is persistence machinery, not an
authentication/authorization endpoint. Trusted transport scope must come from the
future host. Outbox acknowledgments are append-only batch inputs and replayable.
Queries expose cursor/limit history and outboxes separately from mutation.

Reopen checks exact SQLite schema/index/trigger identity, integrity and history
hashes, and compares all projections with deterministic in-memory replay.
`rebuild` repairs derived rows for an already-open maintenance repository without
provider calls. External damage is rejected on open. Journal mode is deliberately
SQLite DELETE with synchronous FULL: this profile has no WAL sidecars to omit.
Unexpected WAL, rollback-journal or incomplete files fail closed; automatic hot
journal/runtime crash recovery and coordinated AQ backup are integration work.

Artifacts are bounded to 16 MiB in this local profile. Files are synced and
published without replacement before scoped availability rows commit. Interrupted
publication can leave unreferenced bytes, never advertised missing bytes. Reads
require the scoped reference and check content hash and size; this check is not
an authorization decision. Ordinary protocol artifact references may describe
external/unavailable evidence and are not availability advertisements.

All opened roots hold shared advisory locks; offline backup requires an exclusive
lock, copies manifests/databases/artifacts to a private staging directory, validates
the complete copy, then publishes the destination. Restore follows the same path.
There is no WAL in this profile. Direct SQLite/file writes that ignore repository
locks are outside the offline API contract. Symlink store members are rejected.

## WP-002 field mapping

`wp002-process.json` is a definition fixture, not provider qualification.

| Meaning | Existing seam |
| --- | --- |
| Requested profile and controls | EffectSubmission resource selector and EffectInputRef |
| Selected profile and realization | ResolvedEffect canonical resource, normalized input, connector and registry snapshot |
| Executable, dependencies, configuration, adapter and profile definition | ImmutableArtifactManifest with exact artifact hashes |
| Requested/advertised/applied/observed controls | Distinct ControlEvidence fields, version 1, through EvidenceRef |
| Image applicability | Known digest / NotApplicable / Unknown; process uses NotApplicable |
| Portable provider/run/resource identity | Bounded opaque references; no host paths or PID fields |
| Enclosing instance | Separate EvidenceRef plus source and trust; no implication of worker isolation |
| Binding | Receipt EvidenceRef binds controls; controls and normalized input bind manifest bytes |

Positive and negative applicability tests and configuration mutation checks verify
this mapping. Receipt dependency versions must match the selected manifest
profile-definition digest and ResolvedEffect resolver version; independent negative
cases reject each mismatch. ControlEvidence is a typed evidence-artifact schema, not a new worker
message family. A demonstrated wire gap still requires a narrow amendment before
adapter freeze. No such gap has been established by these definition fixtures.

## Evidence and retained obligations

`coverage.json` maps each local obligation to independently executable checks.
Run `ct-check -- scripts/check-wi-domain.sh` and the existing contract script,
then `ct-act -W .github/workflows/wi-domain.yml -j domain` (also the changed
contract job). Native checks use the controller Cargo launcher; CI explicitly
uses its configuration. Artifacts and logs belong in external scratch, never here.

Policy version 1 / branch-settings version 1 was adopted 2026-09-18T00:41:03.548Z.
It requires exact source/target review and controller-mediated integration.
Remote protection/publication remains operator-owned, due before first remote
publication and final release approval. `source-evidence.json` records hashes of
supplied policy, approvals, prerequisite receipts and assumptions, not test results.
ADR-001/002/003 are fully approved; ADR-016 has only the supplied early clauses.
WI-AQ-012, WI-AQ-015 and WP-A01 remain open for their named milestones. The target
AQ pin is 29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860, conformance revision 16;
prepared historical AQ 97c9dc26 is unused development input. AB-WI-001/015/016,
production adapters, runtime recovery, independent WP-002 review, actual merged
tree/post-rebase checks, integration-slice verification and parent acceptance
remain separate controller-owned obligations. Nothing here advances those gates.
