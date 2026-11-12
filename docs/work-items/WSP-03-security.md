# WSP-03 security pass

Separate implementation security pass after the adapter and negative regressions
were written. Scope: public protocol, runtime server/transport/journal/connector,
process fixture, proof scope and packaged consumers. This is an implementation
review; CraftingTable's qualifying review remains separate.

Authentication comes exclusively from an operator-provisioned anonymous Unix
endpoint and its server-owned session. Wire types contain no principal, tenant,
engine options or deployment credential. Strict decoding rejects unknown fields,
duplicate keys and oversized input. Submit and inspect both check revocation and
exact operator grants. Worker credentials have no supervisor authentication route.
The fixture clears guest environment and verifies that only stdio is inherited,
after a bounded stdio echo proves guest code has executed. The fixed readiness
message conveys no supervisor authority or credential (F-001 remediation).

Complete bindings, stable keys and generation claims precede dispatch. The journal
owner lock remains held by an outstanding handler across Server drop, fencing
another owner. A crashed supervisor can only inspect retained commands. No crash
window blindly reactivates a backend. Authoritative backend noncommit must exclude
late activation. A consumer reporting unsupported recovery cannot open a server.
Terminal records cannot change; corrupt files/checksums fail reopen. Atomic result
publication uses file/directory fsync and fail-stop on persistence error.

Bounds are enforced in host code: frame size and total I/O time, grant count,
retained command count, evidence size, one outstanding backend call, exact command
deadline and immutable escalation for unfinished observations after expiry.
Authoritative read-only inspect remains available after the execution deadline
to recover retained backend completion; it never reactivates the target. Pending calls return promptly to
WI. Arbitrary trusted handler code cannot be safely killed by Rust; consumers
must enforce engine execution/cancellation bounds outside the guest and isolate
their supervisor process. A stuck handler retains the slot/owner, denying further
activation rather than allowing concurrent dispatch.

The journal root must be provisioned outside WI/guest storage with distinct
operator OS access for hostile guests. The server rejects unsafe ownership,
permissions, symlinks and hard-linked journal files. Checksums detect corruption,
not a malicious operator rewriting its own journal. Rolling back the operator
journal while an engine may remain active is unsupported. These are explicit
composition requirements, not VM isolation claims.

Findings addressed during this pass: generation reuse under another key is
rejected; deadline escalation fences late completion; partial transport failures
close the client channel; journal I/O failure requires recovery. Negative
regressions cover these paths along with stale replies, cross-tenant inspect,
revocation, corrupt journal, concurrent duplicate submission and unsupported
backend admission. Process regressions exercise guest loss, lost acknowledgment,
ambiguous dispatch and fresh host reconnection after WI/supervisor restart.

No open blocking or major finding identified within this bounded trusted-host
contract. Real backend, OS isolation and Kata qualification remain downstream
consumer work, as specified by the accepted WSP-03 scope. Retained pass results
and source revision are in this run's scratch/review and scratch/verification.
