# Selected polish packet acceptance

Scope: record the actual operator acceptance supplied at startup of CraftingTable
run `ecb0df9d-c727-4519-822a-975654382e17` on 2026-11-09 (UTC), then verify the
F-042 through F-050 implementation on the resulting committed head.

The operator supplied this acceptance:

> I accept the evaluation-only packet `9127fbb3da792e3b05f04d0a536cca5a6d6a2d7218d59870a08c4dea747e4049`, expiring **2026-11-15T00:00:00Z**, for its unchanged cases, six operations, reviewed nonsensitive fixtures, eight campaign workers, 15-second wall limit and 65,536-byte retained-output limit.

The previous run prepared and non-live validated the packet on `e2c4486`, with
build evidence at
`/mnt/workhorse/craftingtable/runs/289fdb83-bd81-4888-bade-7364c3a4edfd/scratch/verification/revised-packet/worker-build.json`.
This run revalidated all 322 source bindings, the canonical digest and the
non-live build before recording acceptance. Fresh build evidence is under this
run's `scratch/verification/revised-packet/worker-build.json`. The accepted finite
expiry is less than seven days after acceptance.

`evaluation-approval.json` identifies the actual guidance by run and digest,
without inventing a controller-issued decision ID or operator UUID. The separate
trusted test-host anchor pins its exact bytes. Both remain outside the packet to
avoid recursive hashes. Refresh derived inventory hashes without changing the
accepted packet, inventory dispositions or adopted check definitions.

Verification: run all ten adopted checks on the committed candidate, including
the previously pending live runtime suite and its eight-worker campaign. Keep
logs and fresh campaign evidence under this run's `scratch/verification`.
`WI-WORKER-PROOF-1-polish.md` records the nine selected implementations. This
acceptance enables only their existing evaluation scope; ordinary-use
qualification, reviewed gate acceptance, publication and final integration remain
separate obligations. No implementation deviation or additional deferred work
is introduced by recording this decision.
