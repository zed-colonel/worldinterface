# Selected simplification packet acceptance

Scope: record the actual operator acceptance supplied at startup of CraftingTable
run `7364b809-5445-4ebd-826a-a03d0180fe47` on 2026-11-08 (UTC), then verify the
F-017 through F-041 implementation on the resulting committed head.

The operator supplied this acceptance:

> I accept the evaluation-only packet with canonical digest `dd8191f484a42a772e5ade940ec433befa7b1a71ff76cdbcfb2e568fd733a19c`, expiring **2026-11-14T00:00:00Z**, for its existing cases, six operations, reviewed nonsensitive fixtures, eight campaign workers, 15-second wall limit and 65,536-byte retained-output limit.

The supplied build evidence is
`/mnt/workhorse/craftingtable/runs/444110b3-eff7-47f7-875f-d703ece6a101/scratch/verification/revised-packet/worker-build.json`.
That run prepared and non-live validated the packet on `d02eecb`. Before recording
acceptance, this run revalidated all 322 source bindings and the canonical digest.
The finite expiry is less than seven days after the supplied acceptance.

`evaluation-approval.json` identifies this actual guidance by run and digest; it
does not invent a controller-issued decision ID or operator UUID. The separate
trusted test-host anchor pins its exact bytes. Both are excluded from the packet
to prevent recursive hashes. Derived inventory hashes are refreshed, preserving
the accepted packet bytes and all required check definitions.

Verification: run all ten adopted checks on the committed candidate. Keep their
logs and any fresh campaign evidence under this run's `scratch/verification`.
The implementation note `WI-WORKER-PROOF-1-simplification.md` records the selected
changes and explicit inventory dispositions. This acceptance enables only the
existing evaluation scope; ordinary-use qualification, reviewed gate acceptance,
publication and final integration remain separate obligations. No implementation
deviation or additional deferred work is introduced by recording this decision.
