# Target test package slots

manifest.yaml reserves one runtime slot per W1–W59 and four fresh-store scenarios.
All slots remain unimplemented. The checker tests in
`tools/wi-contract-check/tests/checker.rs` validate this package's structure and
reject false runtime receipts; they do not execute these slots.

WI-02 must supply a real store-opening harness for store-scenarios.json. Materialize
the input file map in a new temporary directory, invoke the target store API, and
compare every original byte on rejection. Initialize/open scenarios must also
validate resulting schema and replay. Do not use the checker’s matrix validation
as evidence that the runtime rejected a store. Later slices supply executable
harnesses for the remaining invariant slots.

For each implemented case, replace the slot deliberately, increment package
revision, bind its canonical JSON source record SHA-256 and fixture manifests,
and submit an independently reviewed runtime receipt with actual execution
identities. Canonical source-record bytes here mean compact serde_json encoding
with lexicographically ordered object keys (default serde_json Map).
