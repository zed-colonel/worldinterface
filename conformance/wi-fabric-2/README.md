# WI-FABRIC-2 conformance package, revision 1

The package is partially implemented. AB-WI-001, AB-WI-015 and AB-WI-016 register
WI-02 G1 consumer tests under `crates/worldinterface-runtime`. The unrelated
W1–W59 skeleton slots and historical store scenario inputs remain unimplemented;
they are coverage obligations, not passing runtime evidence. Foundation source
PR tests and WP-002 definitions remain registered separately in `domain/`.

`integration/fixtures.json` binds case filters and the locator golden vector.
Each baseline source hash binds the imported case using recursively sorted
compact JSON. Runtime receipts separately bind current AQ identity, consumer
revision, executable source, fixtures, resolved package/features, toolchain,
environment, logs, nonempty case results and independent review. The checker
rejects planned cases, stale pins, altered artifacts and self-review.

Implementation registration does not assert passing independently accepted case
receipts. Preserve those external artifacts with the exact candidate revision;
rerun after integration/rebase. WI-AQ-G1 and parent acceptance remain controller
obligations. Native integration uses the pinned launcher; the AQ-free native
foundation/checker launcher conflict is documented in the WI-02 integration work
item. All scoped CI jobs use the controller's explicit Cargo configuration.
