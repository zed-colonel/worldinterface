# Operator publication staging

WSP-02 prepared eight 0.2.0-alpha.1 archives and verification evidence on
`wi-supervisor-proof-1`. WSP-05 prepares 0.2.0-alpha.2 with WI-WORKER-G5. Publication is
operator-owned. Neither work item tags, pushes, publishes, merges, changes remote
protection, or records publication completion.

The historical WSP-02 CraftingTable repository-policy observation had settings version 1,
observed 2026-11-10T05:57:55.908Z, integration branch `wi-supervisor-proof-1` at
`b67245d07f45b1e4e0ddf42d835355fc3cbb3a85`, and proposed `main` freeze at
`928b5ef07797762bc002920af05a01aa4a4b0e0c`. Its controller controls require a
qualifying review of exact source and target revisions before controller
integration, under operator approval or recorded delegation. Final promotion
requires explicit operator approval; `main` and `master` are manual branches.
A proposed freeze or matching ref is an observation, not independent adoption
of a freeze. Retain separately the applicable adopted freeze instruction and
fresh observations at publication. These local controls do not establish GitHub
protection or constrain direct Git outside CraftingTable.

For WSP-05, the supplied adopted repository-policy evidence is settings version 1,
observed 2026-11-15T03:55:06.734Z: integration `wi-supervisor-proof-1` at
`451aa6484a9552cc17eaa0d9c3c6f5b561a01ec8`, no reported issues, and proposed
`main` freeze at `928b5ef07797762bc002920af05a01aa4a4b0e0c`. The same controller
review and integration controls apply. This fresh local observation records no
remote protection inspection or publication. Remote obligations remain in steps
2–5; their absence does not block local preparation.

The former WI-01 procedure's `wi-fabric-2` destination, `f48df82` freeze,
controller Cargo and `ct-act` references are historical. Current checks use Rust
1.89.0 and crates.io ActionQueue 0.3.0. There is no controller Cargo launcher or
cross-project AQ checkout pin. The separate preparation/completion receipt
schemas and their WI validator remain authoritative.

1. At the reviewed item's eventual merge commit, use a clean independent clone
   outside CraftingTable's checkout and an external empty output directory. Keep
   `CARGO_TARGET_DIR` external and separate from evidence. `TMPDIR` must name an
   existing writable directory outside the checkout. Unset, empty or unusable
   `TMPDIR` refuses preparation before toolchain probing, fetching or gates, with
   instructions to create and export it. Preserve CraftingTable's supplied
   temporary and build directories. In an ordinary shell, set up missing values:

   ```sh
   if [ -z "${TMPDIR:-}" ]; then
     export TMPDIR="$(mktemp -d /tmp/wi-release-tmp.XXXXXXXX)"
   fi
   export TMP="$TMPDIR" TEMP="$TMPDIR"
   if [ -z "${CARGO_TARGET_DIR:-}" ]; then
     export CARGO_TARGET_DIR="$(mktemp -d /tmp/wi-release-build.XXXXXXXX)"
   fi
   ```

   Caller-supplied paths are canonicalized for gates and staging, retaining the
   store boundary's rejection of symlink components. Run:

   ```sh
   cargo +1.89.0 fetch --locked --manifest-path crates/worldinterface-foundation/Cargo.toml
   cargo +1.89.0 fetch --locked --manifest-path crates/worldinterface-runtime/Cargo.toml
   python3 -B scripts/release.py prepare --version 0.2.0-alpha.2 \
     --commit "$(git rev-parse HEAD)" --output /absolute/new/wi-alpha2
   python3 -B scripts/release.py verify --manifest /absolute/new/wi-alpha2/release-manifest.json
   crate-release plan /absolute/new/wi-alpha2/release-manifest.json
   crate-release check /absolute/new/wi-alpha2/release-manifest.json
   ```

   Preparation fetches dependencies, runs every adopted argv with separate logs,
   builds archive dependencies through an external Cargo local registry of exact
   checksum-verified archives, runs all packaged self-contained tests, builds
   rustdoc with warnings denied, exercises the registry consumer and installs both
   workers. Local-registry Cargo homes live under disposable staging; the shared
   crates.io archive cache is read only during registry seeding. Rustdoc clears
   the external target's `doc` directory before generating release documentation.
   Archive sources come from the committed Git tree; ignored checkout files are
   excluded. Gates clear inherited compiler flags, wrappers, C/C++ compiler
   overrides and Cargo build/target/profile overrides. `gate-environment.json`
   records that environment separately from the worker install environment in
   `build-environment.json`; both are hashed into the receipt and manifest.
   Worker builds add the path remappings recorded in the recipe. It produces source.bundle, notes, API docs, archives, resolution and
   production-feature evidence, the linked consumer handoff, the WI preparation
   receipt, release-manifest.json and SHA256SUMS. Failures retain logs without a
   successful manifest. Rebase or merge requires fresh preparation. CraftingTable
   separately runs its adopted definitions through `ct-check --declared ID` on
   the committed candidate; the script's direct gate runs do not replace that.

2. Before first remote publication and final release approval, configure and
   independently verify the applicable remote branch/review protections (the
   plan's default protects GitHub `main` against force pushes and deletion).
   Record destination, branches, observed settings, observer and observation time
   in hashed external evidence. Verify it covers the current destination and
   settings. Missing or stale verification blocks publication, not local
   preparation. Authentication failure establishes neither presence nor absence
   of protection. No exemption is claimed by WSP-02 or WSP-05. Preserve this observation
   with the publication records and reference it in the operator's final
   preparation receipt; validate that receipt through WI's receipt validator.

3. After the protection gate and release approval, create `v0.2.0-alpha.2` at the
   prepared merge commit on `wi-supervisor-proof-1` and push the tag. Run
   `crate-release publish /absolute/new/wi-alpha2/release-manifest.json --verified`.
   The operator retains the exact publisher path, digest, invocation and
   destination. The tagged integration commit reaches `main` only through the
   approved final promotion. Alpha.1 used this route for `v0.2.0-alpha.1`; a
   subsequent published code change needs another pre-release.

4. From an external copy, run the registry-only consumer and worker install route
   in [0.2.0-alpha.2.md](0.2.0-alpha.2.md), using crates.io with no replacement or
   patches. Retain resolution, lock, commands, worker measurements and outcome
   under `~/.local/share/worldinterface-releases/<version>/`. Record publication
   for the Exoskeleton map's waiting item only after registry availability and
   consumer verification. In that published map record, relay the exact prepared
   `consumer-handoff.json` file and its SHA-256 from `release-manifest.json`, with
   the version and source commit, to EXV-11 (EXV-27 for alpha.2). Supply the file as
   an input to the downstream work item so its agents need only crates.io network
   access. The receiving operator verifies the digest against the trusted release
   manifest before forwarding it; the consumer verifies the supplied digest and
   compares the handoff's crate checksums to its crates.io lockfile. Keep the
   prepared bytes intact, including `publication_status: not-performed`; the map
   record and separate completion receipt establish subsequent publication.

5. Capture actual remote refs, published artifacts and fresh protection settings,
   including observer/time. Write a separate `publication-completion` receipt per
   `completion.schema.json`, referencing the preparation receipt digest, exact
   source revision, operator, action, script digest, destination, successful
   command/log and observed state. Validate it with:

   ```sh
   cargo run --locked --offline --manifest-path tools/wi-contract-check/Cargo.toml \
     -- publication check /absolute/path/publication-completion.json
   ```

Receipt validation establishes structure, artifact integrity and identity
consistency. Operator review establishes observation credibility and publication
authority. No completion receipt is produced by preparation, and no release
bundle certifies a source revision other than the exact clean commit it names.
