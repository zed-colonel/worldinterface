# Operator publication staging

WI-01 prepares evidence and a procedure. It does not publish, merge, create remote
refs, alter protection settings, or claim the parent gate complete. Local baseline
`worldinterface/pre-wi-fabric-2` resolves to
`f48df82e98203fa94c86eecae60425205c3e00e9`. Remote publication/protection remain
unverified. The integration destination is `wi-fabric-2`; only the controller may
merge under operator approval or recorded integration policy.

At the release milestone the operator uses their `bin` publication script. The
script path, digest, destination and exact invocation must be supplied in the
completion receipt; this repository cannot infer them or invent protection rules.

1. Verify the exact clean reviewed source revision with controller Cargo,
   `ct-check`, and `ct-act`; preserve commands, toolchain/environment, fixture,
   upstream and artifact identities outside build caches. Keep candidate, merged,
   and post-rebase receipts separate. Rebase or merge requires new verification.
2. Prepare a `publication-preparation` receipt conforming to preparation.schema.json.
   Bind the source and baseline revisions, artifact hashes, successful commands
   and verification receipts. Its publication status is always `not-performed`.
   Validate with `wi-contract-check publication check PREPARATION.json`.
3. At the release gate, the operator checks upstream and WI runtime conformance,
   authorizes the exact script/action/destination, and runs the script. Feature
   freeze and the branch/review policy are applied by the operator. Do not infer
   protection settings from the existence of a local branch or tag.
4. Independently capture observed remote refs, published artifacts and actual
   protection/review settings, including observation time. Write a separate
   `publication-completion` receipt, referencing the preparation file digest,
   source revision, operator, action, script digest, destination, successful command
   and observed state artifact. Validate with the same command.

Receipt validation checks structure, artifact bytes and identity consistency. It
cannot prove a remote observation truthful or authorize publication. Operator
review of the observation and script remains required. No completion receipt is
included in WI-01. The candidate preparation is written outside source after the
clean commit is verified, avoiding a self-referential source revision.
