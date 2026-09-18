# Operator publication staging

WI-01 prepares evidence and a procedure. It does not publish, merge, create remote
refs, alter remote protection settings, or claim the parent gate complete.

The adopted authority is `craftingtable-repository-policy.json`, policy version 1,
branch settings version 1, adopted at `2026-09-18T00:41:03.548Z` for plan version
`4782bed3-97da-4d00-9535-64086eff6ea4`. Its controller-local controls already apply:

- Experimental `main` is frozen at
  `f48df82e98203fa94c86eecae60425205c3e00e9`; evidence extraction is permitted,
  further feature work is not. Ordinary controller merges into it are blocked.
- Work proceeds on controller-managed work-item/slice branches. Only the controller
  may integrate into `wi-fabric-2`, after qualifying review of the exact current
  source and target commits, under operator approval or recorded integration policy.
- Final promotion requires explicit operator approval. `main`, `master`, the
  repository default and configured manual-merge branches cannot receive automatic
  integration.

These are local workflow controls, not remote hosting protections or an operating
system write lock; direct Git commands outside CraftingTable are not constrained
by them. Assess the supplied controller policy and review/integration records as
local control evidence. Record fresh, timestamped observations of `main`,
`worldinterface/pre-wi-fabric-2`, candidate and integration revisions separately
from the adopted policy. A matching branch or tag is only a baseline observation,
not proof of protection. Prior operator decisions are guidance, not test results.

Remote protection and publication completion remain unverified. Under the adopted
policy the operator must configure and independently verify applicable remote
branch/review protections **before the first remote publication and final release
approval**. This remote obligation does not defer the local freeze. All remote
operations remain operator-owned; WI-FABRIC-2 supplies publication preparation.

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
3. Before any first remote publication action, including running the publication
   script, the operator configures applicable remote branch/review protections and
   obtains independent verification. Record the destination, applicable branches,
   observed settings, observer and observation time in a hashed evidence artifact
   referenced by the preparation receipt's `artifacts`. Check that this evidence
   covers the intended destination and current settings; absent or stale evidence
   blocks publication. An authentication failure proves neither presence nor
   absence of protection. The same protection gate must be satisfied before final
   release approval. Preparation alone does not satisfy either gate.
4. At the release gate, after the protection gate above is satisfied, the operator
   checks upstream and WI runtime conformance, authorizes the exact
   script/action/destination, and runs the script. Local freeze and controller-local
   review/integration controls continue to apply throughout; final promotion still
   needs explicit operator approval.
5. Independently capture observed remote refs, published artifacts and actual
   protection/review settings, including observation time. This post-publication
   observation supplements, and cannot replace, the pre-publication verification.
   Write a separate
   `publication-completion` receipt, referencing the preparation file digest,
   source revision, operator, action, script digest, destination, successful command
   and observed state artifact. Validate with the same command.

Receipt validation checks structure, artifact bytes and identity consistency. It
cannot prove a remote observation truthful or authorize publication. Operator
review of the observation and script remains required. No completion receipt is
included in WI-01. The candidate preparation is written outside source after the
clean commit is verified, avoiding a self-referential source revision.
