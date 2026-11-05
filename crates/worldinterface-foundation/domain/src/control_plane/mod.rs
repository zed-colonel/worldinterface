//! Embedded application boundary. All reducers are deterministic and AQ-free.
//! Repository and bootstrap ports are trusted host interfaces, never request routes.
use crate::{agency::*, authorization::*, ingress::digest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use worldinterface_protocol::{control_plane::*, *};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    Unavailable,
    Invalid,
    Conflict,
    Uninitialized,
    Profile,
    Storage,
    Unresolved,
}
pub type Result<T> = std::result::Result<T, Error>;
fn hash(v: &impl Serialize) -> Result<ContentHash> {
    digest(v).map_err(|_| Error::Invalid)
}
fn collection_hash(v: &impl Serialize) -> Result<ContentHash> {
    Ok(ContentHash::of(&canonical_collection(v).map_err(|_| Error::Invalid)?))
}
fn timestamp(now: u64) -> Result<Timestamp> {
    Timestamp::from_epoch_millis(now).map_err(|_| Error::Invalid)
}

pub trait Authentication {
    type Session;
    fn authenticate(&self, session: &Self::Session) -> Result<AuthenticatedCaller>;
}
pub trait TrustedClock {
    fn now_ms(&self) -> u64;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub status: ProfileStatus,
    pub max_credential_lifetime_ms: u64,
}
impl Configuration {
    pub fn select(profile: Option<Profile>, tenant: OpaqueRef, lifetime_ms: u64) -> Result<Self> {
        let profile = profile.ok_or(Error::Profile)?;
        if lifetime_ms == 0 || lifetime_ms > 120_000 {
            return Err(Error::Profile);
        }
        Ok(Self {
            status: ProfileStatus {
                profile,
                non_production: profile == Profile::LocalDevelopment,
                tenant,
            },
            max_credential_lifetime_ms: lifetime_ms,
        })
    }
    fn validate(&self) -> Result<()> {
        if *self
            != Self::select(
                Some(self.status.profile),
                self.status.tenant.clone(),
                self.max_credential_lifetime_ms,
            )?
        {
            return Err(Error::Profile);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Secret {
    pub custody: OpaqueRef,
    pub version: u64,
    pub generation: u64,
    pub expires_at_ms: u64,
    pub revoked: bool,
}

/// Existing policy, grant and binding formats remain unchanged inside envelopes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum Content {
    Policy(Policy),
    Grant(Grant),
    Binding(Binding),
    Secret(Secret),
}
impl Content {
    fn kind(&self) -> ObjectKind {
        match self {
            Self::Policy(_) => ObjectKind::Policy,
            Self::Grant(_) => ObjectKind::Grant,
            Self::Binding(_) => ObjectKind::Binding,
            Self::Secret(_) => ObjectKind::Secret,
        }
    }
    fn authority(&self) -> Authority {
        match self {
            Self::Policy(p) => p
                .platform
                .union(&p.resource)
                .cloned()
                .chain(p.information_use.iter().cloned())
                .collect(),
            Self::Grant(g) => g.authority.clone(),
            Self::Binding(b) => b.principal_authority.clone(),
            Self::Secret(_) => Authority::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attribution {
    pub requester: PrincipalId,
    pub actor: ActorId,
    pub accountable: PrincipalId,
    pub capacity: ActingCapacityRef,
    pub authorizer: PrincipalId,
    pub approvals: Vec<OpaqueRef>,
    pub at_ms: u64,
    pub request: ClientRequestId,
    pub command: ContentHash,
    pub bootstrap: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub schema: u32,
    pub reference: ObjectRef,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<ContentHash>,
    pub content_digest: ContentHash,
    pub content: Content,
    pub attribution: Attribution,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCommand {
    pub request: ClientRequestId,
    pub reference: ObjectRef,
    pub expected_revision: u64,
    pub content: Content,
    pub claims: IdentityClaims,
    pub approvals: Vec<Approval>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Get { reference: ObjectRef },
    Query { query: Query },
    Admin { command: Box<AdminCommand> },
    Issue { request: Box<CredentialRequest> },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: ClientRequestId,
    pub operation: Operation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Object(ObjectView),
    Page(Page),
    Revision(Box<Revision>),
    Credential(Box<CredentialMetadata>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retry {
    digest: ContentHash,
    outcome: Outcome,
}

/// Produced only by trusted authorization ingestion. No public wire constructor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permit {
    reference: ObjectRef,
    binding: CredentialScope,
    expires_at_ms: u64,
    reservations: Vec<UseLimit>,
    demand: ContentHash,
    revoked: bool,
}

/// Trusted-host bootstrap is separate from ordinary commands and runs once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub configuration: Configuration,
    pub operator: PrincipalId,
    pub request: ClientRequestId,
    pub objects: Vec<(ObjectRef, Content)>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub revision: u64,
    pub last_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<Configuration>,
    pub history: Vec<Revision>,
    pub objects: Vec<ObjectView>,
    pub permits: Vec<Permit>,
    pub issuances: BTreeMap<ContentHash, CredentialMetadata>,
    pub revocations: BTreeMap<ContentHash, IssuanceStatus>,
    pub uses: BTreeMap<ContentHash, u32>,
    requests: BTreeMap<ContentHash, Retry>,
}
impl State {
    fn config(&self) -> Result<&Configuration> {
        self.configuration.as_ref().ok_or(Error::Uninitialized)
    }
    pub fn current(&self, reference: &ObjectRef) -> Option<&Revision> {
        self.history.iter().rev().find(|r| &r.reference == reference)
    }
    fn contents(&self, scope: &Scope) -> Vec<&Revision> {
        let mut seen = BTreeSet::new();
        self.history
            .iter()
            .rev()
            .filter(|r| r.reference.scope == *scope && seen.insert(r.reference.clone()))
            .collect()
    }
    fn authority_revisions(
        &self,
        scope: &Scope,
        principal: &PrincipalId,
    ) -> Result<BTreeMap<OpaqueRef, ContentHash>> {
        let mut revisions = BTreeMap::new();
        for r in self.contents(scope) {
            let applicable = match &r.content {
                Content::Policy(_) => true,
                Content::Binding(b) => &b.principal == principal,
                Content::Grant(g) => &g.subject == principal,
                _ => false,
            };
            if applicable && revisions.insert(r.reference.id.clone(), hash(r)?).is_some() {
                return Err(Error::Unavailable);
            }
        }
        Ok(revisions)
    }
    fn bindings(&self) -> Vec<Binding> {
        let mut seen = BTreeSet::new();
        self.history
            .iter()
            .rev()
            .filter(|r| seen.insert(r.reference.clone()))
            .filter_map(|r| match &r.content {
                Content::Binding(b) => Some(b.clone()),
                _ => None,
            })
            .collect()
    }
    fn scope(
        &self,
        caller: &AuthenticatedCaller,
        claims: &IdentityClaims,
        now: u64,
    ) -> Result<(Scope, Binding, Authority)> {
        if caller.tenant() != &self.config()?.status.tenant {
            return Err(Error::Unavailable);
        }
        let (b, a) = resolve_binding(caller, claims, &self.bindings(), &timestamp(now)?)
            .map_err(|_| Error::Unavailable)?;
        let scope = Scope {
            tenant: caller.tenant().clone(),
            organization: b.capacity.organization_ref.clone(),
        };
        // The binding's owning organization must agree with its server-owned capacity.
        if !self
            .contents(&scope)
            .iter()
            .any(|r| matches!(&r.content, Content::Binding(v) if v == &b))
        {
            return Err(Error::Unavailable);
        }
        Ok((scope, b, a))
    }
    fn policy(&self, scope: &Scope, now: u64) -> Result<Policy> {
        let policies: Vec<_> = self
            .contents(scope)
            .into_iter()
            .filter_map(|r| match &r.content {
                Content::Policy(p) => Some(p.clone()),
                _ => None,
            })
            .collect();
        if policies.len() != 1 || policies[0].expires_at <= timestamp(now)? {
            return Err(Error::Unavailable);
        }
        Ok(policies[0].clone())
    }
    fn authority(
        &self,
        scope: &Scope,
        b: &Binding,
        agency: &Authority,
        now: u64,
    ) -> Result<(Authority, Policy, Vec<UseLimit>)> {
        let policy = self.policy(scope, now)?;
        let mut effective = agency.clone();
        for a in [&policy.platform, &policy.resource, &policy.information_use] {
            effective = effective.intersection(a).cloned().collect();
        }
        let grants: Vec<_> = self
            .contents(scope)
            .into_iter()
            .filter_map(|r| match &r.content {
                Content::Grant(g) if g.subject == b.principal => Some(g),
                _ => None,
            })
            .collect();
        if grants.is_empty() {
            return Err(Error::Unavailable);
        }
        let mut limits = Vec::new();
        for g in grants {
            if g.revoked
                || g.version == 0
                || g.tenant != scope.tenant
                || g.evidence.artifact.scope != scope.tenant
                || g.expires_at <= timestamp(now)?
                || !policy.grant_issuers.contains(&g.issuer)
                || g.max_uses == 0
                || g.max_uses > 10_000
            {
                return Err(Error::Unavailable);
            }
            effective = effective.intersection(&g.authority).cloned().collect();
            limits.push(UseLimit {
                key: hash(&("grant", &g.tenant, &g.issuer, &g.id))?,
                max_uses: g.max_uses,
            });
        }
        Ok((effective, policy, limits))
    }
    fn consume(&mut self, limits: &[UseLimit]) -> Result<()> {
        // Validate the complete reservation set before changing any count.
        for l in limits {
            if self.uses.get(&l.key).copied().unwrap_or(0) >= l.max_uses {
                return Err(Error::Unavailable);
            }
        }
        for l in limits {
            *self.uses.entry(l.key.clone()).or_default() += 1;
        }
        Ok(())
    }
    fn visible(&self, scope: &Scope, authority: &Authority, r: &ObjectRef) -> Result<ObjectView> {
        if &r.scope != scope || !authority.contains(&read_demand(r)?) {
            return Err(Error::Unavailable);
        }
        self.objects.iter().find(|o| &o.reference == r).cloned().ok_or(Error::Unavailable)
    }
    fn push_revision(&mut self, r: Revision) -> Result<()> {
        let view = ObjectView {
            reference: r.reference.clone(),
            revision: r.revision,
            content: hash(&r)?,
            artifact: None,
        };
        self.objects.retain(|o| o.reference != r.reference);
        self.objects.push(view);
        if let Content::Secret(s) = &r.content {
            self.revocations.insert(
                hash(&(&r.reference, s.version, s.generation))?,
                if r.attribution.bootstrap {
                    IssuanceStatus::Delivered
                } else {
                    IssuanceStatus::Pending
                },
            );
        }
        self.history.push(r);
        Ok(())
    }
}

pub fn read_demand(reference: &ObjectRef) -> Result<ContentHash> {
    hash(&("wi-control-read-v1", reference))
}
pub fn query_demand(scope: &Scope, query: &Query) -> Result<ContentHash> {
    hash(&("wi-control-query-v1", scope, query.kind))
}
pub fn admin_demand(command: &AdminCommand) -> Result<ContentHash> {
    // Request id, claims and approval documents cannot alter the exact mutation.
    hash(&("wi-control-admin-v1", &command.reference, command.expected_revision, &command.content))
}
/// Authority covers the exact capability ceiling; fresh revision/authorization
/// bindings and expiry are additionally checked against the verified permit.
/// Excluding self-referential policy hashes avoids a recursive authority digest.
pub fn issue_demand(request: &CredentialRequest) -> Result<ContentHash> {
    let b = &request.binding;
    hash(&(
        "wi-control-issue-v1",
        &b.scope,
        &b.principal,
        b.audience,
        &b.recipient,
        &b.provider,
        &b.operation,
        &b.resource,
        &b.run,
        b.class,
        &b.secret,
        b.secret_version,
        b.generation,
    ))
}

fn validate_content(
    reference: &ObjectRef,
    content: &Content,
    old: Option<&Revision>,
    now: u64,
) -> Result<()> {
    if reference.kind != content.kind() {
        return Err(Error::Invalid);
    }
    match content {
        Content::Binding(b) => {
            if b.id != reference.id
                || b.tenant != reference.scope.tenant
                || b.capacity.organization_ref != reference.scope.organization
                || b.version != old.map_or(1, |r| r.revision + 1)
                || b.expires_at <= timestamp(now)?
            {
                return Err(Error::Invalid);
            }
            // resolve_binding validates attenuation and cycles even before activating a binding.
            if !b.revoked {
                resolve_binding(
                    &AuthenticatedCaller::from_trusted_authentication(
                        b.tenant.clone(),
                        b.principal.clone(),
                    ),
                    &IdentityClaims::default(),
                    std::slice::from_ref(b),
                    &timestamp(now)?,
                )
                .map_err(|_| Error::Invalid)?;
            }
        }
        Content::Grant(g) => {
            if g.id != reference.id
                || g.tenant != reference.scope.tenant
                || g.evidence.artifact.scope != g.tenant
                || g.version != old.map_or(1, |r| r.revision + 1)
                || g.expires_at <= timestamp(now)?
                || g.max_uses == 0
                || g.max_uses > 10_000
            {
                return Err(Error::Invalid);
            }
            if let Some(Revision { content: Content::Grant(previous), .. }) = old {
                if g.issuer != previous.issuer || g.subject != previous.subject {
                    return Err(Error::Invalid);
                }
            }
        }
        Content::Policy(p) => {
            if p.version != 1 || p.expires_at <= timestamp(now)? {
                return Err(Error::Invalid);
            }
        }
        Content::Secret(s) => {
            if s.version != old.map_or(1, |r| r.revision + 1) || s.expires_at_ms <= now {
                return Err(Error::Invalid);
            }
            if let Some(Revision { content: Content::Secret(previous), .. }) = old {
                if s.generation != previous.generation.checked_add(1).ok_or(Error::Invalid)? {
                    return Err(Error::Invalid);
                }
            } else if s.generation != 0 {
                return Err(Error::Invalid);
            }
        }
    }
    if content.authority().len() > 256 {
        return Err(Error::Invalid);
    }
    hash(content)?;
    Ok(())
}

fn principals(content: &Content) -> Vec<PrincipalId> {
    match content {
        Content::Binding(b) => std::iter::once(b.principal.clone())
            .chain(std::iter::once(b.accountable.clone()))
            .chain(b.sponsor.iter().cloned())
            .chain(b.capacity.organization_ref.iter().cloned())
            .chain(b.delegations.iter().flat_map(|d| [d.issuer.clone(), d.subject.clone()]))
            .collect(),
        Content::Grant(g) => vec![g.issuer.clone(), g.subject.clone()],
        Content::Policy(p) => std::iter::once(p.authorizer.clone())
            .chain(p.grant_issuers.iter().cloned())
            .chain(p.approvers.iter().cloned())
            .collect(),
        Content::Secret(_) => vec![],
    }
}
fn validate_principals(state: &State, scope: &Scope, content: &Content) -> Result<()> {
    if principals(content).iter().any(|p| {
        !state.objects.iter().any(|o| {
            o.reference.scope == *scope
                && o.reference.kind == ObjectKind::Principal
                && o.reference.id.as_str() == p.as_str()
        })
    }) {
        return Err(Error::Unavailable);
    }
    Ok(())
}

pub fn bootstrap(old: &State, command: &Bootstrap, now: u64) -> Result<State> {
    if old.configuration.is_some() || old.revision != 0 {
        return Err(Error::Conflict);
    }
    command.configuration.validate()?;
    if command.objects.is_empty() || command.objects.len() > 256 {
        return Err(Error::Invalid);
    }
    let binding = command
        .objects
        .iter()
        .find_map(|(_, c)| match c {
            Content::Binding(b) if b.principal == command.operator => Some(b.clone()),
            _ => None,
        })
        .ok_or(Error::Invalid)?;
    let mut new = old.clone();
    new.configuration = Some(command.configuration.clone());
    let mut seen = BTreeSet::new();
    for (reference, content) in &command.objects {
        if reference.scope.tenant != command.configuration.status.tenant
            || !seen.insert(reference.clone())
        {
            return Err(Error::Invalid);
        }
        validate_content(reference, content, None, now)?;
        // Bootstrap is the host's explicit ownership attestation. Later commands
        // must refer to these principals or to trusted catalog registrations.
        for principal in principals(content) {
            let reference = ObjectRef {
                scope: reference.scope.clone(),
                kind: ObjectKind::Principal,
                id: principal.as_str().to_string().try_into().map_err(|_| Error::Invalid)?,
            };
            if !new.objects.iter().any(|o| o.reference == reference) {
                new.objects.push(ObjectView {
                    reference,
                    revision: 1,
                    content: hash(&principal)?,
                    artifact: None,
                });
            }
        }
        new.push_revision(Revision {
            schema: 1,
            reference: reference.clone(),
            revision: 1,
            previous: None,
            content_digest: hash(content)?,
            content: content.clone(),
            attribution: Attribution {
                requester: command.operator.clone(),
                actor: binding.actor.clone(),
                accountable: binding.accountable.clone(),
                capacity: binding.capacity.clone(),
                authorizer: command.operator.clone(),
                approvals: vec![],
                at_ms: now,
                request: command.request.clone(),
                command: hash(command)?,
                bootstrap: true,
            },
        })?;
    }
    new.revision = 1;
    new.last_at_ms = now;
    Ok(new)
}

fn approvals(
    policy: &Policy,
    b: &Binding,
    command: &AdminCommand,
    now: u64,
) -> Result<Vec<UseLimit>> {
    if command.approvals.len() > 64 || policy.approval_required && command.approvals.is_empty() {
        return Err(Error::Unavailable);
    }
    let exact = hash(&(
        "wi-control-approval-v1",
        &command.reference.scope,
        admin_demand(command)?,
        hash(policy)?,
        hash(b)?,
    ))?;
    let mut ids = BTreeSet::new();
    let mut limits = Vec::new();
    for a in &command.approvals {
        if !ids.insert(&a.id)
            || a.version == 0
            || a.revoked
            || a.expires_at <= timestamp(now)?
            || a.exact_binding != exact
            || a.evidence.artifact.scope != command.reference.scope.tenant
            || !policy.approvers.contains(&a.approver)
            || a.max_uses == 0
            || a.max_uses > 10_000
            || policy.separation_of_duties
                && (a.approver == b.principal
                    || a.approver == b.accountable
                    || Some(&a.approver) == b.sponsor.as_ref()
                    || a.approver == policy.authorizer)
        {
            return Err(Error::Unavailable);
        }
        limits.push(UseLimit {
            key: hash(&("approval", &command.reference.scope.tenant, &a.approver, &a.id))?,
            max_uses: a.max_uses,
        });
    }
    Ok(limits)
}
pub fn administration_approval_binding(
    policy: &Policy,
    binding: &Binding,
    command: &AdminCommand,
) -> Result<ContentHash> {
    hash(&(
        "wi-control-approval-v1",
        &command.reference.scope,
        admin_demand(command)?,
        hash(policy)?,
        hash(binding)?,
    ))
}

pub fn apply(
    old: &State,
    caller: &AuthenticatedCaller,
    request: &Request,
    now: u64,
) -> Result<(State, Outcome)> {
    if now < old.last_at_ms {
        return Err(Error::Invalid);
    }
    if caller.tenant() != &old.config()?.status.tenant {
        return Err(Error::Unavailable);
    }
    let key = hash(&(caller.tenant(), caller.principal(), &request.id))?;
    let exact = hash(request)?;
    // Acknowledging an already committed administrative command grants no new
    // authority, including after that command revokes the requester's grant.
    if let Some(retry) = old.requests.get(&key) {
        if retry.digest != exact {
            return Err(Error::Conflict);
        }
        if matches!(request.operation, Operation::Admin { .. }) {
            return Ok((old.clone(), retry.outcome.clone()));
        }
    }
    let claims = match &request.operation {
        Operation::Admin { command } => command.claims.clone(),
        _ => IdentityClaims::default(),
    };
    let (scope, b, agency) = old.scope(caller, &claims, now)?;
    let (authority, policy, mut limits) = old.authority(&scope, &b, &agency, now)?;
    if let Some(retry) = old.requests.get(&key) {
        if retry.digest != exact {
            return Err(Error::Conflict);
        }
        let allowed = match &request.operation {
            Operation::Get { reference } => old.visible(&scope, &authority, reference).is_ok(),
            Operation::Query { query } => {
                authority.contains(&query_demand(&scope, query)?)
                    && match &retry.outcome {
                        Outcome::Page(p) => p
                            .objects
                            .iter()
                            .all(|o| old.visible(&scope, &authority, &o.reference).is_ok()),
                        _ => false,
                    }
            }
            Operation::Admin { command } => authority.contains(&admin_demand(command)?),
            Operation::Issue { request } => {
                authority.contains(&issue_demand(request)?)
                    && validate_credential(old, request, now).is_ok()
            }
        };
        if !allowed {
            return Err(Error::Unavailable);
        }
        return Ok((old.clone(), retry.outcome.clone()));
    }
    let mut new = old.clone();
    let outcome = match &request.operation {
        Operation::Get { reference } => {
            Outcome::Object(old.visible(&scope, &authority, reference)?)
        }
        Operation::Query { query } => {
            if query.limit == 0
                || query.limit > 100
                || query.tenant_filter.as_ref().is_some_and(|t| t != &scope.tenant)
                || !authority.contains(&query_demand(&scope, query)?)
            {
                return Err(Error::Unavailable);
            }
            // Bind cursor to caller, organization, exact query, current authority and projection.
            let cursor_binding = collection_hash(&(
                &scope,
                caller.principal(),
                &b,
                &authority,
                query.kind,
                &query.tenant_filter.iter().collect::<Vec<_>>(),
                query.limit,
                &old.objects
                    .iter()
                    .filter(|o| old.visible(&scope, &authority, &o.reference).is_ok())
                    .collect::<Vec<_>>(),
            ))?;
            if query.cursor.as_ref().is_some_and(|c| c.binding != cursor_binding) {
                return Err(Error::Unavailable);
            }
            let mut objects: Vec<_> = old
                .objects
                .iter()
                .filter(|o| o.reference.kind == query.kind)
                .filter_map(|o| old.visible(&scope, &authority, &o.reference).ok())
                .collect();
            objects.sort_by(|a, b| a.reference.id.cmp(&b.reference.id));
            let visible_count = objects.len() as u64;
            if let Some(c) = &query.cursor {
                objects.retain(|o| o.reference.id > c.after);
            }
            let more = objects.len() > query.limit as usize;
            objects.truncate(query.limit as usize);
            let next = if more {
                objects
                    .last()
                    .map(|o| Cursor { binding: cursor_binding, after: o.reference.id.clone() })
            } else {
                None
            };
            Outcome::Page(Page { objects, visible_count, next })
        }
        Operation::Admin { command } => {
            if command.request != request.id
                || command.reference.scope != scope
                || !authority.contains(&admin_demand(command)?)
                || !command.content.authority().is_subset(&authority)
            {
                return Err(Error::Unavailable);
            }
            let previous = old.current(&command.reference);
            if previous.map_or(0, |r| r.revision) != command.expected_revision {
                return Err(Error::Conflict);
            }
            validate_content(&command.reference, &command.content, previous, now)?;
            validate_principals(old, &scope, &command.content)?;
            if let Content::Policy(_) = command.content {
                if old.contents(&scope).iter().any(|r| {
                    r.reference.kind == ObjectKind::Policy && r.reference != command.reference
                }) {
                    return Err(Error::Invalid);
                }
            }
            // Issuer/authorizer provenance cannot be assigned to an unrelated principal.
            if matches!(&command.content, Content::Grant(g) if previous.is_none() && (g.issuer != b.accountable || !policy.grant_issuers.contains(&g.issuer)))
                || matches!(&command.content, Content::Policy(p) if p.authorizer != b.accountable)
            {
                return Err(Error::Unavailable);
            }
            limits.extend(approvals(&policy, &b, command, now)?);
            let r = Revision {
                schema: 1,
                reference: command.reference.clone(),
                revision: command.expected_revision.checked_add(1).ok_or(Error::Invalid)?,
                previous: previous.map(hash).transpose()?,
                content_digest: hash(&command.content)?,
                content: command.content.clone(),
                attribution: Attribution {
                    requester: caller.principal().clone(),
                    actor: b.actor.clone(),
                    accountable: b.accountable.clone(),
                    capacity: b.capacity.clone(),
                    authorizer: policy.authorizer.clone(),
                    approvals: command.approvals.iter().map(|a| a.id.clone()).collect(),
                    at_ms: now,
                    request: request.id.clone(),
                    command: exact.clone(),
                    bootstrap: false,
                },
            };
            new.push_revision(r.clone())?;
            Outcome::Revision(Box::new(r))
        }
        Operation::Issue { request: credential } => {
            if credential.request != request.id
                || credential.binding.scope != scope
                || credential.binding.principal != *caller.principal()
                || credential.permit.scope != scope
                || !authority.contains(&issue_demand(credential)?)
            {
                return Err(Error::Unavailable);
            }
            validate_credential(old, credential, now)?;
            let permit = old
                .permits
                .iter()
                .find(|p| p.reference == credential.permit)
                .ok_or(Error::Unavailable)?;
            if !authority.contains(&permit.demand) {
                return Err(Error::Unavailable);
            }
            limits.extend(permit.reservations.clone());
            // A logical use shared by both paths is reserved once.
            limits.sort_by(|a, b| a.key.cmp(&b.key));
            limits.dedup_by(|a, b| a.key == b.key);
            let id =
                hash(&("wi-issuance-v1", &scope, caller.principal(), &request.id, credential))?;
            let m = CredentialMetadata {
                issuance: id.clone(),
                permit: credential.permit.clone(),
                binding: credential.binding.clone(),
                status: IssuanceStatus::Pending,
                reason: None,
            };
            new.issuances.insert(id, m.clone());
            Outcome::Credential(Box::new(m))
        }
    };
    new.consume(&limits)?;
    new.requests.insert(key, Retry { digest: exact, outcome: outcome.clone() });
    new.revision = new.revision.checked_add(1).ok_or(Error::Invalid)?;
    new.last_at_ms = now;
    Ok((new, outcome))
}

fn validate_credential(s: &State, request: &CredentialRequest, now: u64) -> Result<()> {
    let b = &request.binding;
    let permit =
        s.permits.iter().find(|p| p.reference == request.permit).ok_or(Error::Unavailable)?;
    let mut expected = permit.binding.clone();
    expected.expires_at_ms = b.expires_at_ms;
    if permit.revoked
        || b != &expected
        || b.expires_at_ms <= now
        || b.expires_at_ms
            > now.checked_add(s.config()?.max_credential_lifetime_ms).ok_or(Error::Invalid)?
        || b.expires_at_ms > permit.expires_at_ms
        || s.config()?.status.profile == Profile::LocalDevelopment
            && b.class == CredentialClass::Production
        || b.audience == Audience::Worker && b.class == CredentialClass::Production
    {
        return Err(Error::Unavailable);
    }
    if b.revisions != s.authority_revisions(&b.scope, &b.principal)? {
        return Err(Error::Unavailable);
    }
    for (id, revision) in &b.revisions {
        if !s
            .contents(&b.scope)
            .iter()
            .any(|r| &r.reference.id == id && hash(r).ok().as_ref() == Some(revision))
        {
            return Err(Error::Unavailable);
        }
    }
    let r = s.current(&b.secret).ok_or(Error::Unavailable)?;
    let Content::Secret(secret) = &r.content else {
        return Err(Error::Unavailable);
    };
    if b.secret.scope != b.scope
        || secret.revoked
        || secret.version != b.secret_version
        || secret.generation != b.generation
        || b.expires_at_ms > secret.expires_at_ms
        || s.revocations.get(&hash(&(&b.secret, secret.version, secret.generation))?)
            != Some(&IssuanceStatus::Delivered)
    {
        return Err(Error::Unavailable);
    }
    Ok(())
}

/// Trusted ingestion derives exact scope from the existing effect authorization
/// engine, approved resource resolution and *current* managed revisions.
pub fn prepare_permit(
    s: &State,
    caller: &AuthenticatedCaller,
    intent: &EffectIntent,
    verified: &VerifiedSnapshot,
    input: PermitInput,
    now: u64,
) -> Result<Permit> {
    let (scope, binding, _) = s.scope(caller, &IdentityClaims::default(), now)?;
    let facts = verified.facts();
    let agency = resolve_agency(caller, &intent.submission, &facts.bindings, &timestamp(now)?)
        .map_err(|_| Error::Unavailable)?;
    if agency.binding != binding || s.policy(&scope, now)? != facts.policy {
        return Err(Error::Unavailable);
    }
    let resolution = crate::resource::resolve_resource(
        intent,
        &scope.tenant,
        &facts.registry,
        &facts.facts,
        &timestamp(now)?,
    )
    .map_err(|_| Error::Unavailable)?;
    let evaluation = authorize(intent, &agency, &resolution, verified, &timestamp(now)?)
        .map_err(|_| Error::Unavailable)?;
    if evaluation.decision != Decision::Allow {
        return Err(Error::Unavailable);
    }
    let (audience, provider, operation, run) = match &resolution.fact.document.target {
        crate::resource::Target::Worker { provider, operation, run, .. } => {
            (Audience::Worker, provider.clone(), operation.clone(), run.clone())
        }
        crate::resource::Target::Connector { provider, operation, .. } => {
            (Audience::Connector, provider.clone(), operation.clone(), input.run.clone())
        }
        _ => return Err(Error::Unavailable),
    };
    if input.reference.kind != ObjectKind::Authorization
        || input.reference.scope != scope
        || input.secret.scope != scope
        || input.secret.kind != ObjectKind::Secret
        || input.run != run
        || audience == Audience::Worker && input.class == CredentialClass::Production
    {
        return Err(Error::Unavailable);
    }
    let mut revisions = BTreeMap::new();
    // Exact managed bindings, policy and grants, including every referenced version.
    for content in std::iter::once(Content::Policy(facts.policy.clone()))
        .chain(std::iter::once(Content::Binding(binding)))
        .chain(facts.grants.iter().cloned().map(Content::Grant))
    {
        let r = s
            .contents(&scope)
            .into_iter()
            .find(|r| r.content == content)
            .ok_or(Error::Unavailable)?;
        if revisions.insert(r.reference.id.clone(), hash(r)?).is_some() {
            return Err(Error::Invalid);
        }
    }
    if revisions != s.authority_revisions(&scope, caller.principal())? {
        return Err(Error::Unavailable);
    }
    let r = s.current(&input.secret).ok_or(Error::Unavailable)?;
    let Content::Secret(secret) = &r.content else {
        return Err(Error::Unavailable);
    };
    let expires_at_ms = input
        .run_expires_at_ms
        .min(evaluation.expires_at.epoch_millis().map_err(|_| Error::Invalid)?)
        .min(secret.expires_at_ms);
    if expires_at_ms <= now {
        return Err(Error::Unavailable);
    }
    Ok(Permit {
        reference: input.reference,
        binding: CredentialScope {
            scope,
            principal: caller.principal().clone(),
            audience,
            recipient: input.recipient,
            provider,
            operation,
            resource: resolution.resolved.resource,
            run,
            authorization: evaluation.exact_binding,
            revisions,
            class: input.class,
            secret: input.secret,
            secret_version: secret.version,
            generation: secret.generation,
            expires_at_ms,
        },
        expires_at_ms,
        reservations: evaluation.reservations,
        demand: hash(&intent.submission)?,
        revoked: false,
    })
}
/// Trusted host chooses the recipient endpoint and attests the current run deadline.
pub struct PermitInput {
    pub reference: ObjectRef,
    pub secret: ObjectRef,
    pub recipient: OpaqueRef,
    pub run: OpaqueRef,
    pub run_expires_at_ms: u64,
    pub class: CredentialClass,
}
impl Permit {
    pub fn binding(&self) -> &CredentialScope {
        &self.binding
    }
}

pub fn register_permit(old: &State, permit: &Permit) -> Result<State> {
    if old.permits.iter().any(|p| p.reference == permit.reference) {
        return Err(Error::Conflict);
    }
    let mut new = old.clone();
    new.objects.push(ObjectView {
        reference: permit.reference.clone(),
        revision: 1,
        content: hash(permit)?,
        artifact: None,
    });
    new.permits.push(permit.clone());
    new.revision += 1;
    Ok(new)
}
/// Trusted host withdrawal when run, approval or external evidence is invalidated.
pub fn withdraw_permit(old: &State, reference: &ObjectRef) -> Result<State> {
    let mut new = old.clone();
    let permit =
        new.permits.iter_mut().find(|p| &p.reference == reference).ok_or(Error::Unavailable)?;
    if permit.revoked {
        return Ok(old.clone());
    }
    permit.revoked = true;
    let content = hash(permit)?;
    if let Some(object) = new.objects.iter_mut().find(|o| &o.reference == reference) {
        object.revision += 1;
        object.content = content;
    }
    new.revision += 1;
    Ok(new)
}

pub fn register_object(old: &State, object: &ObjectView) -> Result<State> {
    if object.reference.scope.tenant != old.config()?.status.tenant
        || object.revision == 0
        || matches!(
            object.reference.kind,
            ObjectKind::Policy
                | ObjectKind::Grant
                | ObjectKind::Binding
                | ObjectKind::Secret
                | ObjectKind::Authorization
        )
        || object.artifact.as_ref().is_some_and(|a| a.scope != object.reference.scope.tenant)
    {
        return Err(Error::Invalid);
    }
    if old.objects.iter().any(|o| o.reference == object.reference && o.revision >= object.revision)
    {
        return Err(Error::Conflict);
    }
    let mut new = old.clone();
    new.objects.retain(|o| o.reference != object.reference);
    new.objects.push(object.clone());
    new.revision += 1;
    Ok(new)
}

/// Custody must enforce exact binding, deadline, recipient and generation itself.
/// It delivers directly; errors contain only bounded reason codes. On uncertainty
/// the adapter must invalidate any possibly delivered capability before returning.
pub trait Custody {
    fn deliver(
        &self,
        custody: &OpaqueRef,
        metadata: &CredentialMetadata,
    ) -> std::result::Result<(), CustodyReason>;
    fn revoke(
        &self,
        secret: &ObjectRef,
        version: u64,
        generation: u64,
    ) -> std::result::Result<(), CustodyReason>;
}

/// Revalidate within the store's exclusive delivery/revocation transaction.
pub fn delivery_reference(s: &State, id: &ContentHash, now: u64) -> Result<OpaqueRef> {
    if now < s.last_at_ms {
        return Err(Error::Unavailable);
    }
    let m = s.issuances.get(id).ok_or(Error::Unavailable)?;
    if m.status != IssuanceStatus::Pending {
        return Err(Error::Unresolved);
    }
    let permit = s.permits.iter().find(|p| p.reference == m.permit).ok_or(Error::Unavailable)?;
    validate_credential(
        s,
        &CredentialRequest {
            request: "delivery".to_string().try_into().map_err(|_| Error::Invalid)?,
            permit: permit.reference.clone(),
            binding: m.binding.clone(),
        },
        now,
    )?;
    // Also enforce current agency/grant revocation/expiry at delivery time.
    let caller = AuthenticatedCaller::from_trusted_authentication(
        m.binding.scope.tenant.clone(),
        m.binding.principal.clone(),
    );
    let (scope, b, a) = s.scope(&caller, &IdentityClaims::default(), now)?;
    let (effective, _, _) = s.authority(&scope, &b, &a, now)?;
    let request = CredentialRequest {
        request: "delivery".to_string().try_into().map_err(|_| Error::Invalid)?,
        permit: m.permit.clone(),
        binding: m.binding.clone(),
    };
    if !effective.contains(&permit.demand) || !effective.contains(&issue_demand(&request)?) {
        return Err(Error::Unavailable);
    }
    match &s.current(&m.binding.secret).ok_or(Error::Unavailable)?.content {
        Content::Secret(secret) => Ok(secret.custody.clone()),
        _ => Err(Error::Unavailable),
    }
}
pub fn finish_delivery(
    old: &State,
    id: &ContentHash,
    result: std::result::Result<(), CustodyReason>,
) -> Result<State> {
    let mut new = old.clone();
    let m = new.issuances.get_mut(id).ok_or(Error::Unavailable)?;
    if m.status != IssuanceStatus::Pending {
        return Err(Error::Unresolved);
    }
    match result {
        Ok(()) => m.status = IssuanceStatus::Delivered,
        Err(reason) => {
            m.status = if reason == CustodyReason::Rejected || reason == CustodyReason::Stale {
                IssuanceStatus::Rejected
            } else {
                IssuanceStatus::Unresolved
            };
            m.reason = Some(reason);
        }
    }
    new.revision += 1;
    Ok(new)
}
pub fn finish_revocation(
    old: &State,
    secret: &ObjectRef,
    version: u64,
    generation: u64,
    result: std::result::Result<(), CustodyReason>,
) -> Result<State> {
    let key = hash(&(secret, version, generation))?;
    if old.revocations.get(&key) != Some(&IssuanceStatus::Pending) {
        return Err(Error::Unresolved);
    }
    let mut new = old.clone();
    new.revocations.insert(
        key,
        if result.is_ok() { IssuanceStatus::Delivered } else { IssuanceStatus::Unresolved },
    );
    new.revision += 1;
    Ok(new)
}

/// Store implementation serializes mutations with delivery/revocation, commits
/// issuance intent before calling custody, and never repeats an uncertain call.
pub trait Repository {
    fn execute(
        &self,
        caller: &AuthenticatedCaller,
        request: &Request,
        now: u64,
    ) -> Result<(Outcome, bool)>;
    fn deliver(
        &self,
        id: &ContentHash,
        clock: &impl TrustedClock,
        custody: &impl Custody,
        fresh: bool,
    ) -> Result<CredentialMetadata>;
    fn revoke(&self, revision: &Revision, custody: &impl Custody, fresh: bool) -> Result<()>;
}
pub struct Service<R, A, C, K> {
    repository: R,
    authentication: A,
    clock: C,
    custody: K,
}
impl<R: Repository, A: Authentication, C: TrustedClock, K: Custody> Service<R, A, C, K> {
    pub fn new(repository: R, authentication: A, clock: C, custody: K) -> Self {
        Self { repository, authentication, clock, custody }
    }
    pub fn execute(&self, session: &A::Session, request: &Request) -> Result<Outcome> {
        let caller = self.authentication.authenticate(session)?;
        let (outcome, fresh) = self.repository.execute(&caller, request, self.clock.now_ms())?;
        match outcome {
            Outcome::Credential(m) => Ok(Outcome::Credential(Box::new(self.repository.deliver(
                &m.issuance,
                &self.clock,
                &self.custody,
                fresh,
            )?))),
            Outcome::Revision(r) => {
                if matches!(r.content, Content::Secret(_)) {
                    self.repository.revoke(&r, &self.custody, fresh)?;
                }
                Ok(Outcome::Revision(r))
            }
            other => Ok(other),
        }
    }
}
