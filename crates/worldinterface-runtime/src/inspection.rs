//! Authenticated receipt and descriptive evidence inspection.
use crate::{
    authority::{caller, Authentication},
    Result,
};
use std::collections::BTreeMap;
use worldinterface_domain::authorization::RequiredControl;
use worldinterface_domain::{
    developmental::{self, CampaignArmV1, DevelopmentalComparisonV1, DevelopmentalSnapshotV1},
    receipt::{DisclosureProfile, ReceiptAccess, ReceiptView},
};
use worldinterface_protocol::{
    evidence::{ClassifiedEvidenceV1, CompletenessClaimV1, EvidenceClass, EvidenceGapV1},
    receipt::ProofBundleV1,
    ArtifactRef, ContentHash, EffectIntentId, EvidenceRef, OpaqueRef, ReceiptId,
};
use worldinterface_storage::StoreRoot;

/// A host authorizer chooses the maximum disclosure for each authenticated caller.
pub trait ReceiptDisclosure<A: Authentication> {
    fn profile(
        &self,
        session: &A::Session,
        tenant: &OpaqueRef,
        receipt: &ReceiptId,
    ) -> Result<DisclosureProfile>;
    fn effect_profile(
        &self,
        session: &A::Session,
        tenant: &OpaqueRef,
        intent: &EffectIntentId,
    ) -> Result<DisclosureProfile>;
    fn bundle_allowed(
        &self,
        session: &A::Session,
        tenant: &OpaqueRef,
        id: &OpaqueRef,
    ) -> Result<bool>;
    /// Additional bundle evidence has its own disclosure decision. Receipt
    /// ownership and artifact availability do not authorize these references.
    fn evidence_allowed(
        &self,
        _session: &A::Session,
        _tenant: &OpaqueRef,
        _reference: &EvidenceRef,
    ) -> Result<bool> {
        Ok(false)
    }
}

pub struct ReceiptInspector<'a, A, D> {
    pub root: &'a StoreRoot,
    pub authentication: &'a A,
    pub disclosure: &'a D,
}

#[derive(Clone, Debug)]
pub struct EffectChain {
    pub intent: EffectIntentId,
    pub admission: Option<ContentHash>,
    pub runs: Vec<OpaqueRef>,
    pub attempt: Option<OpaqueRef>,
    pub receipts: Vec<(ReceiptId, ContentHash, u64)>,
    pub publications: Vec<ArtifactRef>,
}
#[derive(Clone, Debug)]
pub struct WorkerControlInspection {
    pub enclosing_instance: Option<EvidenceRef>,
    pub controls: BTreeMap<OpaqueRef, RequiredControl>,
    pub missing: Vec<OpaqueRef>,
    pub worker_enforcement_evidence: Vec<EvidenceRef>,
    pub producer_report: Option<worldinterface_worker::evidence::WorkerEvidenceReportV1>,
}

impl<A: Authentication, D: ReceiptDisclosure<A>> ReceiptInspector<'_, A, D> {
    fn access(&self, session: &A::Session, receipt: &ReceiptId) -> Result<ReceiptAccess> {
        let principal = caller(self.authentication, session)?;
        let profile = self.disclosure.profile(session, principal.tenant(), receipt)?;
        Ok(ReceiptAccess::from_trusted_authorizer(
            principal.tenant().clone(),
            principal.principal().clone(),
            profile,
        ))
    }
    pub fn receipt(
        &self,
        session: &A::Session,
        receipt: &ReceiptId,
    ) -> Result<Option<ReceiptView>> {
        self.root.inspect_receipt(&self.access(session, receipt)?, receipt)
    }
    pub fn proof_bundle(
        &self,
        session: &A::Session,
        id: &OpaqueRef,
        receipt: &ReceiptId,
    ) -> Result<Option<ProofBundleV1>> {
        let access = self.access(session, receipt)?;
        if access.disclosure() != DisclosureProfile::Full
            || !self.disclosure.bundle_allowed(session, access.tenant(), id)?
        {
            return Err("proof disclosure denied".into());
        }
        let receipt_view =
            self.root.inspect_receipt(&access, receipt)?.ok_or("proof receipt unavailable")?;
        let Some(bundle) = self.root.inspect_proof_bundle(&access, id, |reference| {
            self.disclosure.evidence_allowed(session, access.tenant(), reference)
        })?
        else {
            return Ok(None);
        };
        if !receipt_view.digest.as_ref().is_some_and(|digest| bundle.records.contains(digest)) {
            return Err("proof bundle does not bind requested receipt".into());
        }
        for member in self.root.proof_bundle_receipts(access.tenant(), &bundle)? {
            let member_access = self.access(session, &member)?;
            if member_access.disclosure() != DisclosureProfile::Full
                || self.root.inspect_receipt(&member_access, &member)?.is_none()
            {
                return Err("proof bundle member disclosure denied".into());
            }
        }
        Ok(Some(bundle))
    }
    pub fn receipt_proofs(
        &self,
        session: &A::Session,
        receipt: &ReceiptId,
    ) -> Result<Vec<(ContentHash, worldinterface_protocol::receipt::SigningProofV1)>> {
        self.root.receipt_proofs(&self.access(session, receipt)?, receipt)
    }
    pub fn effect_chain(&self, session: &A::Session, id: &EffectIntentId) -> Result<EffectChain> {
        let principal = caller(self.authentication, session)?;
        let disclosure = self.disclosure.effect_profile(session, principal.tenant(), id)?;
        let state = self.root.effect_snapshot(principal.tenant())?;
        let effect = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == principal.principal())
            .ok_or("effect unavailable")?;
        let admission = effect.dispatches.last().and_then(|d| d.admission.as_ref());
        let receipts = self.root.effect_receipts(principal.tenant(), id)?;
        // Every receipt disclosure is checked independently by the host.
        for (receipt, _, _) in &receipts {
            self.access(session, receipt)?;
        }
        Ok(EffectChain {
            intent: id.clone(),
            admission: admission.map(|a| a.canonical_digest.clone()),
            runs: admission.map(|a| a.observed_runs.clone()).unwrap_or_default(),
            attempt: effect.attempt.clone(),
            receipts,
            publications: if disclosure == DisclosureProfile::Full {
                effect.outcomes.last().map(|o| o.durable_outputs.clone()).unwrap_or_default()
            } else {
                Vec::new()
            },
        })
    }
    pub fn developmental(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
        campaign: Option<CampaignArmV1>,
    ) -> Result<DevelopmentalSnapshotV1> {
        let principal = caller(self.authentication, session)?;
        if self.disclosure.effect_profile(session, principal.tenant(), id)?
            != DisclosureProfile::Full
        {
            return Err("developmental disclosure denied".into());
        }
        let state = self.root.effect_snapshot(principal.tenant())?;
        let effect = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == principal.principal())
            .ok_or("effect unavailable")?;
        Ok(developmental::inspect(effect, campaign)?)
    }
    pub fn compare(
        &self,
        session: &A::Session,
        left: &EffectIntentId,
        right: &EffectIntentId,
        left_arm: Option<CampaignArmV1>,
        right_arm: Option<CampaignArmV1>,
    ) -> Result<DevelopmentalComparisonV1> {
        let a = self.developmental(session, left, left_arm)?;
        let b = self.developmental(session, right, right_arm)?;
        Ok(developmental::compare_arms(&a, &b)?)
    }
    pub fn authorization_parity(
        &self,
        session: &A::Session,
        left: &EffectIntentId,
        right: &EffectIntentId,
    ) -> Result<Vec<(OpaqueRef, developmental::Comparison)>> {
        let comparison = self.compare(session, left, right, None, None)?;
        let names = [
            "agency",
            "resource",
            "policy",
            "grants",
            "approvals",
            "information_use",
            "connector",
            "environment",
            "receipt_profile",
        ];
        Ok(comparison
            .fields
            .into_iter()
            .filter(|(name, _)| names.contains(&name.as_str()))
            .collect())
    }
    pub fn worker_controls(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
    ) -> Result<WorkerControlInspection> {
        let principal = caller(self.authentication, session)?;
        if self.disclosure.effect_profile(session, principal.tenant(), id)?
            != DisclosureProfile::Full
        {
            return Err("worker evidence disclosure denied".into());
        }
        let state = self.root.effect_snapshot(principal.tenant())?;
        let effect = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == principal.principal())
            .ok_or("effect unavailable")?;
        let worker = effect.authorization_inputs.as_ref().and_then(|a| a.worker.as_ref());
        let controls = worker.map(|w| w.controls.clone()).unwrap_or_default();
        let mut missing = worker
            .map(|w| {
                w.mandatory
                    .iter()
                    .filter(|name| {
                        controls.get(*name).is_none_or(|control| {
                            !matches!(
                                control.enforcement,
                                worldinterface_domain::authorization::Enforcement::Enforced
                            )
                        })
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_else(Vec::new);
        if worker.is_none() {
            missing.push("worker-profile-missing".to_string().try_into()?);
        }
        let producer_report = self.worker_evidence(session, id)?;
        if let Some(report) = &producer_report {
            for kind in &report.missing {
                let name = serde_json::to_value(kind)?;
                let reference: OpaqueRef =
                    name.as_str().ok_or("invalid missing worker control")?.to_owned().try_into()?;
                if !missing.contains(&reference) {
                    missing.push(reference);
                }
            }
        }
        Ok(WorkerControlInspection {
            // Authorization realization evidence does not attest an enclosing instance.
            enclosing_instance: producer_report
                .as_ref()
                .and_then(|r| r.enclosing.as_ref().map(|e| e.evidence.clone())),
            controls,
            missing,
            // A control value in the authorization snapshot is not a separate
            // worker enforcement attestation.
            worker_enforcement_evidence: Vec::new(),
            producer_report,
        })
    }
    /// Read the producer's incomplete report through its authoritative receipt.
    /// This preserves claims and gaps; it confers no worker enforcement authority.
    pub fn worker_evidence(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
    ) -> Result<Option<worldinterface_worker::evidence::WorkerEvidenceReportV1>> {
        let principal = caller(self.authentication, session)?;
        if self.disclosure.effect_profile(session, principal.tenant(), id)?
            != DisclosureProfile::Full
        {
            return Err("worker evidence disclosure denied".into());
        }
        let state = self.root.effect_snapshot(principal.tenant())?;
        let effect = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == principal.principal())
            .ok_or("effect unavailable")?;
        let Some((receipt, _, _)) =
            self.root.effect_receipts(principal.tenant(), id)?.last().cloned()
        else {
            return Ok(None);
        };
        let access = self.access(session, &receipt)?;
        if access.disclosure() != DisclosureProfile::Full {
            return Err("worker receipt disclosure denied".into());
        }
        let view =
            self.root.inspect_receipt(&access, &receipt)?.ok_or("worker receipt unavailable")?;
        let worldinterface_protocol::Record::BoundaryReceipt(receipt) =
            view.canonical.ok_or("worker receipt redacted")?.record
        else {
            return Err("invalid worker receipt".into());
        };
        let reports: Vec<_> = receipt
            .evidence
            .iter()
            .filter(|e| e.schema.as_str() == "wi.worker-evidence:1")
            .collect();
        if reports.len() > 1 {
            return Err("ambiguous worker report".into());
        }
        let Some(reference) = reports.first() else {
            return Ok(None);
        };
        let bytes = self.root.read_artifact(&reference.artifact)?;
        let report: worldinterface_worker::evidence::WorkerEvidenceReportV1 =
            serde_json::from_value(worldinterface_protocol::strict_json(&bytes)?)?;
        report.validate()?;
        let worker = effect
            .authorization_inputs
            .as_ref()
            .and_then(|a| a.worker.as_ref())
            .ok_or("worker binding missing")?;
        if worldinterface_protocol::canonical_data(&report)? != bytes
            || report.binding.principal != *principal.principal()
            || report.binding.artifact_scope != *principal.tenant()
            || report.binding.executable != worker.manifest.executable
            || report.binding.dependencies != worker.manifest.dependencies
            || report.binding.configuration != worker.manifest.configuration
            || report.binding.adapter != worker.manifest.adapter
            || report.binding.inputs != vec![effect.intent.submission.input.artifact.clone()]
            || report.binding.run != worker.run
            || report.binding.resource != worker.resource
            || report.binding.aq_revision != worker.aq_version
            || report.binding.wi_revision != worker.wi_version
            || report.binding.authorization != worker.evidence
            || !effect
                .authorization_inputs
                .as_ref()
                .unwrap()
                .grants
                .iter()
                .any(|g| g.evidence == report.binding.grant)
            || report
                .enclosing
                .as_ref()
                .is_some_and(|e| e.evidence.artifact.scope != *principal.tenant())
        {
            return Err("worker report differs from receipt source".into());
        }
        if let Some(enclosing) = &report.enclosing {
            self.root.read_artifact(&enclosing.evidence.artifact)?;
        }
        Ok(Some(report))
    }
    /// This derived view keeps connector assertions separate from verified
    /// counterparty evidence and records absent worker controls as gaps.
    pub fn completeness(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
        log_policy: EvidenceRef,
    ) -> Result<CompletenessClaimV1> {
        let principal = caller(self.authentication, session)?;
        if self.disclosure.effect_profile(session, principal.tenant(), id)?
            != DisclosureProfile::Full
        {
            return Err("evidence disclosure denied".into());
        }
        if log_policy.artifact.scope != *principal.tenant() {
            return Err("cross-tenant log policy".into());
        }
        self.root.read_artifact(&log_policy.artifact)?;
        let state = self.root.effect_snapshot(principal.tenant())?;
        let effect = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == principal.principal())
            .ok_or("effect unavailable")?;
        let mut evidence = Vec::new();
        if let Some(inputs) = &effect.authorization_inputs {
            for grant in &inputs.grants {
                evidence.push(ClassifiedEvidenceV1 {
                    version: 1,
                    class: EvidenceClass::Authorization,
                    reference: grant.evidence.clone(),
                    source: "grant-snapshot".to_string().try_into()?,
                    verified: false,
                });
            }
        }
        if let Some(outcome) = effect.outcomes.last() {
            for reference in &outcome.evidence {
                evidence.push(ClassifiedEvidenceV1 {
                    version: 1,
                    class: EvidenceClass::SelfAssertion,
                    reference: reference.clone(),
                    source: "execution-port-assertion".to_string().try_into()?,
                    verified: false,
                });
            }
            if let Some(reference) = &outcome.boundary_record {
                evidence.push(ClassifiedEvidenceV1 {
                    version: 1,
                    class: EvidenceClass::SelfAssertion,
                    reference: reference.clone(),
                    source: "connector-boundary-assertion".to_string().try_into()?,
                    verified: false,
                });
            }
        }
        let mut gaps = Vec::new();
        for (class, reason) in [
            (EvidenceClass::Provider, "provider-evidence-missing"),
            (EvidenceClass::Transparency, "transparency-evidence-missing"),
            (EvidenceClass::Executor, "worker-enforcement-evidence-missing"),
        ] {
            gaps.push(EvidenceGapV1 {
                expected_class: class,
                reason: reason.to_string().try_into()?,
            });
        }
        if effect.outcomes.is_empty() {
            gaps.push(EvidenceGapV1 {
                expected_class: EvidenceClass::Executor,
                reason: "execution-outcome-missing".to_string().try_into()?,
            });
        }
        let through = state.last_at.clone().unwrap_or_else(|| effect.intent.submitted_at.clone());
        let claim = CompletenessClaimV1 {
            version: 1,
            scope: principal.tenant().clone(),
            from: effect.intent.submitted_at.clone(),
            through,
            log_policy,
            evidence,
            gaps,
        };
        claim.validate()?;
        Ok(claim)
    }
}

impl<A: Authentication, D: ReceiptDisclosure<A>> ReceiptInspector<'_, A, D> {
    /// Full disclosure is required for provider observations and escalation details.
    pub fn reconciliation_cases(
        &self,
        session: &A::Session,
        intent: &EffectIntentId,
    ) -> Result<Vec<worldinterface_domain::reconciliation::ReconciliationCase>> {
        let principal = caller(self.authentication, session)?;
        if self.disclosure.effect_profile(session, principal.tenant(), intent)?
            != DisclosureProfile::Full
        {
            return Err("reconciliation evidence disclosure denied".into());
        }
        self.root.reconciliation_cases(&principal, intent)
    }
}

/// Authenticated event -> outbox -> accepted wait chain. AQ terminality is descriptive.
pub struct ContinuationInspector<'a, A, P> {
    pub root: &'a StoreRoot,
    pub authentication: &'a A,
    pub policy: &'a P,
}
#[derive(Clone, Debug)]
pub struct ContinuationView {
    pub signal: worldinterface_domain::continuation::SignalObligation,
    pub waits: Vec<worldinterface_domain::continuation::WaitCheckpoint>,
    pub accepted: Vec<worldinterface_domain::continuation::ResumeAcceptance>,
}
impl<A: Authentication, P: crate::continuation::EventAuthorization>
    ContinuationInspector<'_, A, P>
{
    pub fn event(
        &self,
        session: &A::Session,
        kind: worldinterface_storage::StoreKind,
        id: &OpaqueRef,
    ) -> Result<ContinuationView> {
        let caller = caller(self.authentication, session)?;
        let signal =
            self.root.signal_obligation(kind, id).map_err(|_| "event disclosure denied")?;
        if signal.event.route.tenant != *caller.tenant()
            || !self.policy.authorize_read(&caller, &signal.event)
        {
            return Err("event disclosure denied".into());
        }
        let mut waits = Vec::new();
        let mut accepted = Vec::new();
        for run in self.root.flow_snapshot(caller.tenant())?.runs.values() {
            if &run.principal != caller.principal() {
                continue;
            }
            waits.extend(
                run.waits
                    .values()
                    .filter(|w| w.route.as_ref() == Some(&signal.event.route))
                    .cloned(),
            );
            accepted.extend(
                run.accepted_resumes.values().filter(|a| a.event.as_ref() == Some(id)).cloned(),
            );
        }
        Ok(ContinuationView { signal, waits, accepted })
    }
}
