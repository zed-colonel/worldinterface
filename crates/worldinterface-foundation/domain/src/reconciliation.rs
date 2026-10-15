//! Observation-only reconciliation. Closing polling never asserts provider failure.
use crate::{
    effect::{CommitObservation, EffectAggregate},
    ingress::digest,
};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    Pending,
    Running,
    ConfirmedCommitted,
    ConfirmedNotCommitted,
    Escalated,
    StillUnknown,
}
impl CaseStatus {
    pub fn terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationReceiptV1 {
    pub version: u32,
    pub case: OpaqueRef,
    pub original_source_revision: u64,
    pub source_revision: u64,
    pub observation: u32,
    pub result: CaseStatus,
    pub evidence: Vec<EvidenceRef>,
    pub at: Timestamp,
    pub digest: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationCase {
    pub version: u32,
    pub id: OpaqueRef,
    pub tenant: OpaqueRef,
    pub intent: EffectIntentId,
    pub dispatch_index: u32,
    pub attempt: OpaqueRef,
    pub original_source_revision: u64,
    pub execution: ConnectorExecution,
    pub resource: CanonicalResourceRef,
    pub policy: ContentHash,
    pub created_at: Timestamp,
    pub deadline_millis: u64,
    pub next_eligible_millis: u64,
    pub observations: u32,
    pub revision: u32,
    pub status: CaseStatus,
    pub commit: CommitObservation,
    pub evidence: Vec<EvidenceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<OpaqueRef>,
    /// Immutable serialized AQ request, filled before the first admission attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_request: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aq_run: Option<OpaqueRef>,
    pub queue_store: OpaqueRef,
    pub receipts: Vec<ReconciliationReceiptV1>,
}
impl ReconciliationCase {
    pub fn from_uncertain(
        e: &EffectAggregate,
        revision: u64,
        at: &Timestamp,
    ) -> Result<Option<Self>> {
        let Some(execution) =
            e.resolution.as_ref().and_then(|r| r.resolved.connector_execution.as_ref())
        else {
            return Ok(None);
        };
        let outcome = e.outcomes.last().ok_or("missing uncertainty")?;
        if outcome
            .reconciliation_admission
            .as_ref()
            .is_some_and(|b| b.is_empty() || b.len() > 16384)
        {
            return Err("reconciliation admission bytes bound".into());
        }
        let dispatch_index = u32::try_from(e.dispatches.len().checked_sub(1).ok_or("no dispatch")?)
            .map_err(|_| "dispatch overflow")?;
        let admission = e
            .dispatches
            .last()
            .and_then(|d| d.admission.as_ref())
            .ok_or("missing production admission")?;
        let id = format!(
            "wi-reconcile:{}",
            digest(&(
                "wi-reconciliation-case:1",
                &e.agency.binding.tenant,
                &e.intent.id,
                dispatch_index,
                &outcome.attempt
            ))?
            .as_str()
        )
        .try_into()?;
        let created = at.epoch_millis()?;
        let deadline_millis = created
            .checked_add(execution.binding.descriptor.reconciliation.timeout_millis)
            .ok_or("reconciliation deadline overflow")?;
        let deadline_millis = match &e.intent.submission.deadline {
            Some(d) => deadline_millis.min(d.epoch_millis()?),
            None => deadline_millis,
        };
        Ok(Some(Self {
            version: 1,
            id,
            tenant: e.agency.binding.tenant.clone(),
            intent: e.intent.id.clone(),
            dispatch_index,
            attempt: outcome.attempt.clone(),
            original_source_revision: revision,
            execution: execution.clone(),
            resource: e.resolution.as_ref().ok_or("no resolution")?.resolved.resource.clone(),
            policy: digest(&e.authorization_inputs.as_ref().ok_or("no policy")?.policy)?,
            created_at: at.clone(),
            deadline_millis,
            next_eligible_millis: created,
            observations: 0,
            revision: 0,
            status: CaseStatus::Pending,
            commit: outcome.commit.clone(),
            // Original evidence remains reachable through the immutable source receipt.
            evidence: vec![],
            escalation: None,
            admission_request: outcome.reconciliation_admission.clone(),
            admission_sequence: None,
            aq_run: None,
            queue_store: admission.queue_store.clone(),
            receipts: vec![],
        }))
    }
    pub fn start(&mut self, expected: u32, now: u64) -> Result<()> {
        if self.revision != expected
            || self.status != CaseStatus::Pending
            || now < self.next_eligible_millis
            || self.observations
                >= self.execution.binding.descriptor.reconciliation.max_observations
            || now >= self.deadline_millis
        {
            return Err("reconciliation start fence or budget".into());
        }
        self.observations += 1;
        self.revision += 1;
        self.status = CaseStatus::Running;
        Ok(())
    }
    pub fn observe(
        &mut self,
        expected: u32,
        result: CaseStatus,
        evidence: Vec<EvidenceRef>,
        at: &Timestamp,
        source_revision: u64,
    ) -> Result<()> {
        if self.revision != expected
            || self.status.terminal()
            || matches!(
                result,
                CaseStatus::Pending
                    | CaseStatus::ConfirmedCommitted
                    | CaseStatus::ConfirmedNotCommitted
            ) && self.status != CaseStatus::Running
            || evidence.is_empty()
            || evidence.len() > 2
            || evidence.iter().any(|e| {
                e.artifact.scope != self.tenant
                    || e.artifact.size == 0
                    || e.artifact.size > self.execution.binding.descriptor.max_evidence_bytes
            })
            || self.commit == CommitObservation::ConfirmedCommitted
                && result == CaseStatus::ConfirmedNotCommitted
            || !matches!(
                result,
                CaseStatus::Pending
                    | CaseStatus::ConfirmedCommitted
                    | CaseStatus::ConfirmedNotCommitted
                    | CaseStatus::Escalated
                    | CaseStatus::StillUnknown
            )
        {
            return Err("reconciliation observation fence or evidence".into());
        }
        let now = at.epoch_millis()?;
        let policy = &self.execution.binding.descriptor.reconciliation;
        self.status = if result == CaseStatus::Pending
            && (self.observations >= policy.max_observations || now >= self.deadline_millis)
        {
            CaseStatus::StillUnknown
        } else {
            result
        };
        if !matches!(self.status, CaseStatus::Escalated | CaseStatus::StillUnknown)
            && self.status != CaseStatus::Pending
            && self.status != CaseStatus::Running
            && self.observations == 0
        {
            return Err("confirmation requires an observation".into());
        }
        match self.status {
            CaseStatus::ConfirmedCommitted => self.commit = CommitObservation::ConfirmedCommitted,
            CaseStatus::ConfirmedNotCommitted => {
                self.commit = CommitObservation::ConfirmedNotCommitted
            }
            CaseStatus::Escalated => {
                self.escalation = Some("manual-reconciliation-required:1".to_string().try_into()?)
            }
            _ => {}
        }
        self.next_eligible_millis = now
            .saturating_add(
                policy.backoff_millis.saturating_mul(u64::from(self.observations.max(1))),
            )
            .min(self.deadline_millis);
        for e in &evidence {
            if !self.evidence.contains(e) {
                self.evidence.push(e.clone());
            }
        }
        if self.evidence.len() > 64 {
            return Err("reconciliation evidence accumulation bound".into());
        }
        let hash = digest(&(
            "wi-reconciliation-receipt:1",
            &self.id,
            self.original_source_revision,
            source_revision,
            self.observations,
            self.status,
            &evidence,
            at,
        ))?;
        self.receipts.push(ReconciliationReceiptV1 {
            version: 1,
            case: self.id.clone(),
            original_source_revision: self.original_source_revision,
            source_revision,
            observation: self.observations,
            result: self.status,
            evidence,
            at: at.clone(),
            digest: hash,
        });
        self.revision += 1;
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CaseAction {
    PrepareAdmission {
        request: Vec<u8>,
    },
    Acknowledge {
        sequence: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run: Option<OpaqueRef>,
    },
    Start,
    Observe {
        result: CaseStatus,
        evidence: Vec<EvidenceRef>,
    },
}
