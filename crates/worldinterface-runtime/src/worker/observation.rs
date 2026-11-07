//! One owner transaction commits lifecycle evidence and its signal obligation.
use crate::Result;
use worldinterface_domain::{continuation::*, worker::*};
use worldinterface_protocol::*;
use worldinterface_storage::{StoreKind, StoreRoot};
use worldinterface_worker::ports::{Observation, ObservationKind};
pub fn commit(
    root: &StoreRoot,
    binding: &WorkerBinding,
    route: EventRoute,
    observed: Observation,
) -> Result<OpaqueRef> {
    if observed.provider != binding.provider.as_str()
        || observed.run != binding.run.as_str()
        || observed.sequence == 0
        || route.tenant != binding.tenant
        || route.audience != binding.principal
        || route.correlation != binding.event_correlation()?
        || route.kind != EventKind::WorkerObservation
        || route.generation.is_some()
    {
        return Err("worker observation binding mismatch".into());
    }
    let current = root
        .worker_snapshot(&binding.tenant, &binding.provider, &binding.run)?
        .ok_or("worker absent")?;
    if current.binding != *binding {
        return Err("worker observation realization mismatch".into());
    }
    let bytes = canonical_data(&observed)?;
    let source: OpaqueRef = format!(
        "wi-worker:{}",
        ContentHash::of(&canonical_data(&(
            "wi-worker-observation-1",
            &observed.provider,
            &observed.run,
            &observed.event
        ))?)
        .as_str()
    )
    .try_into()?;
    let fact = match observed.kind {
        ObservationKind::Accepted => WorkerFact::StartAccepted,
        ObservationKind::Provisioned => WorkerFact::Provisioned,
        ObservationKind::Running => WorkerFact::Running,
        ObservationKind::Exited => WorkerFact::Exited,
        ObservationKind::ArtifactPublished => {
            return Err("provider cannot assert WI publication".into())
        }
        ObservationKind::CollectionReady => WorkerFact::CollectionReady,
        ObservationKind::Released => WorkerFact::Released,
        ObservationKind::Quarantined => WorkerFact::Quarantined,
    };
    let observation = WorkerObservation {
        event: source.clone(),
        provider_sequence: observed.sequence,
        fact,
        digest: ContentHash::of(&bytes),
    };
    if let Some(old) = current.observations.get(&observed.sequence) {
        if old != &observation {
            return Err("worker observation sequence conflict".into());
        }
        let id = root
            .committed_worker_event(&binding.tenant, &source)?
            .ok_or("worker event outbox missing")?;
        let event = root.authoritative_event(StoreKind::Fabric, &id)?;
        if event.route != route || event.digest != observation.digest {
            return Err("worker observation route conflict".into());
        }
        return Ok(id);
    }
    let artifact = root.publish(binding.tenant.clone(), &bytes)?;
    root.commit_worker_observation(binding, observation, route, artifact)
}
