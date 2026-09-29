//! Live observations must match the immutable target document before authorization.
use crate::Result;
use worldinterface_domain::resource::{ResolutionFact, ResolverSnapshot, TargetDocument};
use worldinterface_protocol::{EffectSubmission, OpaqueRef, Timestamp};
/// A trusted resolver captures the exact current filesystem, network, audience, or provider target.
pub trait ResourceObserver {
    fn observe(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
        registry: &ResolverSnapshot,
        at: &Timestamp,
    ) -> Result<ResolutionFact>;
    fn compare_before_use(&self, fact: &ResolutionFact) -> Result<TargetDocument>;
}
pub fn require_current(observer: &impl ResourceObserver, fact: &ResolutionFact) -> Result<()> {
    if observer.compare_before_use(fact)? != fact.document {
        return Err("target changed before use".into());
    }
    Ok(())
}
