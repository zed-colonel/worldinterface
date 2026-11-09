//! Pure publication states. Availability alone does not establish outcome.
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{ArtifactRef, OpaqueRef};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicationState {
    Identified,
    Durable,
    Available,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub identity: OpaqueRef,
    pub artifact: ArtifactRef,
    pub state: PublicationState,
}
impl Publication {
    pub fn advance(&self, next: PublicationState) -> Result<Self, String> {
        if !matches!(
            (&self.state, &next),
            (PublicationState::Identified, PublicationState::Durable)
                | (PublicationState::Durable, PublicationState::Available)
        ) {
            return Err("publication transition conflict".into());
        }
        Ok(Self { identity: self.identity.clone(), artifact: self.artifact.clone(), state: next })
    }
}
