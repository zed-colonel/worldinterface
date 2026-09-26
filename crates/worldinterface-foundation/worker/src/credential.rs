use crate::{mapping::Operation, nonempty, Result};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::ContentHash;

/// Non-secret binding only. The credential token is never represented here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialBindingV1 {
    pub schema: u32,
    pub reference: String,
    pub issuer: String,
    pub audience: String,
    pub principal: String,
    pub provider: String,
    pub run: String,
    pub operations: Vec<Operation>,
    pub artifact_scope: Vec<String>,
    pub authorization_digest: ContentHash,
    pub expires_unix_ms: u64,
    pub revocation_generation: u64,
}
impl CredentialBindingV1 {
    pub fn permits(
        &self,
        provider: &str,
        run: &str,
        operation: Operation,
        artifact: Option<&str>,
        authorization: &ContentHash,
        now_ms: u64,
        generation: u64,
    ) -> Result<()> {
        if self.schema != 1
            || self.provider != provider
            || self.run != run
            || &self.authorization_digest != authorization
            || now_ms >= self.expires_unix_ms
            || generation != self.revocation_generation
            || !self.operations.contains(&operation)
            || !operation.available()
            || artifact.is_some_and(|a| !self.artifact_scope.iter().any(|s| s == a))
        {
            return Err("credential binding does not permit request".into());
        }
        for s in [
            &self.reference,
            &self.issuer,
            &self.audience,
            &self.principal,
            &self.provider,
            &self.run,
        ] {
            nonempty(s)?;
        }
        if self.operations.len() > 6 || self.artifact_scope.len() > 64 {
            return Err("credential scope exceeds bound".into());
        }
        Ok(())
    }
}
