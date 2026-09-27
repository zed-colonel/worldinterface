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
/// Values obtained from WI's trusted authorization and transport context, never
/// from the worker's credential artifact or request body.
#[derive(Clone, Copy)]
pub struct CredentialExpectation<'a> {
    pub issuer: &'a str,
    pub audience: &'a str,
    pub principal: &'a str,
    pub provider: &'a str,
    pub run: &'a str,
    pub authorization_digest: &'a ContentHash,
}
impl CredentialBindingV1 {
    pub fn permits(
        &self,
        expected: &CredentialExpectation<'_>,
        operation: Operation,
        artifact: Option<&str>,
        now_ms: u64,
        generation: u64,
    ) -> Result<()> {
        if self.schema != 1
            || self.issuer != expected.issuer
            || self.audience != expected.audience
            || self.principal != expected.principal
            || self.principal == "root"
            || self.provider != expected.provider
            || self.run != expected.run
            || &self.authorization_digest != expected.authorization_digest
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
        for reference in &self.artifact_scope {
            nonempty(reference)?;
        }
        Ok(())
    }
}
