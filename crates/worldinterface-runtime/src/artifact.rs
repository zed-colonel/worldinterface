use crate::Result;
use worldinterface_protocol::ArtifactRef;
use worldinterface_storage::StoreRoot;

/// Trusted host implementation. Context is a host-authenticated session type,
/// never a deserialized locator, causal attribution, or caller-claimed scope.
/// Implementations must authorize the exact reference on every read.
pub trait ArtifactAuthorization {
    type AuthenticatedContext;
    fn authorize_read(&self, context: &Self::AuthenticatedContext, artifact: &ArtifactRef) -> bool;
}

pub struct ArtifactResolver<'a, A: ?Sized> {
    store: &'a StoreRoot,
    authorization: &'a A,
}
impl<'a, A: ArtifactAuthorization + ?Sized> ArtifactResolver<'a, A> {
    pub fn new(store: &'a StoreRoot, authorization: &'a A) -> Self {
        Self { store, authorization }
    }
    pub fn read(
        &self,
        context: &A::AuthenticatedContext,
        artifact: &ArtifactRef,
    ) -> Result<Vec<u8>> {
        if !self.authorization.authorize_read(context, artifact) {
            return Err("artifact access denied".into());
        }
        self.store.read_artifact(artifact)
    }
}
