//! Always kill and reap crash helpers, including when their parent panics.
use std::ops::{Deref, DerefMut};
use std::process::Child;

pub(crate) struct KillOnDrop(pub(crate) Child);
impl Deref for KillOnDrop {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl DerefMut for KillOnDrop {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
