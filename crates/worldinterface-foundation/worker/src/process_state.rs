//! Private adapter facts. No serialization, getters, or ordinary diagnostic formatting.
#[allow(dead_code)] // Adapter-only facts are intentionally uninstantiated in this slice.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) boot: String,
    pub(crate) birth: u64,
    pub(crate) ownership_nonce: String,
    pub(crate) tree: String,
    pub(crate) workspace: String,
    pub(crate) export_root: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    VerifiedOwned,
    Absent,
    Mismatched,
    Ambiguous,
}
#[allow(dead_code)] // Exercised with synthetic facts until the process adapter exists.
pub(crate) fn assess(
    expected: &ProcessIdentity,
    observed: Option<&ProcessIdentity>,
    complete: bool,
) -> Ownership {
    if !complete {
        return Ownership::Ambiguous;
    }
    match observed {
        None => Ownership::Absent,
        Some(o) if o == expected => Ownership::VerifiedOwned,
        Some(_) => Ownership::Mismatched,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pid_equality_is_insufficient() {
        let a = ProcessIdentity {
            pid: 7,
            boot: "a".into(),
            birth: 1,
            ownership_nonce: "n".into(),
            tree: "t".into(),
            workspace: "w".into(),
            export_root: "e".into(),
        };
        let mut b = a.clone();
        b.boot = "b".into();
        assert_eq!(assess(&a, Some(&b), true), Ownership::Mismatched);
        assert_eq!(assess(&a, Some(&a), false), Ownership::Ambiguous);
    }
}
