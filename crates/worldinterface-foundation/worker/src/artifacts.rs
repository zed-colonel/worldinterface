use crate::{nonempty, Result};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::ContentHash;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportV1 {
    pub reference: String,
    pub relative_name: String,
    pub digest: ContentHash,
    pub size: u64,
    pub required_verifiers: Vec<String>,
    pub passed_verifiers: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportManifestV1 {
    pub schema: u32,
    pub provider: String,
    pub run: String,
    pub owner: String,
    pub exports: Vec<ExportV1>,
}
impl ExportManifestV1 {
    pub fn validate(&self, provider: &str, run: &str, owner: &str, scope: &[String]) -> Result<()> {
        if self.schema != 1
            || self.provider != provider
            || self.run != run
            || self.owner != owner
            || self.exports.len() > 64
        {
            return Err("export binding or bound invalid".into());
        }
        for s in [&self.provider, &self.run, &self.owner] {
            nonempty(s)?;
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut destinations: Vec<&str> = Vec::new();
        for e in &self.exports {
            nonempty(&e.reference)?;
            if !scope.contains(&e.reference)
                || !seen.insert(&e.reference)
                || e.size > 64 * 1024 * 1024
                || e.relative_name.is_empty()
                || e.relative_name.chars().any(char::is_control)
                || e.relative_name.starts_with('/')
                || e.relative_name
                    .split('/')
                    .any(|p| p.is_empty() || p == "." || p == ".." || p.contains('\\'))
                || e.required_verifiers.len() > 16
                || e.required_verifiers.iter().any(|v| nonempty(v).is_err())
                || !e.required_verifiers.iter().all(|v| e.passed_verifiers.contains(v))
            {
                return Err("unsafe or unverified export".into());
            }
            // Each export names a file. A duplicate destination or a file used
            // as another export's parent has no unambiguous file-tree meaning.
            if destinations.iter().any(|previous| {
                *previous == e.relative_name
                    || e.relative_name
                        .strip_prefix(previous)
                        .is_some_and(|rest| rest.starts_with('/'))
                    || previous
                        .strip_prefix(&e.relative_name)
                        .is_some_and(|rest| rest.starts_with('/'))
            }) {
                return Err("conflicting export destination".into());
            }
            destinations.push(&e.relative_name);
        }
        Ok(())
    }
}
pub fn verify_content(export: &ExportV1, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != export.size || ContentHash::of(bytes) != export.digest {
        return Err("artifact content mismatch".into());
    }
    Ok(())
}
