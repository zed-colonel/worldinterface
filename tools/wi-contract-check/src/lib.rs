//! Repository contract tooling only. Never link this crate into the runtime.
pub mod catalog;
pub mod contracts;
pub mod evaluation;
pub mod evidence;
pub mod handoff;
pub mod inventory;
pub mod policy;
pub mod proof;
pub mod refresh;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Debug, Serialize, Deserialize)]
pub struct Finding {
    pub rule: String,
    pub item: String,
    pub path: String,
    pub source: String,
    pub disposition: String,
    pub diagnostic: String,
}
pub fn finding(rule: &str, path: &str, item: &str, source: &str, diagnostic: &str) -> Finding {
    Finding {
        rule: rule.into(),
        item: item.into(),
        path: path.into(),
        source: source.into(),
        disposition: "reject; amend the source or restore the approved contract".into(),
        diagnostic: diagnostic.into(),
    }
}
pub fn hash(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}
pub fn read(path: &Path) -> Result<Value> {
    let bytes = fs::read(path)?;
    Ok(
        if path.extension().and_then(|x| x.to_str()) == Some("toml")
            || path.file_name().is_some_and(|x| x == "Cargo.lock")
        {
            serde_json::to_value(toml::from_str::<toml::Value>(std::str::from_utf8(&bytes)?)?)?
        } else {
            serde_yaml::from_slice(&bytes)?
        },
    )
}
pub fn files(root: &Path) -> Result<Vec<PathBuf>> {
    fn walk(p: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        for e in fs::read_dir(p)? {
            let e = e?;
            let name = e.file_name();
            let n = name.to_string_lossy();
            if matches!(n.as_ref(), ".git" | "target" | "dev-output") {
                continue;
            }
            if e.file_type()?.is_symlink() {
                return Err(
                    format!("symlink requires explicit review: {}", e.path().display()).into()
                );
            }
            if e.file_type()?.is_dir() {
                walk(&e.path(), out)?
            } else {
                out.push(e.path())
            }
        }
        Ok(())
    }
    let mut out = vec![];
    if root.exists() {
        walk(root, &mut out)?
    }
    out.sort();
    Ok(out)
}
pub fn relative(root: &Path, p: &Path) -> String {
    p.strip_prefix(root).unwrap_or(p).to_string_lossy().replace('\\', "/")
}
pub fn array<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    v[key].as_array().ok_or_else(|| format!("missing array {key}").into())
}
pub fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing nonempty string {key}").into())
}

pub mod continuations;

pub mod worker_campaign;
