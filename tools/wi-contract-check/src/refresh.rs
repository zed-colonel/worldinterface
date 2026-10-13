//! A source hash refresh preserves inventory structure and every disposition decision.
use crate::{catalog, hash, inventory, read, Result};
use serde_json::Value;
use std::{fs, path::Path};

/// A caller cannot use the refresh to change any non-source-hash field,
/// including adding/removing a field or changing its type.
pub fn hash_only_change(before: &Value, after: &Value) -> bool {
    match (before, after) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.get(key).is_some_and(|other| {
                        if key == "source_sha256" {
                            value.is_string() && other.is_string()
                        } else {
                            hash_only_change(value, other)
                        }
                    })
                })
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| hash_only_change(a, b))
        }
        _ => before == after,
    }
}
/// Preserve all original bytes except the designated hash values.
pub fn render_hashes(original: &str, updated: &Value) -> Result<String> {
    let before: Value = serde_json::from_str(original)?;
    if !hash_only_change(&before, updated) {
        return Err("non-hash change in refresh".into());
    }
    fn collect(value: &Value, hashes: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    if key == "source_sha256" {
                        hashes.push(value.as_str().unwrap().to_owned());
                    } else {
                        collect(value, hashes);
                    }
                }
            }
            Value::Array(a) => {
                for value in a {
                    collect(value, hashes);
                }
            }
            _ => {}
        }
    }
    let mut hashes = vec![];
    collect(updated, &mut hashes);
    let re = regex::Regex::new(r#"("source_sha256"\s*:\s*")[0-9a-f]{64}(")"#)?;
    let mut next = hashes.iter();
    let rendered = re
        .replace_all(original, |c: &regex::Captures<'_>| {
            format!("{}{}{}", &c[1], next.next().map(String::as_str).unwrap_or("invalid"), &c[2])
        })
        .into_owned();
    if serde_json::from_str::<Value>(&rendered)? != *updated || next.next().is_some() {
        return Err("unsupported source hash layout; no files written".into());
    }
    Ok(rendered)
}
/// Compute the complete update before writing anything. Structural drift needs review.
pub fn prepare(root: &Path) -> Result<Vec<(String, Value)>> {
    let current_path = "inventory/current.json";
    let old = read(&root.join(current_path))?;
    let items = inventory::generate(root)?;
    let new = serde_json::to_value(&items)?;
    if !hash_only_change(&old, &new) {
        return Err(
            "inventory structural drift; review inventory and dispositions explicitly".into()
        );
    }
    if !inventory::check_dispositions(&items, &read(&root.join("inventory/dispositions.json"))?)
        .is_empty()
    {
        return Err("inventory dispositions rejected".into());
    }
    let path = "inventory/connector-disposition.json";
    let old_plan = read(&root.join(path))?;
    let mut plan: catalog::Disposition = serde_json::from_value(old_plan.clone())?;
    for family in &mut plan.families {
        for item in &mut family.items {
            let source = items
                .iter()
                .find(|i| i.id == item.id)
                .ok_or("stale catalog inventory reference")?;
            item.source_sha256.clone_from(&source.source_sha256);
        }
        for surface in &mut family.surfaces {
            surface.source_sha256 = hash(fs::read(root.join(&surface.path))?);
        }
    }
    let new_plan = serde_json::to_value(&plan)?;
    if !hash_only_change(&old_plan, &new_plan)
        || !catalog::validate_disposition(root, &items, &plan)?.is_empty()
    {
        return Err("catalog structural drift; families, ownership, destinations, registration sites and treatments require explicit review".into());
    }
    Ok(vec![(current_path.into(), new), (path.into(), new_plan)])
}
pub fn refresh(root: &Path) -> Result<()> {
    let changes = prepare(root)?
        .into_iter()
        .map(|(path, value)| {
            let rendered = render_hashes(&fs::read_to_string(root.join(&path))?, &value)?;
            Ok((path, rendered))
        })
        .collect::<Result<Vec<_>>>()?;
    for (path, rendered) in changes {
        fs::write(root.join(path), rendered)?;
    }
    Ok(())
}
