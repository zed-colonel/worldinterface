//! Directory watcher for hot-loading WASM connector modules.
//!
//! When the `watcher` feature is enabled, this module provides a filesystem
//! watcher that detects new `.connector.toml` files in the connectors directory
//! and emits `(manifest_path, wasm_path)` pairs for the host to hot-load.

use std::path::PathBuf;

use tokio::sync::mpsc;

/// Spawn a directory watcher that detects new `.connector.toml` files.
///
/// Returns a join handle for the blocking watcher thread and a channel
/// receiver that yields `(manifest_path, wasm_path)` pairs when new
/// connectors are detected.
///
/// `Create` and `Modify` events are processed — on Linux (inotify),
/// `std::fs::write()` may emit `Modify` rather than `Create` for new files
/// depending on filesystem and timing. The `.connector.toml` + `.wasm` pair
/// check is the real filter.
#[cfg(feature = "watcher")]
#[allow(clippy::type_complexity)]
pub fn spawn_connector_watcher(
    dir: PathBuf,
) -> Result<(tokio::task::JoinHandle<()>, mpsc::Receiver<(PathBuf, PathBuf)>), notify::Error> {
    use notify::{Event, EventKind, RecursiveMode, Watcher};

    let (tx, rx) = mpsc::channel(32);
    let handle = tokio::task::spawn_blocking(move || {
        let rt_tx = tx;
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, _>| {
            if let Ok(event) = res {
                let _ = notify_tx.send(event);
            }
        })
        .expect("failed to create filesystem watcher");
        if let Err(e) = watcher.watch(&dir, RecursiveMode::NonRecursive) {
            tracing::error!(error = %e, "failed to watch connectors directory");
            return;
        }
        tracing::info!(dir = %dir.display(), "watching connectors directory for new modules");
        for event in notify_rx {
            if matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_)) {
                for path in &event.paths {
                    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if name.ends_with(".connector.toml") {
                        if let Some(stem) = name.strip_suffix(".connector.toml") {
                            let wasm_path = dir.join(format!("{stem}.wasm"));
                            if wasm_path.exists() {
                                let _ = rt_tx.blocking_send((path.clone(), wasm_path));
                            }
                        }
                    }
                }
            }
        }
    });
    Ok((handle, rx))
}
