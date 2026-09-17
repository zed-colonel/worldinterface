//! In-memory signal coordination for signal.await / signal.emit connectors.
//!
//! Each signal key maps to a one-shot channel. `register_waiter` creates the
//! channel and returns the receiver. `emit` sends the payload and removes the
//! entry. `remove_waiter` cleans up on timeout/cancellation.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Mutex;

use serde_json::Value;

/// In-memory signal coordination for signal.await / signal.emit connectors.
///
/// Each signal key maps to a one-shot channel. `register_waiter` creates the
/// channel and returns the receiver. `emit` sends the payload and removes the
/// entry. `remove_waiter` cleans up on timeout/cancellation.
pub struct SignalRegistry {
    waiters: Mutex<HashMap<String, mpsc::Sender<Value>>>,
}

impl SignalRegistry {
    pub fn new() -> Self {
        Self { waiters: Mutex::new(HashMap::new()) }
    }

    /// Register a waiter for the given key. Returns a receiver that will
    /// receive the payload when `emit` is called with the same key.
    pub fn register_waiter(&self, key: &str) -> mpsc::Receiver<Value> {
        let (tx, rx) = mpsc::channel();
        self.waiters.lock().unwrap().insert(key.to_string(), tx);
        rx
    }

    /// Emit a signal, delivering the payload to the matching waiter.
    /// Returns `true` if a waiter was found and delivery succeeded.
    pub fn emit(&self, key: &str, payload: Value) -> bool {
        if let Some(tx) = self.waiters.lock().unwrap().remove(key) {
            tx.send(payload).is_ok()
        } else {
            false
        }
    }

    /// Remove a waiter without delivering a payload (cleanup on timeout/cancel).
    pub fn remove_waiter(&self, key: &str) {
        self.waiters.lock().unwrap().remove(key);
    }

    /// Number of active waiters (for testing).
    pub fn waiter_count(&self) -> usize {
        self.waiters.lock().unwrap().len()
    }
}

impl Default for SignalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ── T1: registry_emit_delivers_to_waiter ──
    #[test]
    fn registry_emit_delivers_to_waiter() {
        let registry = SignalRegistry::new();
        let rx = registry.register_waiter("question:abc");

        let payload = json!({"answer": "yes"});
        assert!(registry.emit("question:abc", payload.clone()));

        let received = rx.recv().unwrap();
        assert_eq!(received, payload);
    }

    // ── T2: registry_emit_without_waiter_returns_false ──
    #[test]
    fn registry_emit_without_waiter_returns_false() {
        let registry = SignalRegistry::new();
        assert!(!registry.emit("nonexistent", json!({})));
    }

    // ── T3: registry_remove_waiter_cleans_up ──
    #[test]
    fn registry_remove_waiter_cleans_up() {
        let registry = SignalRegistry::new();
        let _rx = registry.register_waiter("question:abc");
        assert_eq!(registry.waiter_count(), 1);

        registry.remove_waiter("question:abc");
        assert_eq!(registry.waiter_count(), 0);
        assert!(!registry.emit("question:abc", json!({})));
    }

    // ── T4: registry_waiter_count ──
    #[test]
    fn registry_waiter_count() {
        let registry = SignalRegistry::new();
        assert_eq!(registry.waiter_count(), 0);

        let _rx1 = registry.register_waiter("a");
        assert_eq!(registry.waiter_count(), 1);

        let _rx2 = registry.register_waiter("b");
        assert_eq!(registry.waiter_count(), 2);

        registry.emit("a", json!({}));
        assert_eq!(registry.waiter_count(), 1);

        registry.remove_waiter("b");
        assert_eq!(registry.waiter_count(), 0);
    }
}
