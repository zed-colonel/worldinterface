//! Fuel budget and wall-clock timeout helpers.
//!
//! wasmtime fuel metering counts WASM instructions. Epoch interruption
//! provides wall-clock timeout. Together they bound both CPU and wall time.

use std::time::Duration;

/// Calculate the epoch deadline for a given timeout and epoch interval.
///
/// The epoch ticker increments every `interval_ms`. To achieve a `timeout`
/// duration, we need `timeout_ms / interval_ms` epoch ticks from the
/// current epoch.
pub fn epoch_ticks_for_timeout(timeout: Duration, interval_ms: u64) -> u64 {
    let timeout_ms = timeout.as_millis() as u64;
    timeout_ms.div_ceil(interval_ms)
}

/// Default epoch ticker interval in milliseconds.
pub const EPOCH_INTERVAL_MS: u64 = 100;

/// Default fuel budget (1 billion — ~10 seconds of computation).
pub const DEFAULT_FUEL: u64 = 1_000_000_000;

/// Default wall-clock timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default WASM linear memory limit (64MB).
pub const DEFAULT_MAX_MEMORY: usize = 67_108_864;

/// Default WASM stack limit (1MB).
pub const WASM_STACK_LIMIT: usize = 1_048_576;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_ticks_calculation() {
        // 30s timeout with 100ms interval = 300 ticks
        assert_eq!(epoch_ticks_for_timeout(Duration::from_secs(30), 100), 300);
        // 10s = 100 ticks
        assert_eq!(epoch_ticks_for_timeout(Duration::from_secs(10), 100), 100);
        // 150ms = 2 ticks (rounds up)
        assert_eq!(epoch_ticks_for_timeout(Duration::from_millis(150), 100), 2);
    }
}
