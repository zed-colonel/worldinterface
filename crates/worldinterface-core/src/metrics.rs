//! Metrics recording trait for cross-crate observability.
//!
//! Defines the [`MetricsRecorder`] trait that allows leaf crates (coordinator,
//! connector, contextstore) to record metrics without depending on a specific
//! metrics implementation like Prometheus. The daemon provides a real
//! implementation; embedded/test use gets [`NoopMetricsRecorder`].

/// Trait for recording observability metrics across UI crates.
///
/// Implemented by the daemon's Prometheus registry. When no metrics
/// are configured, [`NoopMetricsRecorder`] is used.
pub trait MetricsRecorder: Send + Sync {
    /// Record a successful step completion with connector name and duration.
    fn record_step_completed(&self, connector: &str, duration_secs: f64);

    /// Record a step failure with connector name.
    fn record_step_failed(&self, connector: &str);

    /// Record a connector invocation (before execution).
    fn record_connector_invocation(&self, connector: &str);

    /// Record a ContextStore write operation.
    fn record_contextstore_write(&self);

    /// Record a flow run completing successfully.
    fn record_flow_completed(&self);

    /// Record a flow run failing.
    fn record_flow_failed(&self);
}

/// No-op implementation for embedded/test use without metrics.
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn record_step_completed(&self, _: &str, _: f64) {}
    fn record_step_failed(&self, _: &str) {}
    fn record_connector_invocation(&self, _: &str) {}
    fn record_contextstore_write(&self) {}
    fn record_flow_completed(&self) {}
    fn record_flow_failed(&self) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn noop_recorder_does_not_panic() {
        let recorder = NoopMetricsRecorder;
        recorder.record_step_completed("delay", 0.5);
        recorder.record_step_failed("http.request");
        recorder.record_connector_invocation("fs.write");
        recorder.record_contextstore_write();
        recorder.record_flow_completed();
        recorder.record_flow_failed();
    }

    #[test]
    fn noop_recorder_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoopMetricsRecorder>();
    }

    #[test]
    fn metrics_recorder_trait_object_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<dyn MetricsRecorder>>();
    }
}
