//! Bridge between `worldinterface-core::metrics::MetricsRecorder` and Prometheus collectors.

use std::sync::Arc;

use worldinterface_core::metrics::MetricsRecorder;

use super::WiMetricsRegistry;

/// Prometheus-backed implementation of [`MetricsRecorder`].
///
/// Delegates to the [`WiMetricsRegistry`]'s Prometheus collectors.
pub struct PrometheusMetricsRecorder {
    registry: Arc<WiMetricsRegistry>,
}

impl PrometheusMetricsRecorder {
    pub fn new(registry: Arc<WiMetricsRegistry>) -> Self {
        Self { registry }
    }
}

impl MetricsRecorder for PrometheusMetricsRecorder {
    fn record_step_completed(&self, connector: &str, duration_secs: f64) {
        let c = self.registry.collectors();
        c.step_runs_total.with_label_values(&[connector, "success"]).inc();
        c.step_duration_seconds.with_label_values(&[connector]).observe(duration_secs);
    }

    fn record_step_failed(&self, connector: &str) {
        self.registry.collectors().step_runs_total.with_label_values(&[connector, "failure"]).inc();
    }

    fn record_connector_invocation(&self, connector: &str) {
        self.registry
            .collectors()
            .connector_invocations_total
            .with_label_values(&[connector])
            .inc();
    }

    fn record_contextstore_write(&self) {
        self.registry.collectors().contextstore_writes_total.inc();
    }

    fn record_flow_completed(&self) {
        self.registry.collectors().flow_runs_total.with_label_values(&["completed"]).inc();
    }

    fn record_flow_failed(&self) {
        self.registry.collectors().flow_runs_total.with_label_values(&["failed"]).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_recorder() -> (Arc<WiMetricsRegistry>, PrometheusMetricsRecorder) {
        let registry = Arc::new(WiMetricsRegistry::new().unwrap());
        let recorder = PrometheusMetricsRecorder::new(Arc::clone(&registry));
        (registry, recorder)
    }

    #[test]
    fn record_step_completed_increments_counter_and_histogram() {
        let (registry, recorder) = make_recorder();
        recorder.record_step_completed("delay", 0.5);
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_step_runs_total{connector=\"delay\",status=\"success\"} 1"));
        assert!(text.contains("wi_step_duration_seconds_count{connector=\"delay\"} 1"));
    }

    #[test]
    fn record_step_failed_increments_counter() {
        let (registry, recorder) = make_recorder();
        recorder.record_step_failed("http.request");
        let text = registry.encode_text().unwrap();
        assert!(
            text.contains("wi_step_runs_total{connector=\"http.request\",status=\"failure\"} 1")
        );
    }

    #[test]
    fn record_connector_invocation_increments() {
        let (registry, recorder) = make_recorder();
        recorder.record_connector_invocation("fs.write");
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_connector_invocations_total{connector=\"fs.write\"} 1"));
    }

    #[test]
    fn record_contextstore_write_increments() {
        let (registry, recorder) = make_recorder();
        recorder.record_contextstore_write();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_contextstore_writes_total 1"));
    }

    #[test]
    fn record_flow_completed_increments() {
        let (registry, recorder) = make_recorder();
        recorder.record_flow_completed();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_flow_runs_total{status=\"completed\"} 1"));
    }

    #[test]
    fn record_flow_failed_increments() {
        let (registry, recorder) = make_recorder();
        recorder.record_flow_failed();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_flow_runs_total{status=\"failed\"} 1"));
    }
}
