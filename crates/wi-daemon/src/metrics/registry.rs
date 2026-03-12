//! WorldInterface daemon Prometheus metrics registry.

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

/// Error type for metrics operations.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),

    #[error("encoding error: {0}")]
    Encoding(#[from] std::string::FromUtf8Error),
}

/// Prometheus metric collectors for WorldInterface observability.
pub struct WiMetrics {
    /// Total flow runs by terminal status. Labels: `status` ∈ {completed, failed, canceled}
    pub flow_runs_total: CounterVec,

    /// Currently active (non-terminal) flow runs.
    pub flow_runs_active: Gauge,

    /// Total step executions by connector and status.
    /// Labels: `connector`, `status` ∈ {success, failure}
    pub step_runs_total: CounterVec,

    /// Step execution duration in seconds by connector.
    /// Labels: `connector`
    pub step_duration_seconds: HistogramVec,

    /// Total ContextStore write operations.
    pub contextstore_writes_total: Counter,

    /// Total connector invocations by connector name.
    /// Labels: `connector`
    pub connector_invocations_total: CounterVec,

    /// Total webhook invocations by path.
    /// Labels: `path`
    pub webhook_invocations_total: CounterVec,

    /// Total webhook invocation errors.
    pub webhook_errors_total: Counter,
}

/// WorldInterface daemon Prometheus metrics registry.
pub struct WiMetricsRegistry {
    registry: Registry,
    collectors: WiMetrics,
}

impl WiMetricsRegistry {
    /// Create a new metrics registry with all WorldInterface collectors registered.
    pub fn new() -> Result<Self, MetricsError> {
        let registry = Registry::new_custom(Some("wi".into()), None)?;

        let flow_runs_total = CounterVec::new(
            Opts::new("flow_runs_total", "Total flow runs by terminal status"),
            &["status"],
        )?;
        registry.register(Box::new(flow_runs_total.clone()))?;

        let flow_runs_active = Gauge::with_opts(Opts::new(
            "flow_runs_active",
            "Currently active (non-terminal) flow runs",
        ))?;
        registry.register(Box::new(flow_runs_active.clone()))?;

        let step_runs_total = CounterVec::new(
            Opts::new("step_runs_total", "Total step executions by connector and status"),
            &["connector", "status"],
        )?;
        registry.register(Box::new(step_runs_total.clone()))?;

        let step_duration_seconds = HistogramVec::new(
            HistogramOpts::new("step_duration_seconds", "Step execution duration in seconds"),
            &["connector"],
        )?;
        registry.register(Box::new(step_duration_seconds.clone()))?;

        let contextstore_writes_total = Counter::with_opts(Opts::new(
            "contextstore_writes_total",
            "Total ContextStore write operations",
        ))?;
        registry.register(Box::new(contextstore_writes_total.clone()))?;

        let connector_invocations_total = CounterVec::new(
            Opts::new("connector_invocations_total", "Total connector invocations"),
            &["connector"],
        )?;
        registry.register(Box::new(connector_invocations_total.clone()))?;

        let webhook_invocations_total = CounterVec::new(
            Opts::new("webhook_invocations_total", "Total webhook invocations by path"),
            &["path"],
        )?;
        registry.register(Box::new(webhook_invocations_total.clone()))?;

        let webhook_errors_total = Counter::with_opts(Opts::new(
            "webhook_errors_total",
            "Total webhook invocation errors",
        ))?;
        registry.register(Box::new(webhook_errors_total.clone()))?;

        Ok(Self {
            registry,
            collectors: WiMetrics {
                flow_runs_total,
                flow_runs_active,
                step_runs_total,
                step_duration_seconds,
                contextstore_writes_total,
                connector_invocations_total,
                webhook_invocations_total,
                webhook_errors_total,
            },
        })
    }

    /// Access the metric collectors for direct manipulation.
    pub fn collectors(&self) -> &WiMetrics {
        &self.collectors
    }

    /// Encode all registered metrics as Prometheus text exposition format.
    pub fn encode_text(&self) -> Result<String, MetricsError> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_creates_all_metrics() {
        let registry = WiMetricsRegistry::new().unwrap();
        let c = registry.collectors();
        // Verify all collectors are accessible (no panics)
        let _ = &c.flow_runs_total;
        let _ = &c.flow_runs_active;
        let _ = &c.step_runs_total;
        let _ = &c.step_duration_seconds;
        let _ = &c.contextstore_writes_total;
        let _ = &c.connector_invocations_total;
        let _ = &c.webhook_invocations_total;
        let _ = &c.webhook_errors_total;
    }

    #[test]
    fn registry_encodes_text() {
        let registry = WiMetricsRegistry::new().unwrap();
        // Touch some metrics so they appear in output (CounterVec requires label init)
        let c = registry.collectors();
        c.flow_runs_total.with_label_values(&["completed"]);
        c.step_runs_total.with_label_values(&["delay", "success"]);
        c.webhook_invocations_total.with_label_values(&["test"]);
        c.connector_invocations_total.with_label_values(&["delay"]);

        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_flow_runs_total"), "missing flow_runs_total");
        assert!(text.contains("wi_step_runs_total"), "missing step_runs_total");
        assert!(text.contains("wi_contextstore_writes_total"), "missing contextstore_writes_total");
        assert!(text.contains("wi_webhook_invocations_total"), "missing webhook_invocations_total");
    }

    #[test]
    fn flow_runs_total_increments() {
        let registry = WiMetricsRegistry::new().unwrap();
        registry.collectors().flow_runs_total.with_label_values(&["completed"]).inc();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_flow_runs_total{status=\"completed\"} 1"));
    }

    #[test]
    fn step_runs_total_increments_by_label() {
        let registry = WiMetricsRegistry::new().unwrap();
        let c = registry.collectors();
        c.step_runs_total.with_label_values(&["delay", "success"]).inc();
        c.step_runs_total.with_label_values(&["fs.write", "success"]).inc();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_step_runs_total{connector=\"delay\",status=\"success\"} 1"));
        assert!(text.contains("wi_step_runs_total{connector=\"fs.write\",status=\"success\"} 1"));
    }

    #[test]
    fn step_duration_histogram_observes() {
        let registry = WiMetricsRegistry::new().unwrap();
        registry.collectors().step_duration_seconds.with_label_values(&["delay"]).observe(0.5);
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_step_duration_seconds"));
        // Check sample count
        assert!(text.contains("wi_step_duration_seconds_count{connector=\"delay\"} 1"));
    }

    #[test]
    fn contextstore_writes_counter() {
        let registry = WiMetricsRegistry::new().unwrap();
        let c = registry.collectors();
        c.contextstore_writes_total.inc();
        c.contextstore_writes_total.inc();
        c.contextstore_writes_total.inc();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_contextstore_writes_total 3"));
    }

    #[test]
    fn webhook_invocations_by_path() {
        let registry = WiMetricsRegistry::new().unwrap();
        let c = registry.collectors();
        c.webhook_invocations_total.with_label_values(&["github/push"]).inc();
        c.webhook_invocations_total.with_label_values(&["github/push"]).inc();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_webhook_invocations_total{path=\"github/push\"} 2"));
    }

    #[test]
    fn webhook_errors_counter() {
        let registry = WiMetricsRegistry::new().unwrap();
        registry.collectors().webhook_errors_total.inc();
        let text = registry.encode_text().unwrap();
        assert!(text.contains("wi_webhook_errors_total 1"));
    }
}
