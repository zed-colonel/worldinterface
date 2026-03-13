//! Prometheus metrics for the WorldInterface daemon.

pub mod recorder;
pub mod registry;

pub use recorder::PrometheusMetricsRecorder;
pub use registry::WiMetricsRegistry;
