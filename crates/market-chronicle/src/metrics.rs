use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, TextEncoder};

/// Metrics collector for Market Chronicle
pub struct Metrics {
    meter_provider: SdkMeterProvider,
    registry: prometheus::Registry,
}

impl Metrics {
    /// Initialize OpenTelemetry metrics with Prometheus exporter
    pub fn new(service_name: &str) -> Result<Self> {
        let registry = prometheus::Registry::new();

        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()?;

        let meter_provider = SdkMeterProvider::builder()
            .with_reader(exporter)
            .with_resource(opentelemetry_sdk::Resource::new(vec![
                KeyValue::new("service.name", service_name.to_string()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION").to_string()),
            ]))
            .build();

        // Set global meter provider
        opentelemetry::global::set_meter_provider(meter_provider.clone());

        Ok(Self {
            meter_provider,
            registry,
        })
    }

    /// Get Prometheus metrics in text format
    pub fn export_prometheus(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer)?)
    }

    /// Shutdown metrics gracefully
    pub fn shutdown(&self) -> Result<()> {
        self.meter_provider.shutdown()?;
        Ok(())
    }
}
