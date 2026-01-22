use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

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

/// Global metrics instances for different components
pub struct MetricsRecorder {
    meter: opentelemetry::metrics::Meter,
}

impl MetricsRecorder {
    pub fn new(component: &'static str) -> Self {
        let meter = opentelemetry::global::meter(component);
        Self { meter }
    }
    
    /// Worker metrics
    pub fn worker_messages_received(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("worker.messages.received")
            .with_description("Total messages received from WebSocket")
            .init()
    }
    
    pub fn worker_active_streams(&self) -> opentelemetry::metrics::UpDownCounter<i64> {
        self.meter
            .i64_up_down_counter("worker.streams.active")
            .with_description("Number of active WebSocket streams")
            .init()
    }
    
    pub fn worker_reconnects(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("worker.reconnects.total")
            .with_description("Total WebSocket reconnections")
            .init()
    }
    
    /// Batch aggregator metrics
    pub fn batch_messages_processed(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("batch.messages.processed")
            .with_description("Total messages processed by batch aggregator")
            .init()
    }
    
    pub fn batch_created(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("batch.created.total")
            .with_description("Total batches created")
            .init()
    }
    
    pub fn batch_size_messages(&self) -> opentelemetry::metrics::Histogram<u64> {
        self.meter
            .u64_histogram("batch.size.messages")
            .with_description("Distribution of batch sizes in messages")
            .init()
    }
    
    pub fn batch_active(&self) -> opentelemetry::metrics::UpDownCounter<i64> {
        self.meter
            .i64_up_down_counter("batch.active")
            .with_description("Number of active batches in memory")
            .init()
    }
    
    /// S3 uploader metrics
    pub fn s3_uploads_total(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("s3.uploads.total")
            .with_description("Total successful S3 uploads")
            .init()
    }
    
    pub fn s3_upload_failures(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("s3.uploads.failures")
            .with_description("Total failed S3 uploads")
            .init()
    }
    
    pub fn s3_upload_duration(&self) -> opentelemetry::metrics::Histogram<f64> {
        self.meter
            .f64_histogram("s3.upload.duration.seconds")
            .with_description("S3 upload duration in seconds")
            .init()
    }
    
    pub fn s3_upload_bytes(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("s3.upload.bytes")
            .with_description("Total bytes uploaded to S3 (compressed)")
            .init()
    }
    
    pub fn s3_compression_ratio(&self) -> opentelemetry::metrics::Histogram<f64> {
        self.meter
            .f64_histogram("s3.compression.ratio")
            .with_description("Compression ratio (uncompressed/compressed)")
            .init()
    }
    
    /// Symbol manager metrics
    pub fn symbols_discovered(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("symbols.discovered.total")
            .with_description("Total new symbols discovered")
            .init()
    }
    
    pub fn symbols_delisted(&self) -> opentelemetry::metrics::Counter<u64> {
        self.meter
            .u64_counter("symbols.delisted.total")
            .with_description("Total symbols delisted")
            .init()
    }
    
    pub fn symbols_active(&self) -> opentelemetry::metrics::UpDownCounter<i64> {
        self.meter
            .i64_up_down_counter("symbols.active")
            .with_description("Number of active symbols")
            .init()
    }
}

/// Shared metrics recorder wrapped in Arc for cloning
pub type SharedMetrics = Arc<MetricsRecorder>;
