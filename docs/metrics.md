# Metrics and Observability

## Overview

Market Chronicle uses **OpenTelemetry** for unified observability, providing metrics, traces, and logs in a vendor-neutral format. Metrics are exposed via Prometheus-compatible HTTP endpoint and can be integrated with any OpenTelemetry-compatible backend.

## Quick Start

### 1. Access Metrics Endpoint

```bash
# Prometheus metrics (scrape endpoint)
curl http://localhost:9090/metrics

# Health check
curl http://localhost:9090/health
```

### 2. Configure Prometheus

**prometheus.yml:**
```yaml
scrape_configs:
  - job_name: 'market-chronicle'
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:9090']
        labels:
          service: 'market-chronicle'
          environment: 'production'
```

### 3. Visualize in Grafana

Import the provided Grafana dashboard (see `grafana-dashboard.json`)

---

## Available Metrics

### Worker Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `worker_messages_received` | Counter | Total messages received from WebSocket | `worker_id`, `exchange`, `market_type` |
| `worker_streams_active` | Gauge | Number of active WebSocket streams | `worker_id`, `exchange` |
| `worker_reconnects_total` | Counter | Total WebSocket reconnections | `worker_id`, `exchange`, `reason` |

**Example Queries:**
```promql
# Message ingestion rate (per second)
rate(worker_messages_received[1m])

# Total active streams across all workers
sum(worker_streams_active)

# Reconnection frequency
rate(worker_reconnects_total[5m])
```

### Batch Aggregator Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `batch_messages_processed` | Counter | Total messages processed | `exchange`, `stream_type` |
| `batch_created_total` | Counter | Total batches created | `exchange`, `stream_type` |
| `batch_size_messages` | Histogram | Distribution of batch sizes (messages) | `exchange`, `stream_type` |
| `batch_active` | Gauge | Number of active batches in memory | - |

**Example Queries:**
```promql
# Batch creation rate
rate(batch_created_total[1m])

# Average batch size
rate(batch_messages_processed[5m]) / rate(batch_created_total[5m])

# 95th percentile batch size
histogram_quantile(0.95, batch_size_messages_bucket)
```

### S3 Uploader Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `s3_uploads_total` | Counter | Successful S3 uploads | `worker_id`, `exchange`, `stream_type` |
| `s3_uploads_failures` | Counter | Failed S3 uploads | `worker_id`, `exchange`, `error_type` |
| `s3_upload_duration_seconds` | Histogram | S3 upload latency distribution | `worker_id` |
| `s3_upload_bytes` | Counter | Total bytes uploaded (compressed) | `worker_id`, `exchange` |
| `s3_compression_ratio` | Histogram | Compression ratio distribution | `exchange`, `stream_type` |

**Example Queries:**
```promql
# Upload success rate
rate(s3_uploads_total[5m]) / (rate(s3_uploads_total[5m]) + rate(s3_uploads_failures[5m]))

# Average upload latency
rate(s3_upload_duration_seconds_sum[5m]) / rate(s3_upload_duration_seconds_count[5m])

# 99th percentile upload time
histogram_quantile(0.99, s3_upload_duration_seconds_bucket)

# Total data volume (MB/sec)
rate(s3_upload_bytes[1m]) / 1024 / 1024

# Average compression ratio
avg(s3_compression_ratio)
```

### Symbol Manager Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `symbols_discovered_total` | Counter | New symbols discovered | `exchange`, `market_type` |
| `symbols_delisted_total` | Counter | Symbols delisted | `exchange`, `market_type` |
| `symbols_active` | Gauge | Number of active symbols | `exchange`, `market_type` |

**Example Queries:**
```promql
# Symbol discovery rate
rate(symbols_discovered_total[1h])

# Total active symbols
sum(symbols_active)

# Delisting rate
rate(symbols_delisted_total[1h])
```

---

## Grafana Dashboard

### Pre-built Dashboard

A complete Grafana dashboard is provided in `grafana-dashboard.json` with panels for:

1. **Overview**
   - Message ingestion rate (msg/sec)
   - Active workers and streams
   - Batch creation rate
   - S3 upload rate
   - Success/failure rates

2. **Performance**
   - Message processing latency
   - S3 upload duration (p50, p95, p99)
   - Batch size distribution
   - Compression ratio trends

3. **Reliability**
   - WebSocket reconnection rate
   - S3 upload failures
   - Error rates by type
   - Health status

4. **Resource Usage**
   - Active batches in memory
   - Bytes uploaded per minute
   - Compression efficiency

### Import Dashboard

1. Open Grafana
2. Go to **Dashboards** → **Import**
3. Upload `grafana-dashboard.json`
4. Select Prometheus data source
5. Click **Import**

---

## Alerting Rules

### Recommended Prometheus Alerts

**prometheus-alerts.yml:**
```yaml
groups:
  - name: market-chronicle
    interval: 30s
    rules:
      # High reconnection rate
      - alert: HighWebSocketReconnections
        expr: rate(worker_reconnects_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High WebSocket reconnection rate"
          description: "{{ $labels.worker_id }} reconnecting {{ $value }} times/sec"
      
      # S3 upload failures
      - alert: S3UploadFailures
        expr: rate(s3_uploads_failures[5m]) > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "S3 uploads failing"
          description: "{{ $value }} upload failures per second"
      
      # Low message rate (possible connection issue)
      - alert: LowMessageRate
        expr: rate(worker_messages_received[5m]) < 10
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Low message ingestion rate"
          description: "Only {{ $value }} msg/sec (expected > 10)"
      
      # No messages (critical)
      - alert: NoMessages
        expr: rate(worker_messages_received[5m]) == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "No messages being received"
          description: "Zero message rate for 5 minutes"
      
      # High S3 upload latency
      - alert: HighS3Latency
        expr: histogram_quantile(0.95, s3_upload_duration_seconds_bucket) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High S3 upload latency"
          description: "P95 upload time is {{ $value }}s (threshold: 5s)"
```

---

## OpenTelemetry Configuration

### OTLP Exporter (Optional)

To send metrics to an OpenTelemetry Collector or backend:

**Environment variables:**
```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_SERVICE_NAME=market-chronicle
export OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production,host.name=$(hostname)"
```

The service will automatically export to both:
1. **Prometheus endpoint** (`:9090/metrics`) for scraping
2. **OTLP endpoint** (if configured) for push-based export

### Supported Backends

- **Prometheus** (pull-based scraping)
- **Grafana Cloud** (via OTLP)
- **Datadog** (via OTLP)
- **New Relic** (via OTLP)
- **Honeycomb** (via OTLP)
- **AWS CloudWatch** (via OTLP)
- **Google Cloud Monitoring** (via OTLP)
- **Azure Monitor** (via OTLP)

---

## Metrics Port Configuration

Default port: **9090**

To change the metrics port, set environment variable:
```bash
export METRICS_PORT=8080
```

Or modify `main.rs`:
```rust
let metrics_server = MetricsServer::new(metrics.clone(), 8080);
```

---

## Performance Impact

### Overhead Analysis

- **CPU**: < 1% additional overhead
- **Memory**: ~10-20MB for metrics storage
- **Network**: ~1KB/sec metrics scrape bandwidth
- **Latency**: < 1μs per metric operation

### Metrics Collection Frequency

- **Counter/Gauge updates**: Real-time (on event)
- **Histogram recording**: Real-time (on event)
- **Metrics export**: Every 10 seconds
- **Prometheus scrape**: Configurable (default: 15s)

---

## Troubleshooting

### Issue: Metrics endpoint not accessible

**Check:**
1. Service is running: `curl http://localhost:9090/health`
2. Port is not blocked by firewall
3. Logs for metrics server startup

### Issue: No metrics data in Prometheus

**Check:**
1. Prometheus is scraping: Check Prometheus targets UI
2. Scrape interval and timeout are appropriate
3. Check service logs for metric export errors

### Issue: High cardinality warnings

**Solution:** Metrics with labels can create high cardinality. Avoid:
- Using unique IDs (UUIDs) as labels
- High-cardinality dimensions like timestamps
- Labels with unbounded values

**Good labels:**
- `exchange` (binance, bybit)
- `market_type` (spot, perpetual)
- `stream_type` (trade, depth, kline)
- `worker_id` (0-100)

**Bad labels:**
- ❌ `symbol` (thousands of values)
- ❌ `batch_id` (UUID)
- ❌ `timestamp`

---

## Integration Examples

### Kubernetes ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: market-chronicle
spec:
  selector:
    matchLabels:
      app: market-chronicle
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

### Docker Compose with Prometheus

```yaml
version: '3.8'
services:
  market-chronicle:
    image: market-chronicle:latest
    ports:
      - "9090:9090"
  
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9091:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

---

## Further Reading

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [Grafana Dashboarding](https://grafana.com/docs/grafana/latest/dashboards/)
- [OpenTelemetry Rust SDK](https://github.com/open-telemetry/opentelemetry-rust)
