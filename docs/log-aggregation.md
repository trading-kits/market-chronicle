# Log Aggregation with Vector + Loki + Grafana

This guide explains the log aggregation setup using Vector, Loki, and Grafana.

## Architecture

```
Docker Containers
       ↓
    Vector (Log Collection & Processing)
       ↓
    Loki (Log Storage)
       ↓
   Grafana (Visualization)
```

## Components

### Vector
- **Purpose**: High-performance log collection from Docker containers
- **Port**: 8686 (API/Health)
- **Config**: `vector.toml`
- Automatically collects logs from all Docker containers
- Parses JSON logs and extracts log levels
- Adds labels for efficient querying in Loki

### Loki
- **Purpose**: Log storage and indexing
- **Port**: 3100
- **Config**: `loki-config.yml`
- Stores logs efficiently with 31-day retention
- Indexed by labels (container, level, image)

### Grafana
- **Purpose**: Log visualization and exploration
- **Port**: 3000
- **Login**: admin / admin
- Automatically configured with Loki datasource

## Usage

### Starting the Stack

```bash
# Start all services
docker-compose up -d

# Check Vector is collecting logs
docker logs -f market-chronicle-vector

# Check Loki is receiving logs
curl http://localhost:3100/ready
```

### Viewing Logs in Grafana

1. Open Grafana: http://localhost:3000
2. Login: admin / admin (Loki datasource is **auto-configured**)
3. Go to **Explore** (compass icon in sidebar)
4. Select **Loki** datasource

### Example Queries

```logql
# All logs from market-chronicle container
{container_name="market-chronicle"}

# Error logs from all containers
{level="ERROR"}

# Logs from specific image
{image=~".*market-chronicle.*"}

# Logs containing specific text
{container_name="market-chronicle"} |= "error"

# Logs with rate over time
rate({container_name="market-chronicle"}[5m])

# JSON field extraction
{container_name="market-chronicle"} | json | level="ERROR"
```

### LogQL Operators

- `|=` - Contains string
- `!=` - Does not contain string
- `|~ "regex"` - Regex match
- `!~ "regex"` - Negative regex match
- `| json` - Parse JSON logs
- `| logfmt` - Parse logfmt logs
- `rate()` - Calculate rate over time
- `count_over_time()` - Count logs over time

## Configuration

### Vector Configuration (`deploy/vector.toml`)

The Vector pipeline:
1. **Source**: Collects from Docker socket
2. **Transform**: Parses and enriches logs
3. **Sink**: Sends to Loki

Key features:
- Automatic JSON parsing
- Log level extraction and normalization
- Container metadata (name, image, ID)
- Loki label generation

### Loki Configuration (`loki-config.yml`)

Settings:
- **Retention**: 31 days (`retention_period: 744h`)
- **Storage**: Filesystem (for simplicity)
- **Cache**: 100MB embedded cache

To change retention:
```yaml
limits_config:
  retention_period: 168h  # 7 days
```

### Adding Custom Labels

Edit `deploy/vector.toml` to add more labels:

```toml
[transforms.add_loki_labels]
type = "remap"
inputs = ["process_logs"]
source = '''
  .loki_labels = {
    "job": "docker",
    "container_name": .container_name,
    "image": .image,
    "level": .level,
    "environment": "production"  # Add custom label
  }
'''
```

## Troubleshooting

### Vector not collecting logs

```bash
# Check Vector logs
docker logs market-chronicle-vector

# Verify Docker socket access
docker exec market-chronicle-vector ls -la /var/run/docker.sock

# Check Vector API
curl http://localhost:8686/health
```

### Loki not receiving logs

```bash
# Check Loki logs
docker logs market-chronicle-loki

# Test Loki API
curl http://localhost:3100/ready

# Query Loki directly
curl -G -s "http://localhost:3100/loki/api/v1/query" \
  --data-urlencode 'query={job="docker"}'
```

### No logs in Grafana

1. Check Loki datasource configuration in Grafana
2. Verify time range in Grafana (default may be too short)
3. Try query: `{job="docker"}` to see all logs
4. Check Vector is sending to Loki:
   ```bash
   docker logs market-chronicle-vector | grep -i loki
   ```

## Performance Tuning

### High Log Volume

If you have high log volumes, adjust Vector's batch settings in `deploy/vector.toml`:

```toml
[sinks.loki]
type = "loki"
inputs = ["add_loki_labels"]
endpoint = "http://loki:3100"
encoding.codec = "json"

# Batch settings for better performance
batch.max_bytes = 1048576  # 1MB
batch.timeout_secs = 5
```

### Memory Usage

Loki's memory can be adjusted in `deploy/loki-config.yml`:

```yaml
query_range:
  results_cache:
    cache:
      embedded_cache:
        enabled: true
        max_size_mb: 500  # Increase for better query performance
```

## Filtering Specific Containers

To only collect logs from specific containers, edit `deploy/vector.toml`:

```toml
[sources.docker_logs]
type = "docker_logs"
include_containers = ["market-chronicle", "market-chronicle-minio"]
```

Or exclude containers:

```toml
[sources.docker_logs]
type = "docker_logs"
exclude_containers = ["market-chronicle-minio-init"]
```

## Advanced: Sending Logs to Multiple Destinations

Vector can send logs to multiple places simultaneously. Add another sink in `deploy/vector.toml`:

```toml
# Send to S3 for long-term storage
[sinks.s3_archive]
type = "aws_s3"
inputs = ["process_logs"]
bucket = "logs-archive"
region = "us-east-1"
compression = "gzip"
encoding.codec = "json"
```

## Metrics

Vector exposes metrics at http://localhost:8686/metrics (Prometheus format).

Key metrics:
- `component_received_events_total` - Events received by Vector
- `component_sent_events_total` - Events sent to Loki
- `component_errors_total` - Errors in pipeline

## Log Levels

Vector automatically normalizes log levels to:
- `DEBUG`
- `INFO`
- `WARN` / `WARNING`
- `ERROR`
- `FATAL` / `CRITICAL`

This allows consistent filtering across all containers regardless of their logging format.

## Next Steps

1. **Create dashboards** - Build Grafana dashboards for common queries
2. **Set up alerts** - Configure Loki alerting rules for errors
3. **Add sampling** - Sample high-volume debug logs in Vector
4. **Long-term storage** - Archive old logs to S3
5. **Structured logging** - Update your apps to log JSON for better parsing

## Resources

- [LogQL Documentation](https://grafana.com/docs/loki/latest/logql/)
- [Vector Documentation](https://vector.dev/docs/)
- [Loki Documentation](https://grafana.com/docs/loki/latest/)
