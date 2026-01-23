# Market Chronicle

> Real-time cryptocurrency market data archiving service for analytics and research

Market Chronicle is a Rust-based service that collects real-time market data from multiple cryptocurrency exchanges and archives it to S3-compatible storage in a structured, queryable format.

## Features

- **Multi-Exchange Support**: Binance (Spot & Futures), Bybit (Spot & Perpetual)
- **Real-Time Streaming**: WebSocket connections with automatic reconnection
- **Dynamic Scaling**: Automatically spawns workers based on subscription load
- **Efficient Storage**: Hive-style partitioning with zstd compression
- **Rate Limit Compliance**: Built-in rate limiters for each exchange
- **Graceful Shutdown**: Flush all data to S3 before termination
- **Replay Utility**: Download and replay archived data for testing/backtesting
- **Stateless Architecture**: No disk dependencies, easy horizontal scaling
- **Observability**: OpenTelemetry metrics with Prometheus/Grafana support

## Quick Start

### Prerequisites

- Rust 1.75+ (install from [rustup.rs](https://rustup.rs/))
- Docker and Docker Compose (for local development)
- S3-compatible storage (AWS S3, MinIO, or LakeFS)

### Local Development Setup

1. **Clone the repository**:
```bash
git clone https://github.com/yourusername/market-chronicle.git
cd market-chronicle
```

2. **Start development services** (MinIO + LakeFS + Prometheus + Grafana):
```bash
just setup-dev
```

This starts:
- **Market Chronicle** (data collection service with metrics) at `http://localhost:9090`
- **MinIO** (S3-compatible storage) at `http://localhost:9000`
- **MinIO Console** at `http://localhost:9001` (admin/admin)
- **LakeFS** (data versioning) at `http://localhost:8000`
- **PostgreSQL** (LakeFS metadata)
- **Prometheus** (metrics storage) at `http://localhost:9091`
- **Grafana** (metrics visualization) at `http://localhost:3000` (admin/admin)
- **Vector** (log collection) at `http://localhost:8686`
- **Loki** (log aggregation) at `http://localhost:3100` 

> **Note**: Prometheus runs on port 9091 to avoid conflict with market-chronicle's metrics endpoint (port 9090)

3. **Create configuration file**:
```bash
cp config.example.toml config.toml
```

Edit `config.toml` to configure exchanges and symbols.

4. **Build and run**:
```bash
just build
just run
```

### First Run

For a minimal test configuration, create `config.toml`:

```toml
[s3]
endpoint = "http://localhost:9000"
bucket = "market-data"
region = "us-east-1"
prefix = "dev"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
upload_worker_count = "auto"  # Auto-compute based on workload
# partitions_per_worker = 75  # Optional tuning (default: 75)
batch_size = 500
batch_timeout_ms = 10000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://stream.binance.com:9443/stream"
rest_endpoint = "https://api.binance.com"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
```

Run the service:
```bash
cargo run --bin market-chronicle -- config.toml
```

You should see logs indicating:
- WebSocket workers connecting to Binance
- Market messages being received
- Batches being created and uploaded to S3

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Exchange WebSocket APIs                      │
│                  (Binance, Bybit, etc.)                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────┴────────┐
                    │  WebSocket      │
                    │  Worker Pool    │
                    │  (Dynamic)      │
                    └────────┬────────┘
                             │ MarketMessage
                    ┌────────┴────────┐
                    │  Message        │
                    │  Channel        │
                    │  (10k buffer)   │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │  Batch          │
                    │  Aggregator     │
                    │  (by partition) │
                    └────────┬────────┘
                             │ Batch
                    ┌────────┴────────┐
                    │  Batch          │
                    │  Channel        │
                    │  (1k buffer)    │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │  S3 Upload      │
                    │  Worker Pool    │
                    │  (compress)     │
                    └────────┬────────┘
                             │ NDJSON.zst
                    ┌────────┴────────┐
                    │  S3 / LakeFS    │
                    │  (Hive format)  │
                    └─────────────────┘
```

### Key Design Principles

1. **Stateless**: No disk buffering, all state in memory
2. **Backpressure**: Automatic flow control via bounded channels
3. **Partitioned**: Data organized by exchange, symbol, stream type, time
4. **Compressed**: zstd compression for efficient storage
5. **Scalable**: Easy horizontal scaling (deploy multiple instances)

See [docs/architecture.md](docs/architecture.md) for detailed architecture philosophy.

## Configuration

Market Chronicle uses TOML configuration files. See [config.example.toml](config.example.toml) for a complete annotated example.

### Minimal Configuration

```toml
[s3]
endpoint = "http://localhost:9000"
bucket = "market-data"
region = "us-east-1"
prefix = "production"
access_key_id = "your-key"
secret_access_key = "your-secret"
upload_worker_count = "auto"  # Auto-compute based on workload
# partitions_per_worker = 75  # Optional tuning (default: 75)
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://stream.binance.com:9443/stream"
rest_endpoint = "https://api.binance.com"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade", "depth"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
```

### Subscription Modes

**Explicit Mode** (recommended for testing):
```toml
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
```

**All Mode** (for production, high volume):
```toml
mode = "all"
symbols = []  # Fetched via REST API
symbol_refresh_interval_s = 3600  # Refresh every hour
```

See [docs/configuration-guide.md](docs/configuration-guide.md) for complete configuration reference.

## Data Format

### S3 Key Structure (Hive-Style Partitioning)

```
{prefix}/exchange={exchange}/market-type={spot|perpetual}/stream-type={type}/
symbol={symbol}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.ndjson.zst
```

Example:
```
production/exchange=binance/market-type=spot/stream-type=trade/
symbol=BTCUSDT/year=2024/month=01/day=15/hour=10/
a1b2c3d4-e5f6-7890-abcd-ef1234567890.ndjson.zst
```

### File Format (NDJSON with zstd)

Each file contains newline-delimited JSON, compressed with zstd:

```json
{"local_timestamp":1705334567890,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
{"local_timestamp":1705334568123,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
{"local_timestamp":1705334568456,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
```

### Querying with DuckDB

```sql
-- Install and load httpfs extension
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 credentials
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Query with partition pruning
SELECT 
    local_timestamp,
    symbol,
    data
FROM read_ndjson_auto('s3://market-data/production/exchange=binance/market-type=spot/stream-type=trade/symbol=BTCUSDT/year=2024/month=01/day=15/**/*.ndjson.zst')
WHERE local_timestamp >= 1705334567000 
  AND local_timestamp < 1705338167000
ORDER BY local_timestamp
LIMIT 100;
```

## Replay Utility

Market Chronicle includes a replay utility for testing and backtesting:

```bash
cargo run --bin market-chronicle-replay -- \
  --config config.toml \
  --start-time "2024-01-15T00:00:00Z" \
  --end-time "2024-01-15T23:59:59Z" \
  --exchange binance \
  --symbol BTCUSDT \
  --speed 2.0
```

Options:
- `--start-time`: Start of time range (RFC3339 format)
- `--end-time`: End of time range (RFC3339 format)
- `--exchange`: Filter by exchange (optional)
- `--symbol`: Filter by symbol (optional)
- `--stream-type`: Filter by stream type (optional)
- `--speed`: Replay speed multiplier (1.0 = real-time, 2.0 = 2x speed)
- `--output`: Output file path (optional, defaults to stdout)
- `--stats-only`: Print statistics only, don't output messages

## Production Deployment

### Docker Compose (Recommended for Local/Development)

The project includes a complete Docker Compose setup with all dependencies:

```bash
# Build and start all services (MinIO, LakeFS, Prometheus, Grafana, Market Chronicle)
just docker-up

# View logs
just docker-logs

# Stop all services
just docker-down
```

This starts the complete stack:
- **MinIO**: S3-compatible storage on port 9000
- **LakeFS**: Data versioning on port 8000
- **Prometheus**: Metrics on port 9091
- **Grafana**: Dashboards on port 3000
- **Market Chronicle**: Service with metrics on port 9090

**Configuration**: Copy `config.docker.toml` to `config.toml` and customize:

```bash
cp config.docker.toml config.toml
# Edit config.toml with your settings
just docker-rebuild
```

### Docker (Standalone)

1. **Build Docker image**:
```bash
docker build -t market-chronicle:latest .
```

2. **Run container**:
```bash
docker run -d \
  --name market-chronicle \
  -p 9090:9090 \
  -v $(pwd)/config.toml:/config/config.toml:ro \
  -e RUST_LOG=info \
  market-chronicle:latest
```

3. **View logs**:
```bash
docker logs -f market-chronicle
```

4. **Check metrics**:
```bash
curl http://localhost:9090/metrics
```


### Multi-Instance Deployment

For high availability and rate limit distribution:

1. Deploy 2-3 instances with different S3 endpoints:
```toml
# Instance 1
[s3]
endpoint = "minio-1:9000"

# Instance 2
[s3]
endpoint = "minio-2:9000"

# Instance 3
[s3]
endpoint = "minio-3:9000"
```

2. Each instance collects identical data
3. Query layer handles [deduplication](docs/deduplication.md)
4. Benefits:
   - 3x rate limit capacity
   - No data loss if one instance fails
   - Can detect data corruption by comparing

See [geo-distributed-deployment.md](docs/geo-distributed-deployment.md) for detailed guide.

## Monitoring

Market Chronicle includes built-in OpenTelemetry metrics with Prometheus exporter support.

### Metrics Server

The service exposes metrics on port 9090:

- **Metrics endpoint**: `http://localhost:9090/metrics` (Prometheus format)
- **Health check**: `http://localhost:9090/health`

### Available Metrics

The service tracks comprehensive metrics for all components:

**WebSocket Workers**:
- `worker_messages_received_total` - Total messages received per worker
- `worker_active_streams` - Number of active stream subscriptions
- `worker_reconnects_total` - WebSocket reconnection count

**Batch Aggregator**:
- `batch_messages_processed_total` - Messages processed into batches
- `batch_created_total` - Number of batches created
- `batch_size_messages` - Distribution of batch sizes
- `batch_active` - Currently active batches in memory

**S3 Uploader**:
- `s3_uploads_total` - Successful S3 uploads
- `s3_upload_failures_total` - Failed S3 uploads
- `s3_upload_duration_seconds` - Upload latency distribution
- `s3_upload_bytes_total` - Total bytes uploaded
- `s3_compression_ratio` - Compression ratio distribution

**Symbol Manager**:
- `symbols_discovered_total` - New symbols discovered
- `symbols_delisted_total` - Symbols removed
- `symbols_active` - Currently active symbols

### Prometheus + Grafana Setup

The included `docker-compose.yml` provides a complete monitoring stack:

```bash
# Start all services including Prometheus + Grafana
just setup-dev

# Access Grafana at http://localhost:3000 (admin/admin)
# Prometheus datasource is pre-configured
```

See [docs/metrics.md](docs/metrics.md) for detailed metrics documentation, PromQL examples, and Grafana dashboard setup.

### Health Check

The service logs to stdout/stderr. Monitor for:

- `"Starting graceful shutdown"` - Service stopping
- `"Failed to upload batch"` - S3 upload failures
- `"Connection closed, reconnecting"` - WebSocket issues
- `"Worker shutting down"` - Worker lifecycle

### Alerting

Example Prometheus alert rules are provided in `prometheus-alerts.yml`. Key alerts include:

- Service down for > 5 minutes
- Upload failures > 10% for 5 minutes
- All workers disconnected
- S3 upload latency > 30s
- High memory usage

See [docs/metrics.md](docs/metrics.md) for complete alerting configuration.

## Development

### Build Commands

```bash
just build          # Build all crates (debug)
just build-release  # Build all crates (release)
just test           # Run all tests
just lint           # Run clippy linter
just fmt            # Format code
just run            # Run main service
```

### Development Environment

```bash
just setup-dev      # Start MinIO + LakeFS
just stop-dev       # Stop development services
just clean-dev      # Remove all containers and volumes
just logs-dev       # View logs from all services
```

### Running Tests

```bash
# Run all tests
just test

# Run specific test
cargo test --test config_test

# Run with output
cargo test -- --nocapture
```

## Documentation

- [Architecture](docs/architecture.md) - Design philosophy and tradeoffs
- [Data Flow](docs/data-flow.md) - Complete pipeline with diagrams
- [Graceful Shutdown](docs/graceful-shutdown.md) - Shutdown sequence and data loss
- [Configuration Guide](docs/configuration-guide.md) - Complete config reference
- [Metrics & Observability](docs/metrics.md) - OpenTelemetry, Prometheus, Grafana setup
- [Docker Setup](DOCKER_SETUP.md) - Local development environment guide
- [Geo-Distributed Deployment](docs/geo-distributed-deployment.md) - Multi-instance patterns

## License
