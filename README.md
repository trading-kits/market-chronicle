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

2. **Start development services** (MinIO + LakeFS):
```bash
make setup-dev
```

This starts:
- **MinIO** (S3-compatible storage) at `http://localhost:9000`
- **MinIO Console** at `http://localhost:9001` (admin/admin)
- **LakeFS** (data versioning) at `http://localhost:8000`
- **PostgreSQL** (LakeFS metadata)

3. **Create configuration file**:
```bash
cp config.example.toml config.toml
```

Edit `config.toml` to configure exchanges and symbols.

4. **Build and run**:
```bash
make build
make run
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
upload_worker_count = 2
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
upload_worker_count = 4
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
{prefix}/exchange={exchange}/market_type={spot|perpetual}/stream_type={type}/
symbol={symbol}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.ndjson.zst
```

Example:
```
production/exchange=binance/market_type=spot/stream_type=trade/
symbol=BTCUSDT/year=2024/month=01/day=15/hour=10/
a1b2c3d4-e5f6-7890-abcd-ef1234567890.ndjson.zst
```

### File Format (NDJSON with zstd)

Each file contains newline-delimited JSON, compressed with zstd:

```json
{"timestamp":1705334567890,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
{"timestamp":1705334568123,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
{"timestamp":1705334568456,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
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
    timestamp,
    symbol,
    data
FROM read_ndjson_auto('s3://market-data/production/exchange=binance/market_type=spot/stream_type=trade/symbol=BTCUSDT/year=2024/month=01/day=15/**/*.ndjson.zst')
WHERE timestamp >= 1705334567000 
  AND timestamp < 1705338167000
ORDER BY timestamp
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

### Docker

1. **Build Docker image**:
```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin market-chronicle

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/market-chronicle /usr/local/bin/
ENTRYPOINT ["market-chronicle"]
```

2. **Run container**:
```bash
docker build -t market-chronicle .
docker run -v $(pwd)/config.toml:/config.toml market-chronicle /config.toml
```

### Kubernetes

Example deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: market-chronicle
spec:
  replicas: 2  # For high availability
  selector:
    matchLabels:
      app: market-chronicle
  template:
    metadata:
      labels:
        app: market-chronicle
    spec:
      containers:
      - name: market-chronicle
        image: market-chronicle:latest
        args: ["/config/config.toml"]
        volumeMounts:
        - name: config
          mountPath: /config
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
      volumes:
      - name: config
        configMap:
          name: market-chronicle-config
```

### Multi-Instance Deployment

For high availability and rate limit distribution:

1. Deploy 2-3 instances with different S3 prefixes:
```toml
# Instance 1
[s3]
prefix = "instance-1"

# Instance 2
[s3]
prefix = "instance-2"

# Instance 3
[s3]
prefix = "instance-3"
```

2. Each instance collects identical data
3. Query layer handles deduplication
4. Benefits:
   - 3x rate limit capacity
   - No data loss if one instance fails
   - Can detect data corruption by comparing

See [docs/geo-distributed-deployment.md](docs/geo-distributed-deployment.md) for detailed guide.

## Monitoring

### Key Metrics

Track these metrics for production deployments:

1. **Message Rate**: Messages received per second
2. **Batch Rate**: Batches created per second
3. **Upload Rate**: Successful uploads per second
4. **Channel Fullness**: Message/batch channel utilization
5. **Worker Health**: WebSocket connection status
6. **Upload Latency**: Time from message to S3
7. **Error Rate**: Failed uploads, reconnections

### Health Check

The service logs to stdout/stderr. Monitor for:

- `"Starting graceful shutdown"` - Service stopping
- `"Failed to upload batch"` - S3 upload failures
- `"Connection closed, reconnecting"` - WebSocket issues
- `"Worker shutting down"` - Worker lifecycle

### Alerting

Set up alerts for:

- Service down for > 5 minutes
- Upload failures > 10% for 5 minutes
- All workers disconnected
- Channel utilization > 90%

## Development

### Build Commands

```bash
make build          # Build all crates (debug)
make build-release  # Build all crates (release)
make test           # Run all tests
make lint           # Run clippy linter
make fmt            # Format code
make run            # Run main service
```

### Development Environment

```bash
make setup-dev      # Start MinIO + LakeFS
make stop-dev       # Stop development services
make clean-dev      # Remove all containers and volumes
make logs-dev       # View logs from all services
make minio-console  # Open MinIO console
make lakefs-console # Open LakeFS console
```

### Running Tests

```bash
# Run all tests
make test

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
- [Geo-Distributed Deployment](docs/geo-distributed-deployment.md) - Multi-instance patterns

## Troubleshooting

### Service won't start

**Error**: `Configuration validation failed`

**Solution**: Check config file syntax and required fields. Run:
```bash
cargo run --bin market-chronicle -- config.toml 2>&1 | grep -i error
```

### No data appearing in S3

**Possible causes**:
1. S3 credentials incorrect - check endpoint, bucket, keys
2. Workers not connecting - check WebSocket endpoint
3. No symbols configured - verify `symbols` array or `mode = "all"`
4. Batch size too large - decrease `batch_size` for testing

**Debug**:
```bash
RUST_LOG=debug cargo run --bin market-chronicle -- config.toml
```

### High memory usage

**Causes**:
- Too many active batches (many partitions)
- Channel buffers too large
- Too many workers

**Solutions**:
- Reduce number of subscribed symbols
- Decrease `batch_size`
- Use explicit mode instead of all mode

### Upload failures

**Error**: `Failed to upload batch after 5 retries`

**Possible causes**:
1. S3 service down or unreachable
2. Invalid credentials
3. Network issues
4. Bucket doesn't exist

**Debug**:
```bash
# Test S3 connectivity
aws s3 ls s3://your-bucket --endpoint-url http://localhost:9000
```

## Performance

### Expected Throughput

| Streams | Messages/sec | Workers | Upload Workers | Memory | Storage/day |
|---------|-------------|---------|----------------|--------|-------------|
| 10      | 50          | 1       | 2              | ~200MB | ~1GB        |
| 100     | 500         | 1       | 4              | ~500MB | ~10GB       |
| 1000    | 5000        | 1       | 8              | ~1GB   | ~100GB      |
| 10000   | 50000       | 10      | 16             | ~5GB   | ~1TB        |

### Tuning Guidelines

**Low Volume** (< 100 msg/s):
```toml
batch_size = 500
batch_timeout_ms = 10000
upload_worker_count = 2
compression_level = 5
```

**High Volume** (> 1000 msg/s):
```toml
batch_size = 2000
batch_timeout_ms = 2000
upload_worker_count = 8
compression_level = 1
```

## License

[Choose appropriate license]

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `make lint` and `make test`
5. Submit a pull request

## Support

- Documentation: [docs/](docs/)
- Issues: [GitHub Issues](https://github.com/yourusername/market-chronicle/issues)
- Discussions: [GitHub Discussions](https://github.com/yourusername/market-chronicle/discussions)

## Acknowledgments

Built with:
- [Tokio](https://tokio.rs/) - Async runtime
- [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite) - WebSocket
- [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust) - S3 client
- [zstd](https://github.com/facebook/zstd) - Compression
- [serde](https://serde.rs/) - Serialization
