**Objective:** Design and implement a Rust-based service to archive real-time market data from multiple cryptocurrency exchanges to S3.

### Workspace Layout

**Cargo Workspace Structure:**
- **Two binary crates:**
  - `market-chronicle` - Main service binary for real-time data archiving
    - WebSocket connection management
    - Data buffering and snapshot segments
    - S3 upload workers
    - Symbol management and dynamic subscriptions
  - `market-chronicle-replay` - Replay utility binary for debugging and testing
    - Data replay from S3 or local snapshot segments
    - Configurable replay speed and filtering
    - Time-range queries and data validation
- **Shared library code:**
  - Common data structures and types
  - Exchange-specific WebSocket clients
  - S3 client and storage utilities
  - Configuration parsing and validation
  - Compression and serialization utilities

### Core Features

**Configuration:**
- **TOML-based configuration** for all service parameters:
  - Exchange credentials and endpoints
  - **Symbol subscription mode** (per exchange):
    - `mode = "explicit"`: Subscribe to specific symbols listed in `symbols` array
    - `mode = "all"`: Fetch and subscribe to all available trading pairs from exchange
    - `symbol_refresh_interval_s`: Periodic refresh interval for discovering new symbols (only used in "all" mode)
  - Stream subscriptions (stream types, kline intervals)
  - S3 upload worker pool size
  - **Exchange-specific rate limits** (configured per exchange):
    - `ws_connection_rate`: Connections per minute (Binance: 5/min, Bybit: 10/min)
    - `ws_streams_per_connection`: Max subscriptions per connection (Binance Spot: 1024, Bybit: 10)
    - `ws_max_connections`: Hard limit on concurrent connections (Binance: 300, Bybit: 500)
  - Snapshot settings (path, fsync interval)
  - S3 connection details (endpoint, bucket, credentials, region)
  - Buffer thresholds and batch sizes
  - Compression level and retry policies

**Symbol Management:**
- **"all" mode**: Fetch complete list of available symbols via exchange REST APIs
  - Periodic refresh to detect new symbols and delisted pairs
  - Automatic subscription to newly discovered symbols
  - Graceful handling of delisted symbols (close streams, flush data)
- **"explicit" mode**: Subscribe only to symbols specified in configuration
  - No REST API polling or dynamic discovery
  - Static symbol list defined in TOML

**WebSocket Management:**
- Implement connection pooling for WebSocket streams
- Support dynamic subscribing/unsubscribing to live streams
- Built-in rate limiter to comply with exchange API limits
- Handle exchange-specific connection limits (e.g., max streams per connection)
- **✅ Dynamic Worker Spawning:**
  - Workers spawn on-demand based on subscription load
  - Automatically calculate required workers: `ceil(total_streams / streams_per_connection)`
  - **Multiple stream types per symbol support** (e.g., `["trade", "depth", "kline"]`)
  - Correct stream counting: trade=1, depth=1, kline=N (per interval)
  - **Workers never scale down** - once spawned, workers remain active
  - **Streams are permanently assigned** - streams cannot be moved between workers
    - Simplifies connection management and state tracking
    - Prevents complex rebalancing logic
    - Ensures stable stream subscriptions
    - Workers maintain dedicated WebSocket connections
- **✅ Connection Rate Limiting:**
  - **Exchange-specific rate limiters** for creating new WebSocket connections
    - Binance: 5 connections/minute, up to 1024 streams/connection (Spot), 200 streams/connection (Futures)
    - Bybit: 10 connections/minute, up to 10 streams/connection
  - Prevents overwhelming exchanges with connection requests
  - Respects per-exchange API limits and max connection constraints
  - Exponential backoff on connection failures

**WebSocket Workers:**
- Pool of dedicated worker threads for handling WebSocket connections
- Each worker manages one WebSocket connection
- Message parsing and validation
- Automatic reconnection with exponential backoff on connection failures
- Heartbeat/ping-pong handling to maintain connections
- Deserialization of exchange-specific message formats
- **✅ Unsubscribe support** for dynamic symbol management

**Data Pipeline:**
- **Direct WebSocket-to-S3 architecture** (stateless, no disk buffering):
  - WebSocket workers send messages directly to batch aggregator via in-memory channels
  - Batch aggregator groups messages by partition key in memory
  - Batches trigger on size (e.g., 1000 messages) OR timeout (e.g., 5 seconds)
  - S3 upload workers compress with zstd and upload batches to S3
  - **No disk writes**: All buffering happens in memory via tokio channels
  - **Simplified architecture**: Fewer moving parts, easier to reason about
  - **Backpressure handling**: 
    - If S3 uploads are slow, channels fill up
    - Backpressure propagates to WebSocket workers
    - Workers slow down or temporarily pause message consumption
  - **Tradeoffs:**
    - **Data loss on crash**: Messages in memory (not yet uploaded) are lost if service crashes
    - **No recovery mechanism**: No disk-based durability buffer
    - **Simpler operations**: No disk I/O overhead, no segment management, no cleanup tasks
    - **Lower latency**: Data uploaded to S3 as soon as batch is ready (no disk write step)
  - **Memory usage**: Bounded by channel buffer sizes (configurable)

**Batch Aggregator & S3 Upload Workers:**
- **Batch Aggregator:**
  - Receives messages from WebSocket workers via in-memory channel
  - Groups messages by partition key (exchange, market_type, stream_type, symbol, timestamp hour)
  - Batches trigger on size (batch_size) OR age (batch_timeout)
  - Sends complete batches to S3 upload worker pool
- **S3 Upload Workers:**
  - Pool of dedicated worker threads for parallel S3 uploads
  - Serialize batch to NDJSON format
  - Apply zstd compression
  - Upload to S3 with Hive-style partitioning
  - Retry logic with exponential backoff for failed uploads
  - Generate UUID-based keys for each batch (enables idempotent retries)
- **Benefits of direct architecture:**
  - ✅ Minimal latency: Data uploaded as soon as batch ready
  - ✅ Simple codebase: No disk I/O, no segment management
  - ✅ Lower operational complexity
  - ✅ Backpressure automatically handled by channels
- **Tradeoffs:**
  - ❌ No crash recovery: Data lost if service crashes before upload
  - ❌ No local debugging: Can't inspect pre-upload data on disk

**Storage:**
- Archive data to S3-compatible storage (LakeFS)
- **S3 data format:** Newline-delimited JSON (NDJSON) with zstd compression (`.ndjson.zst` files)
- **Hive-style partitioning** for efficient querying and data organization:
  - `exchange=<exchange>/market_type=<spot|perpetual>/stream_type=<trade|kline|depth|etc>/symbol=<symbol>/year=<yyyy>/month=<mm>/day=<dd>/hour=<hh>/`
  - Enables partition pruning for optimized query performance
- **S3 file format example:**
  ```json
  {"timestamp":1768864236,"exchange":"binance","market_type":"spot","stream_type":"depth","symbol":"BTCUSDT","data":{...}}
  {"timestamp":1768864237,"exchange":"binance","market_type":"spot","stream_type":"depth","symbol":"ETHUSDT","data":{...}}
  {"timestamp":1768864238,"exchange":"binance","market_type":"spot","stream_type":"trade","symbol":"BTCUSDT","data":{...}}
  ```
  - One entry per line (NDJSON)
  - Compressed with zstd before upload
  - Human-readable after decompression
  - Efficient for time-series analytics

**Replay Mode:**
- **Data replay capability** for debugging and testing:
  - CLI tool to replay data from S3 storage
  - Query by time range, exchange, symbol, and stream type
  - Configurable replay speed (real-time, faster, slower)
  - Download and decompress `.ndjson.zst` files from S3
  - Output options: stdout, file, or programmatic consumption
  - Use cases: bug reproduction, backtesting, development, and data validation
  - Note: No local snapshot replay (architecture is stateless)

**Graceful Shutdown:**
- **Simplified shutdown sequence** (stateless architecture):
  - Uses `tokio::sync::Notify` for reliable signal propagation
  - Explicit channel closure with `drop()` to signal downstream components
  - **Shutdown order:**
    1. Abort symbol management tasks (prevent new worker spawns in "all" mode)
    2. Shutdown WebSocket workers (stop receiving new data)
    3. Wait briefly for in-flight messages to reach batch aggregator
    4. Close batch aggregator channel → triggers flush of remaining batches
    5. Wait for batch aggregator to complete
    6. Wait for all S3 upload workers to complete (with 30s timeout)
  - **Channel closure detection:**
    - `tokio::select!` with `biased` keyword prioritizes message reception
    - Explicit `None` handling when channels close
    - Prevents shutdown hangs
  - **Timeouts for each stage** to prevent indefinite hangs
  - **Data loss warning**: Any data not yet uploaded to S3 is lost (no disk buffering)

**Geo-Distributed Deployment:**
- **Multiple independent service instances** for high availability and rate limit distribution
- Each instance runs identical configuration but connects from different IP addresses
- **Solves key problems:**
  - **Redundancy**: Multiple services ensure data collection continues if one fails
  - **Rate limit distribution**: Each IP gets separate rate limit quota
    - Example: 3 instances × 300 connections/IP = 900 total connections to Binance
- **Stateless architecture enables easy distribution:**
  - No coordination between instances required
  - No shared state or distributed consensus
  - Simple deployment: just launch more instances in different regions
- **Data handling:**
  - Each instance writes to separate S3 bucket or prefix
  - Duplicate data is created (multiple instances capture same events)
  - **Downstream deduplication**: Query layer handles duplicate removal at query time
  - Tradeoff: Higher storage cost for better reliability and rate limits

**Build & Development:**
- **justfile** for common development tasks:
  - Build (debug/release)
  - Run tests (unit, integration)
  - Linting and formatting (clippy, rustfmt)
  - Run service locally
  - Docker image build
  - Clean build artifacts
  - Install dependencies
  - **✅ Docker Compose commands:**
    - `just setup-dev` - Start all services (MinIO, PostgreSQL, LakeFS)
    - `just stop-dev` - Stop services
    - `just logs-dev` - View logs
    - `just clean-dev` - Remove all containers and volumes
    - `just minio-console` - Open MinIO console
    - `just lakefs-console` - Open LakeFS console

**Documentation:**
- **Comprehensive documentation** in `docs/`:
  - **[Data Flow Architecture](./docs/data-flow.md)** - Complete pipeline with diagrams
    - Direct WebSocket → Batch Aggregator → S3 flow
    - Channel communication patterns
    - Backpressure handling
    - Error handling scenarios
    - Performance considerations and tuning
  - **[Graceful Shutdown](./docs/graceful-shutdown.md)** - Shutdown sequence documentation
    - Simplified shutdown flow with diagrams
    - Technical implementation details
    - Data loss scenarios and mitigation
    - Troubleshooting common issues
  - **[Configuration Guide](./docs/configuration-guide.md)** - Complete config reference
    - All configuration options explained
    - Exchange-specific rate limits
    - Symbol subscription modes (explicit vs. all)
    - Batch size and timeout tuning
    - Performance tuning profiles
    - Real-world examples and troubleshooting
  - **[Architecture Philosophy](./docs/architecture.md)**
    - Stateless design rationale
    - Memory-only buffering tradeoffs
    - When to accept data loss
    - Backpressure strategy
    - Comparison with disk-based architectures
  - **[Geo-Distributed Deployment](./docs/geo-distributed-deployment.md)**
    - Multiple instance deployment patterns
    - Redundancy and high availability
    - Rate limit distribution across IPs
    - Duplicate data handling and downstream deduplication
    - Cost/benefit analysis and when to use

### Supported Exchanges & Streams

**Binance Spot:**
- `<symbol>@trade` - Individual trade data
- `<symbol>@kline_<interval>` - Candlestick data with configurable intervals
- `<symbol>@depth` - Order book depth updates

**Binance USDT Perpetual:**
- `<symbol>@aggTrade` - Aggregated trade data
- `!markPrice@arr@1s` - All mark price updates (1-second intervals)
- `<symbol>@kline_<interval>` - Candlestick data with configurable intervals
- `<symbol>@depth` - Order book depth updates

**Bybit (Spot & USDT Perpetual):**
- `publicTrade.{symbol}` - Public trade data
- `kline.{interval}.{symbol}` - Candlestick data
- `orderbook.{depth}.{symbol}` - Order book snapshots and updates

### Configuration Example

**Exchange-specific rate limits in TOML:**

```toml
[s3]
endpoint = "http://localhost:8000"
bucket = "exchange-data"
region = "us-east-1"
prefix = "main"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
upload_worker_count = 4
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://stream.binance.com:9443/stream"
rest_endpoint = "https://api.binance.com"
mode = "all"  # "all" or "explicit"
symbols = []  # Empty when mode = "all", or explicit list like ["BTCUSDT", "ETHUSDT"]
symbol_refresh_interval_s = 3600  # Only used when mode = "all"
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "1h"]
# Binance-specific rate limits (https://binance-docs.github.io/apidocs/spot/en/#websocket-limits)
ws_connection_rate = 5  # connections per minute
ws_streams_per_connection = 1024  # Binance allows up to 1024 streams per connection
ws_max_connections = 300  # Maximum concurrent connections per IP

[[exchanges]]
name = "binance"
market_type = "perpetual"
ws_endpoint = "wss://fstream.binance.com/stream"
rest_endpoint = "https://fapi.binance.com"
mode = "explicit"  # "all" or "explicit"
symbols = ["BTCUSDT", "ETHUSDT"]  # Required when mode = "explicit"
symbol_refresh_interval_s = 0  # Not used in explicit mode
streams = ["aggTrade", "depth", "kline", "markPrice"]
kline_intervals = ["1m", "5m"]
# Binance Futures rate limits
ws_connection_rate = 5
ws_streams_per_connection = 200  # Futures has lower limit than spot
ws_max_connections = 300

[[exchanges]]
name = "bybit"
market_type = "spot"
ws_endpoint = "wss://stream.bybit.com/v5/public/spot"
rest_endpoint = "https://api.bybit.com"
mode = "all"  # "all" or "explicit"
symbols = []  # Empty when mode = "all"
symbol_refresh_interval_s = 3600  # Only used when mode = "all"
streams = ["publicTrade", "kline", "orderbook"]
# Bybit-specific rate limits (https://bybit-exchange.github.io/docs/v5/ws/connect)
ws_connection_rate = 10  # Bybit allows 10 connections per minute
ws_streams_per_connection = 10  # Bybit V5 allows max 10 subscriptions per connection
ws_max_connections = 500

[[exchanges]]
name = "bybit"
market_type = "perpetual"
ws_endpoint = "wss://stream.bybit.com/v5/public/linear"
rest_endpoint = "https://api.bybit.com"
mode = "explicit"  # "all" or "explicit"
symbols = ["BTCUSDT", "ETHUSDT"]  # Required when mode = "explicit"
symbol_refresh_interval_s = 0  # Not used in explicit mode
streams = ["publicTrade", "kline", "orderbook"]
# Bybit perpetual has same limits as spot
ws_connection_rate = 10
ws_streams_per_connection = 10
ws_max_connections = 500
```
