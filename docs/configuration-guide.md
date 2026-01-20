# Configuration Guide

## Overview

Market Chronicle uses TOML configuration files to define all service parameters. This guide provides complete documentation for all configuration options, tuning recommendations, and real-world examples.

## Configuration File Structure

```toml
[s3]
# S3 configuration...

[[exchanges]]
# Exchange configuration...

[[exchanges]]
# Another exchange...
```

## S3 Configuration

### Complete S3 Section

```toml
[s3]
endpoint = "http://localhost:9000"           # S3-compatible endpoint URL
bucket = "market-data"                       # S3 bucket name
region = "us-east-1"                         # AWS region
prefix = "production"                        # Prefix for all S3 keys
access_key_id = "your-access-key"            # S3 access key
secret_access_key = "your-secret-key"        # S3 secret key
upload_worker_count = 4                      # Number of parallel upload workers
batch_size = 1000                            # Messages per batch before flush
batch_timeout_ms = 5000                      # Max age of batch before flush (milliseconds)
compression_level = 3                        # zstd compression level (1-22)
```

### S3 Parameters

#### `endpoint`
- **Type**: String
- **Required**: Yes
- **Example**: `"http://localhost:9000"`, `"https://s3.amazonaws.com"`
- **Description**: S3-compatible storage endpoint
- **Notes**: 
  - Use MinIO for local development
  - Use LakeFS for versioned data lake
  - Use AWS S3 for production

#### `bucket`
- **Type**: String
- **Required**: Yes
- **Example**: `"market-data"`, `"crypto-archive"`
- **Description**: S3 bucket name where data will be stored
- **Notes**: Bucket must exist before starting service

#### `region`
- **Type**: String
- **Required**: Yes
- **Example**: `"us-east-1"`, `"eu-west-1"`
- **Description**: AWS region for S3 bucket
- **Notes**: Use correct region to avoid cross-region fees

#### `prefix`
- **Type**: String
- **Required**: Yes
- **Example**: `"production"`, `"dev"`, `"instance-1"`
- **Description**: Top-level prefix for all S3 keys
- **Notes**: 
  - Use to separate environments
  - Use to separate multiple service instances
  - Enables easy data cleanup/migration

#### `access_key_id` / `secret_access_key`
- **Type**: String
- **Required**: Yes
- **Description**: S3 credentials
- **Security**: Store in environment variables or secrets manager

#### `upload_worker_count`
- **Type**: Integer
- **Required**: Yes
- **Range**: 1-100
- **Default**: 4
- **Description**: Number of concurrent S3 upload workers
- **Tuning**:
  - More workers = higher upload throughput
  - More workers = more memory usage
  - Formula: `ceil(target_batches_per_second * avg_upload_time_seconds)`

#### `batch_size`
- **Type**: Integer
- **Required**: Yes
- **Range**: 100-10000
- **Default**: 1000
- **Description**: Number of messages per batch before flush
- **Tuning**:
  - Larger = fewer S3 uploads, better compression
  - Smaller = lower latency, more S3 operations
  - Trade-off: Latency vs S3 cost

#### `batch_timeout_ms`
- **Type**: Integer
- **Required**: Yes
- **Range**: 1000-60000
- **Default**: 5000
- **Description**: Maximum batch age before flush (milliseconds)
- **Tuning**:
  - Longer = better batching for low-volume symbols
  - Shorter = lower latency
  - Trade-off: Latency vs batch efficiency

#### `compression_level`
- **Type**: Integer
- **Required**: Yes
- **Range**: 1-22
- **Default**: 3
- **Description**: zstd compression level
- **Tuning**:
  - Higher = better compression, slower encoding
  - Lower = faster encoding, worse compression
  - Levels 1-3: Fast compression
  - Levels 4-9: Balanced
  - Levels 10-22: Maximum compression (very slow)

## Exchange Configuration

### Complete Exchange Section

```toml
[[exchanges]]
name = "binance"                                      # Exchange name
market_type = "spot"                                  # "spot" or "perpetual"
ws_endpoint = "wss://stream.binance.com:9443/stream"  # WebSocket endpoint
rest_endpoint = "https://api.binance.com"             # REST API endpoint
mode = "explicit"                                     # "explicit" or "all"
symbols = ["BTCUSDT", "ETHUSDT"]                      # Symbols to subscribe (explicit mode)
symbol_refresh_interval_s = 3600                      # Symbol refresh interval (all mode)
streams = ["trade", "depth", "kline"]                 # Stream types to subscribe
kline_intervals = ["1m", "5m", "1h"]                  # Kline intervals (if kline stream)
ws_connection_rate = 5                                # Max connections per minute
ws_streams_per_connection = 1024                      # Max streams per connection
ws_max_connections = 300                              # Max concurrent connections
```

### Exchange Parameters

#### `name`
- **Type**: String
- **Required**: Yes
- **Values**: `"binance"`, `"bybit"`
- **Description**: Exchange identifier

#### `market_type`
- **Type**: String
- **Required**: Yes
- **Values**: `"spot"`, `"perpetual"`
- **Description**: Market type to subscribe to

#### `ws_endpoint`
- **Type**: String
- **Required**: Yes
- **Examples**:
  - Binance Spot: `"wss://stream.binance.com:9443/stream"`
  - Binance Futures: `"wss://fstream.binance.com/stream"`
  - Bybit Spot: `"wss://stream.bybit.com/v5/public/spot"`
  - Bybit Linear: `"wss://stream.bybit.com/v5/public/linear"`

#### `rest_endpoint`
- **Type**: String
- **Required**: Yes
- **Examples**:
  - Binance: `"https://api.binance.com"`
  - Binance Futures: `"https://fapi.binance.com"`
  - Bybit: `"https://api.bybit.com"`

#### `mode`
- **Type**: String
- **Required**: Yes
- **Values**: `"explicit"`, `"all"`
- **Description**: Symbol subscription mode
- **`explicit`**: Subscribe only to symbols listed in `symbols` array
- **`all`**: Fetch all available symbols via REST API and subscribe to all

#### `symbols`
- **Type**: Array of strings
- **Required**: Only if `mode = "explicit"`
- **Example**: `["BTCUSDT", "ETHUSDT", "BNBUSDT"]`
- **Description**: List of symbols to subscribe to
- **Notes**: Must be uppercase

#### `symbol_refresh_interval_s`
- **Type**: Integer
- **Required**: Only if `mode = "all"`
- **Range**: 600-86400 (10 minutes to 1 day)
- **Default**: 3600 (1 hour)
- **Description**: How often to fetch symbol list from REST API
- **Notes**: Discovers new symbols and handles delistings

#### `streams`
- **Type**: Array of strings
- **Required**: Yes
- **Binance Values**: `"trade"`, `"depth"`, `"kline"`, `"aggTrade"`, `"markPrice"`
- **Bybit Values**: `"publicTrade"`, `"kline"`, `"orderbook"`
- **Description**: Stream types to subscribe to for each symbol

#### `kline_intervals`
- **Type**: Array of strings
- **Required**: Only if `streams` contains `"kline"`
- **Binance Values**: `"1m"`, `"5m"`, `"15m"`, `"1h"`, `"4h"`, `"1d"`
- **Bybit Values**: `"1"`, `"5"`, `"15"`, `"60"`, `"240"`, `"D"`
- **Description**: Kline (candlestick) intervals to subscribe to
- **Notes**: Each interval creates a separate stream

#### `ws_connection_rate`
- **Type**: Integer
- **Required**: Yes
- **Binance**: 5 connections/minute
- **Bybit**: 10 connections/minute
- **Description**: Rate limit for creating new WebSocket connections
- **Notes**: Must match exchange limits to avoid bans

#### `ws_streams_per_connection`
- **Type**: Integer
- **Required**: Yes
- **Binance Spot**: 1024
- **Binance Futures**: 200
- **Bybit**: 10
- **Description**: Maximum streams per WebSocket connection
- **Notes**: Must match exchange limits

#### `ws_max_connections`
- **Type**: Integer
- **Required**: Yes
- **Binance**: 300 per IP
- **Bybit**: 500 per IP
- **Description**: Hard limit on concurrent connections
- **Notes**: Helps prevent accidental rate limit violations

## Stream Counting

Understanding how streams are counted is critical for capacity planning:

### Stream Types

| Stream Type | Streams per Symbol |
|-------------|-------------------|
| `trade` | 1 |
| `depth` | 1 |
| `kline` | N (one per interval) |
| `aggTrade` | 1 |
| `markPrice` | 1 (global, not per-symbol) |

### Example Calculations

**Example 1**: Binance Spot, explicit mode
```toml
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # 3 symbols
streams = ["trade", "depth"]                  # 2 streams per symbol
```
Total streams: 3 × 2 = **6 streams** = **1 worker needed**

**Example 2**: Binance Spot, explicit mode with klines
```toml
symbols = ["BTCUSDT", "ETHUSDT"]              # 2 symbols
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "1h"]          # 3 intervals
```
Total streams: 2 × (1 trade + 1 depth + 3 klines) = 2 × 5 = **10 streams** = **1 worker needed**

**Example 3**: Bybit Spot, all mode
```toml
mode = "all"  # Assume 500 symbols discovered
streams = ["publicTrade", "orderbook"]        # 2 streams per symbol
```
Total streams: 500 × 2 = **1000 streams** = **100 workers needed** (10 streams per worker)

## Configuration Examples

### Example 1: Development (Single Symbol)

```toml
[s3]
endpoint = "http://localhost:9000"
bucket = "dev-data"
region = "us-east-1"
prefix = "dev"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
upload_worker_count = 2
batch_size = 500
batch_timeout_ms = 10000
compression_level = 1

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

### Example 2: Production (Multiple Exchanges, High Volume)

```toml
[s3]
endpoint = "https://s3.us-east-1.amazonaws.com"
bucket = "prod-market-data"
region = "us-east-1"
prefix = "instance-1"
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
upload_worker_count = 8
batch_size = 2000
batch_timeout_ms = 3000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://stream.binance.com:9443/stream"
rest_endpoint = "https://api.binance.com"
mode = "all"
symbols = []
symbol_refresh_interval_s = 3600
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m"]
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300

[[exchanges]]
name = "binance"
market_type = "perpetual"
ws_endpoint = "wss://fstream.binance.com/stream"
rest_endpoint = "https://fapi.binance.com"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
symbol_refresh_interval_s = 0
streams = ["aggTrade", "depth", "markPrice"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 200
ws_max_connections = 300

[[exchanges]]
name = "bybit"
market_type = "spot"
ws_endpoint = "wss://stream.bybit.com/v5/public/spot"
rest_endpoint = "https://api.bybit.com"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT"]
symbol_refresh_interval_s = 0
streams = ["publicTrade", "orderbook"]
kline_intervals = []
ws_connection_rate = 10
ws_streams_per_connection = 10
ws_max_connections = 500
```

### Example 3: Testing (Minimal Configuration)

```toml
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = 1
batch_size = 10
batch_timeout_ms = 1000
compression_level = 1

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

## Troubleshooting

### Issue: Configuration validation failed

**Error**: `Exchange X has mode='explicit' but symbols list is empty`

**Solution**: Add symbols to `symbols` array or change `mode` to `"all"`

### Issue: Workers not spawning

**Cause**: Stream count calculation error or rate limit

**Solution**: Check logs for rate limiter messages, verify stream counts

### Issue: High S3 costs

**Cause**: Too many small batches

**Solution**: Increase `batch_size` or `batch_timeout_ms`

### Issue: High latency

**Cause**: Batches waiting too long

**Solution**: Decrease `batch_timeout_ms` or `batch_size`

### Issue: Memory usage too high

**Cause**: Too many in-flight messages/batches

**Solution**: Decrease channel buffer sizes (requires code change)
