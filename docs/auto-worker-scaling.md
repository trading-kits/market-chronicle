# Auto Worker Scaling

Market Chronicle supports automatic computation of S3 upload worker count, following the same pattern used for WebSocket workers.

## Overview

Similar to how WebSocket workers are dynamically computed based on stream count, S3 upload workers can now be automatically computed based on expected partition count.

### Worker Types

1. **WebSocket Workers**: Automatically computed as `ceil(total_streams / ws_streams_per_connection)`
2. **S3 Upload Workers**: Can now be auto-computed as `max(2, min(16, partitions / 75))`

## Partition-Based Calculation

### What is a Partition?

Each unique combination of these fields creates a separate partition:
- Exchange (e.g., "binance")
- Market Type (e.g., "spot", "perpetual")
- Stream Type (e.g., "trade", "depth", "kline")
- Symbol (e.g., "BTCUSDT")
- Hour (rounded timestamp)

### How Partitions Drive Workload

The BatchAggregator creates separate batches for each partition. Upload workers process these batches in round-robin fashion. More partitions = more concurrent batches = need more workers.

### Partition Estimation

The system estimates partitions at startup:

```rust
// For each exchange:
partitions = 0
for stream in streams:
    if stream == "kline":
        partitions += symbol_count × kline_intervals.len()
    elif stream == "markPrice":
        partitions += 1  // global, not per-symbol
    else:  // trade, depth, aggTrade, etc.
        partitions += symbol_count

total_partitions = sum across all exchanges
```

### Worker Calculation

```rust
if upload_worker_count == "auto":
    workers = (total_partitions + partitions_per_worker - 1) / partitions_per_worker  // ceiling division
    workers = max(2, min(16, workers))
```

**Heuristic**: 1 worker per ~`partitions_per_worker` partitions
- Default ratio: 75 partitions per worker
- Min: 2 workers (for low volume)
- Max: 16 workers (diminishing returns beyond this)
- **Configurable**: Adjust via `partitions_per_worker` parameter

## Configuration

### Option 1: Auto (Recommended)

```toml
[s3]
upload_worker_count = "auto"
# partitions_per_worker = 75  # Optional, defaults to 75
```

The system will:
1. Count total partitions across all exchanges
2. Compute worker count using formula above
3. Log the decision at startup

### Option 2: Auto with Custom Ratio

```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 50  # More workers for high-volume workloads
```

Use this when:
- You have high message volume
- Small batch sizes
- Slow S3 or high network latency

### Option 3: Manual

```toml
[s3]
upload_worker_count = 8
# partitions_per_worker is ignored with manual count
```

Use manual count when:
- You need precise control
- Testing specific scenarios
- You know your exact workload characteristics

## Examples

### Example 1: Small Deployment

**Config:**
```toml
[[exchanges]]
name = "binance"
market_type = "spot"
symbols = ["BTCUSDT", "ETHUSDT"]  # 2 symbols
streams = ["trade", "depth"]       # 2 streams per symbol
kline_intervals = []
```

**Calculation:**
- Partitions: 2 symbols × 2 streams = 4
- Workers: max(2, min(16, 4/75)) = 2 workers

### Example 2: Medium Deployment

**Config:**
```toml
[[exchanges]]
name = "binance"
market_type = "spot"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # 3 symbols
streams = ["trade", "depth", "kline"]         # 2 + klines
kline_intervals = ["1m", "5m", "1h"]          # 3 intervals

[[exchanges]]
name = "binance"
market_type = "perpetual"
symbols = ["BTCUSDT", "ETHUSDT"]              # 2 symbols
streams = ["aggTrade", "markPrice"]           # 1 per symbol + 1 global
```

**Calculation:**
- Exchange 1: 3 symbols × (1 trade + 1 depth + 3 klines) = 15
- Exchange 2: 2 symbols × 1 aggTrade + 1 markPrice = 3
- Total partitions: 18
- Workers: max(2, min(16, 18/75)) = 2 workers

### Example 3: Large Deployment

**Config:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 75  # Default

[[exchanges]]
name = "binance"
market_type = "spot"
mode = "all"                                  # All symbols (~2500+)
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m"]
```

**Calculation:**
- Partitions: 2500 symbols × (1 trade + 1 depth + 2 klines) = 10,000
- Workers: max(2, min(16, 10000/75)) = 16 workers (capped at max)

### Example 4: High-Volume Tuning

**Scenario:** Large deployment with slow S3, need more workers

**Config:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 50  # Lower ratio = more workers

[[exchanges]]
name = "binance"
market_type = "spot"
mode = "all"
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m"]
```

**Calculation:**
- Partitions: 2500 symbols × 4 streams = 10,000
- Workers: max(2, min(16, 10000/50)) = 16 workers
- Result: Still capped at 16, but would use more if max was higher

## Benefits

1. **Consistency**: Same pattern as WebSocket worker auto-scaling
2. **Simplicity**: No need to manually tune for different deployments
3. **Optimal**: Automatically scales based on actual workload
4. **Safe**: Min/max bounds prevent under/over-provisioning

## Monitoring

At startup, the system logs:

```
Starting S3 upload workers
  worker_count: 4
  estimated_partitions: 285
  mode: Auto
```

Use this information to:
- Verify auto-computation is working
- Understand your partition distribution
- Decide if manual tuning is needed

## Migration Guide

### From Fixed Count

**Before:**
```toml
upload_worker_count = 4
```

**After:**
```toml
upload_worker_count = "auto"  # Will compute appropriate count
```

### Testing

1. Update config to use "auto"
2. Start the service
3. Check startup logs for computed worker_count and estimated_partitions
4. Verify it matches your expectations
5. If not, either:
   - Use manual count for precise control
   - File an issue if the heuristic seems off

## Advanced: Tuning the Heuristic

### Why 75?

The current heuristic (1 worker per 75 partitions) is a conservative estimate based on:
- **Batch creation rate**: 1-10 batches/sec per partition (varies by message volume and `batch_size`)
- **S3 upload time**: 0.1-1 second per batch (varies by size, compression, network)
- **Worker queue depth**: 100 batches (buffer for bursts)
- **Safety margin**: Better to have slightly more workers than needed

**This is somewhat arbitrary** and may not be optimal for your workload.

### When to Adjust

Monitor these metrics to determine if you need different ratios:

1. **Queue depth growing** → Need more workers
   - Check `batch_active` metric
   - If consistently > 50-100, increase workers

2. **Workers underutilized** → Can reduce workers
   - Check S3 upload rates per worker
   - If workers idle most of the time, reduce

3. **High latency** → Need more workers
   - Check time between batch creation and upload
   - If batches wait > 5-10 seconds, increase workers

### How to Adjust

**Option 1: Adjust partitions_per_worker** (easiest, recommended)
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 50  # More workers (was 75)
```

**Option 2: Use manual count** (for precise control)
```toml
[s3]
upload_worker_count = 8  # Based on your metrics
```

**Common Tuning Values:**
```toml
# High volume deployment
partitions_per_worker = 50   # ~2x workers

# Default balanced
partitions_per_worker = 75   # Default

# Low volume deployment  
partitions_per_worker = 100  # ~0.75x workers
```

### Real-World Examples

From our testing:

- **Low volume** (< 100 msg/s, 10 partitions): 75 ratio → 2 workers ✓ (min bound applies)
- **Medium volume** (500 msg/s, 300 partitions): 75 ratio → 4 workers ✓ (good balance)
- **High volume** (5000 msg/s, 10000 partitions): 75 ratio → 16 workers ✓ (max bound applies)

The 75 ratio works well across these ranges because the min/max bounds (2-16) provide guardrails.

## See Also

- [Configuration Guide](./configuration-guide.md) - Full parameter reference
- [Data Flow](./data-flow.md) - How batches flow through the system
- [Graceful Shutdown](./graceful-shutdown.md) - Worker lifecycle management
