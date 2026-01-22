# Tuning `partitions_per_worker` Parameter

## Quick Reference

```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 75  # Default, adjust based on your workload
```

## What It Does

Controls the ratio of partitions to workers when using auto-scaling:

```
workers = max(2, min(16, total_partitions / partitions_per_worker))
```

## Recommended Values

| Workload | Value | Workers (per 300 partitions) | Use Case |
|----------|-------|------------------------------|----------|
| **High Volume** | 50 | 6 workers | High msg rate, small batches, slow S3 |
| **Balanced** | 75 | 4 workers | Default, works for most cases |
| **Low Volume** | 100 | 3 workers | Low msg rate, large batches, fast S3 |
| **Very Low** | 150 | 2 workers | Minimal traffic, testing |

## When to Lower (More Workers)

Use **50** if you have:
- ✓ High message volume (> 1000 msg/s)
- ✓ Small `batch_size` (< 500)
- ✓ Slow S3 or high network latency
- ✓ Complex compression (level > 5)
- ✓ Many small batches being created frequently

**Example:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 50  # More aggressive
batch_size = 500
batch_timeout_ms = 5000
```

## When to Raise (Fewer Workers)

Use **100-150** if you have:
- ✓ Low message volume (< 100 msg/s)
- ✓ Large `batch_size` (> 2000)
- ✓ Fast S3 (local MinIO, good network)
- ✓ Fast compression (level 1-3)
- ✓ Batches created infrequently

**Example:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 100  # More conservative
batch_size = 2000
batch_timeout_ms = 10000
```

## Monitoring & Tuning

### Step 1: Start with Default
```toml
[s3]
upload_worker_count = "auto"
# partitions_per_worker defaults to 75
```

### Step 2: Monitor These Metrics

After running for a while, check:

1. **`batch_active`** (active batches waiting for upload)
   - **High (> 100)**: Lower `partitions_per_worker` → more workers
   - **Low (< 10)**: Raise `partitions_per_worker` → fewer workers

2. **S3 upload duration** (p99 latency)
   - **High (> 2s)**: Lower `partitions_per_worker` → more workers
   - **Low (< 0.5s)**: Workers may be underutilized

3. **Batch creation rate**
   - **High (> 100/s)**: Lower `partitions_per_worker` → more workers
   - **Low (< 10/s)**: Current setting likely fine

### Step 3: Adjust Based on Observations

```toml
# If batches pile up:
partitions_per_worker = 50  # was 75

# If workers mostly idle:
partitions_per_worker = 100  # was 75
```

### Step 4: Re-monitor

After adjustment, run for 30+ minutes and re-check metrics.

## Real Examples

### Example 1: Binance Spot (5 symbols, all streams)

**Config:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 75  # Default

[[exchanges]]
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "1h"]
```

**Calculation:**
- Partitions: 5 symbols × (1 trade + 1 depth + 3 klines) = 25
- Workers: max(2, min(16, 25/75)) = **2 workers** (min bound)

### Example 2: Multi-Exchange (300 partitions)

**Config:**
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 75
```

**Calculation:**
- Partitions: 300 (across all exchanges)
- Workers: max(2, min(16, 300/75)) = **4 workers**

### Example 3: High-Volume Tuning

**Before (slow uploads):**
```toml
partitions_per_worker = 75  # 4 workers for 300 partitions
```
- Observation: `batch_active` consistently > 150
- Problem: Not enough workers

**After:**
```toml
partitions_per_worker = 50  # 6 workers for 300 partitions
```
- Result: `batch_active` now < 50, uploads keep up

## Advanced: Changing Min/Max Bounds

The worker count is bounded by `min=2` and `max=16`. These are hardcoded in `config.rs`.

If you need different bounds, modify:

```rust
// In config.rs WorkerCount::resolve()
calculated.max(2).min(16)  // Change these values
```

**Why bounds exist:**
- **Min (2)**: Even tiny deployments benefit from some parallelism
- **Max (16)**: Diminishing returns beyond this for typical workloads

## FAQ

**Q: What if I have very high partition count (> 1200)?**  
A: The max of 16 workers may be limiting. Consider:
- Using manual `upload_worker_count = 24`
- Or modify max bound in code

**Q: Can I set different ratios per exchange?**  
A: No, it's global. Use manual worker count if you need extreme control.

**Q: What's the cost of having too many workers?**  
A: Minimal - each idle worker uses ~1-2 MB RAM. Better to have slightly too many.

**Q: What happens if I set it very low (e.g., 10)?**  
A: You'll get many workers (up to max of 16). Good for very high-volume deployments.

## Summary

| You Want | Set This |
|----------|----------|
| Default balanced behavior | Omit parameter (uses 75) |
| More workers for high volume | `partitions_per_worker = 50` |
| Fewer workers for low volume | `partitions_per_worker = 100` |
| Precise control | `upload_worker_count = 8` (manual) |

**Remember:** Start with default, monitor, then tune!
