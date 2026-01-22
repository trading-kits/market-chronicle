# Upload Worker Auto-Scaling Implementation

## Summary

Implemented dynamic computation of `upload_worker_count` following the same pattern as WebSocket workers. The system can now automatically determine the optimal number of S3 upload workers based on expected partition count.

## Changes

### Core Configuration (`crates/market-chronicle-core/src/config.rs`)

1. **New `WorkerCount` enum**: Supports both manual count and auto-computation
   ```rust
   pub enum WorkerCount {
       Manual(usize),    // Explicit number like 4
       Auto(String),     // "auto" string for auto-compute
   }
   ```

2. **New `partitions_per_worker` config parameter**: Configurable tuning ratio
   - Default: 75 (1 worker per 75 partitions)
   - Range: 1-1000
   - Only applies when `upload_worker_count = "auto"`
   - Allows users to tune without code changes

3. **Auto-computation logic**: 
   - Formula: `max(2, min(16, partitions / partitions_per_worker))`
   - Min 2 workers (for low volume)
   - Max 16 workers (diminishing returns beyond this)
   - Configurable ratio: Default 1 worker per 75 active partitions

3. **Partition estimation**: 
   - Added `estimate_partition_count()` to `Config` and `ExchangeConfig`
   - Computes expected partitions based on:
     - Symbol count
     - Stream types (trade, depth, kline, etc.)
     - Kline intervals
     - Special handling for markPrice (global, not per-symbol)

### Main Binary (`crates/market-chronicle/src/main.rs`)

- Resolves worker count at startup
- Logs computed values for observability:
  ```
  worker_count: 4
  estimated_partitions: 285
  mode: Auto
  ```

### Configuration Files

Updated all config files to use `"auto"` by default:
- `config.example.toml`
- `config.local.toml`
- `config.docker.toml`

### Documentation

1. **Updated existing docs**:
   - `docs/configuration-guide.md` - Explained auto vs manual options
   - `docs/data-flow.md` - Updated tuning examples
   - `README.md` - Updated quickstart and examples

2. **New documentation**:
   - `docs/auto-worker-scaling.md` - Comprehensive guide covering:
     - Rationale and design
     - Partition calculation
     - Worker computation formula
     - Examples for different deployment sizes
     - Migration guide
     - Monitoring and tuning

### Tests

Added comprehensive tests in `crates/market-chronicle-core/tests/config_test.rs`:
- `test_worker_count_auto()` - Validates auto-computation
- `test_worker_count_manual()` - Validates manual override
- `test_partition_estimation()` - Tests partition counting
- `test_partition_estimation_with_markprice()` - Tests special markPrice handling

All tests passing ✓

## Usage

### Option 1: Auto with Default Ratio (Recommended)
```toml
[s3]
upload_worker_count = "auto"
# partitions_per_worker = 75  # Default, optional
```

### Option 2: Auto with Custom Ratio
```toml
[s3]
upload_worker_count = "auto"
partitions_per_worker = 50  # More workers for high-volume workloads
```

### Option 3: Manual
```toml
[s3]
upload_worker_count = 8
# partitions_per_worker is ignored with manual count
```

## Examples

### Small deployment (5 symbols, 2 streams)
- Partitions: 10
- Workers: 2 (minimum)

### Medium deployment (50 symbols, 4 streams including klines)
- Partitions: 300
- Workers: 4

### Large deployment (2500 symbols, all streams)
- Partitions: 10,000
- Workers: 16 (maximum)

## Benefits

1. **Consistency**: Same pattern as WebSocket worker auto-scaling
2. **Simplicity**: No manual tuning needed for most deployments
3. **Optimal**: Automatically scales based on workload
4. **Configurable**: Fine-tune ratio via config without code changes
5. **Safe**: Min/max bounds prevent under/over-provisioning
6. **Observable**: Logs computed values at startup

## Backward Compatibility

✅ Fully backward compatible
- Existing configs with numeric values continue to work
- Default is now "auto" for new configs
- No breaking changes to API or behavior

## Testing

```bash
# Run unit tests
cargo test --package market-chronicle-core --test config_test

# Build and verify
cargo build --package market-chronicle

# All tests pass, compilation successful
```

## Pattern Consistency

This implementation follows the established pattern:

| Component | Dynamic Computation | Formula |
|-----------|-------------------|---------|
| WebSocket Workers | ✓ | `ceil(streams / streams_per_conn)` |
| S3 Upload Workers | ✓ | `max(2, min(16, partitions / 75))` |

Both now compute worker counts based on workload characteristics at runtime.
