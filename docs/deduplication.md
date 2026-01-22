# Data Deduplication Algorithms

## Overview

When running Market Chronicle in a geo-distributed deployment with multiple instances collecting the same data, **duplicate messages are inevitable**. This document provides detailed algorithms and strategies for efficiently deduplicating market data.

## Why Duplicates Occur

### 1. Active-Active Redundancy

Multiple instances subscribe to the same streams for high availability:

```
Instance 1 → BTCUSDT@trade → S3 (instance-1/)
Instance 2 → BTCUSDT@trade → S3 (instance-2/)
Instance 3 → BTCUSDT@trade → S3 (instance-3/)
```

**Result**: 3x the same trade messages

### 2. Timestamp Collisions

Even single instances can produce duplicates:
- Exchange sends same message multiple times
- WebSocket reconnection causes replay
- Exchange timestamp granularity (milliseconds) → multiple trades per ms

### 3. Clock Skew

Different instances have slightly different system clocks:
- Instance 1: Records trade at 10:00:00.100
- Instance 2: Records same trade at 10:00:00.098
- **Same trade, different timestamps**

## Duplicate Identification

### Primary Key Selection

For market data, the **natural composite key** is:

```rust
struct MessageKey {
    exchange: String,      // "binance"
    market_type: String,   // "spot" or "perpetual"
    stream_type: String,   // "trade", "depth", "kline"
    symbol: String,        // "BTCUSDT"
    timestamp: i64,        // Exchange timestamp (NOT local receive time)
    
    // Stream-specific fields:
    trade_id: Option<i64>,     // For trades
    update_id: Option<i64>,    // For orderbook
    interval: Option<String>,  // For klines
}
```

### Key Selection by Stream Type

#### Trade Streams

```rust
// Unique key
(exchange, symbol, trade_id)

// Binance example
{
  "e": "trade",
  "s": "BTCUSDT",
  "t": 12345,  // trade_id - GLOBALLY UNIQUE
  "T": 1610000000000  // timestamp
}
```

**Dedup Strategy**: Use `trade_id` - guaranteed unique per symbol by exchange.

#### Depth/OrderBook Streams

```rust
// Unique key
(exchange, symbol, update_id)

// Binance example
{
  "e": "depthUpdate",
  "s": "BTCUSDT",
  "u": 157,  // update_id - MONOTONIC
  "U": 156,  // first_update_id
}
```

**Dedup Strategy**: Use `update_id` - monotonically increasing per symbol.

#### Kline/Candlestick Streams

```rust
// Unique key
(exchange, symbol, interval, open_time)

// Binance example
{
  "e": "kline",
  "s": "BTCUSDT",
  "k": {
    "i": "1m",        // interval
    "t": 1610000000000,  // open_time
    "c": false        // is_closed
  }
}
```

**Dedup Strategy**: Keep latest update per `(symbol, interval, open_time)`.

**Note**: Klines are updated multiple times before closing - keep final closed version.

#### Aggregated Trade Streams

```rust
// Unique key
(exchange, symbol, agg_trade_id)

// Similar to trades, use agg_trade_id
```

## Deduplication Algorithms

### Algorithm 1: Hash-Based Deduplication (In-Memory)

**Use Case**: Real-time streaming pipeline, memory-constrained environments

**Approach**: Maintain a rolling window of seen message hashes.

```rust
use std::collections::HashSet;
use std::collections::VecDeque;

struct HashDeduplicator {
    seen_hashes: HashSet<u64>,
    hash_queue: VecDeque<u64>,
    window_size: usize,
}

impl HashDeduplicator {
    fn new(window_size: usize) -> Self {
        Self {
            seen_hashes: HashSet::with_capacity(window_size),
            hash_queue: VecDeque::with_capacity(window_size),
            window_size,
        }
    }
    
    fn is_duplicate(&mut self, message: &Message) -> bool {
        let hash = message.compute_hash();
        
        if self.seen_hashes.contains(&hash) {
            return true;  // Duplicate
        }
        
        // Add to seen set
        self.seen_hashes.insert(hash);
        self.hash_queue.push_back(hash);
        
        // Evict old entries
        if self.hash_queue.len() > self.window_size {
            if let Some(old_hash) = self.hash_queue.pop_front() {
                self.seen_hashes.remove(&old_hash);
            }
        }
        
        false  // Not duplicate
    }
}

impl Message {
    fn compute_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        self.exchange.hash(&mut hasher);
        self.symbol.hash(&mut hasher);
        self.stream_type.hash(&mut hasher);
        
        // Use exchange timestamp, NOT local receive time
        self.exchange_timestamp.hash(&mut hasher);
        
        // Include stream-specific IDs
        self.trade_id.hash(&mut hasher);
        self.update_id.hash(&mut hasher);
        
        hasher.finish()
    }
}
```

**Pros**:
- Fast: O(1) lookup
- Memory efficient with fixed window
- Simple implementation

**Cons**:
- Window size limits detection range
- Hash collisions (rare but possible)
- Doesn't work across restarts

**Configuration**:
```toml
[deduplication]
algorithm = "hash"
window_size = 100000  # Keep last 100k messages
```

### Algorithm 2: ID-Based Deduplication (Exact)

**Use Case**: When exchange provides unique IDs (trade_id, update_id)

**Approach**: Track highest seen ID per symbol, reject older IDs.

```rust
use std::collections::HashMap;

struct IdDeduplicator {
    // Map: (exchange, symbol, stream_type) -> highest_id
    highest_ids: HashMap<(String, String, String), i64>,
}

impl IdDeduplicator {
    fn new() -> Self {
        Self {
            highest_ids: HashMap::new(),
        }
    }
    
    fn is_duplicate(&mut self, message: &Message) -> bool {
        // Only works for messages with IDs
        let id = match message.get_unique_id() {
            Some(id) => id,
            None => return false,  // Can't deduplicate without ID
        };
        
        let key = (
            message.exchange.clone(),
            message.symbol.clone(),
            message.stream_type.clone(),
        );
        
        let highest = self.highest_ids.entry(key).or_insert(0);
        
        if id <= *highest {
            return true;  // Duplicate or out-of-order
        }
        
        *highest = id;
        false
    }
}

impl Message {
    fn get_unique_id(&self) -> Option<i64> {
        match self.stream_type.as_str() {
            "trade" | "aggTrade" => self.trade_id,
            "depth" | "depthUpdate" => self.update_id,
            _ => None,
        }
    }
}
```

**Pros**:
- Perfect accuracy (no false positives)
- Memory efficient (one i64 per symbol)
- Handles restarts (track from file)

**Cons**:
- Only works with exchanges that provide IDs
- Assumes monotonic IDs (usually true)
- Doesn't handle out-of-order messages well

**Configuration**:
```toml
[deduplication]
algorithm = "id"
state_file = "/var/lib/market-chronicle/dedup_state.json"
```

### Algorithm 3: Bloom Filter (Probabilistic)

**Use Case**: Large-scale deduplication with minimal memory

**Approach**: Use Bloom filter for approximate duplicate detection.

```rust
use bloomfilter::Bloom;

struct BloomDeduplicator {
    bloom: Bloom<Vec<u8>>,
}

impl BloomDeduplicator {
    fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        Self {
            bloom: Bloom::new_for_fp_rate(expected_items, false_positive_rate),
        }
    }
    
    fn is_duplicate(&mut self, message: &Message) -> bool {
        let key = message.to_bloom_key();
        
        if self.bloom.check(&key) {
            return true;  // Probably duplicate
        }
        
        self.bloom.set(&key);
        false  // Definitely not seen before
    }
}

impl Message {
    fn to_bloom_key(&self) -> Vec<u8> {
        // Serialize to bytes
        format!(
            "{}:{}:{}:{}:{}",
            self.exchange,
            self.symbol,
            self.stream_type,
            self.exchange_timestamp,
            self.trade_id.unwrap_or(0)
        ).into_bytes()
    }
}
```

**Pros**:
- Very memory efficient (bits per item)
- Fast: O(k) where k = hash functions
- Good for append-only streams

**Cons**:
- False positives possible (configurable rate)
- No false negatives
- Cannot remove items

**Configuration**:
```toml
[deduplication]
algorithm = "bloom"
expected_items = 10_000_000
false_positive_rate = 0.001  # 0.1%
```

**Memory Usage Example**:
- 10M items, 0.1% FP rate: ~14 MB
- 100M items, 0.1% FP rate: ~143 MB

### Algorithm 4: Time-Window Exact Deduplication

**Use Case**: Query-time deduplication across multiple S3 prefixes

**Approach**: Use SQL window functions to deduplicate in queries.

```sql
-- Method 1: ROW_NUMBER()
WITH ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        exchange,
        symbol,
        stream_type,
        COALESCE(trade_id, update_id, 0),  -- Use ID if available
        exchange_timestamp
      ORDER BY 
        s3_prefix,  -- Prefer instance-1 over instance-2
        local_timestamp  -- Then earliest received
    ) as rn
  FROM market_data
  WHERE dt >= '2024-01-01'
    AND exchange = 'binance'
    AND symbol = 'BTCUSDT'
)
SELECT * FROM ranked WHERE rn = 1;

-- Method 2: DISTINCT ON (PostgreSQL)
SELECT DISTINCT ON (
  exchange,
  symbol,
  trade_id,
  exchange_timestamp
)
  *
FROM market_data
WHERE dt >= '2024-01-01'
  AND exchange = 'binance'
  AND symbol = 'BTCUSDT'
ORDER BY
  exchange,
  symbol,
  trade_id,
  exchange_timestamp,
  s3_prefix;  -- Tie-breaker
```

**Pros**:
- Perfect accuracy
- No additional infrastructure
- Flexible (change strategy per query)

**Cons**:
- Slower queries (must scan duplicates)
- Higher S3 costs (store duplicates)
- No reduction in storage

### Algorithm 5: Batch Merge Deduplication

**Use Case**: Periodic deduplication job to reduce storage costs

**Approach**: Read all instances' data for a time range, deduplicate, rewrite.

#### Spark/PySpark Implementation

```python
# Spark batch dedup job
def deduplicate_daily_spark(date: str):
    # 1. Read data from all instances
    df = spark.read.parquet([
        f"s3://bucket/instance-1/dt={date}",
        f"s3://bucket/instance-2/dt={date}",
        f"s3://bucket/instance-3/dt={date}",
    ])
    
    # 2. Add instance preference
    df = df.withColumn("instance_rank", 
        when(col("s3_prefix") == "instance-1", 1)
        .when(col("s3_prefix") == "instance-2", 2)
        .otherwise(3)
    )
    
    # 3. Deduplicate by unique key
    dedup_key = ["exchange", "symbol", "stream_type", 
                 "exchange_timestamp", "trade_id"]
    
    df_dedup = (df
        .withColumn("rn", row_number().over(
            Window
            .partitionBy(dedup_key)
            .orderBy("instance_rank", "local_timestamp")
        ))
        .filter(col("rn") == 1)
        .drop("rn", "instance_rank")
    )
    
    # 4. Write to deduplicated location
    df_dedup.write.parquet(f"s3://bucket/deduplicated/dt={date}")
    
    # 5. (Optional) Delete source data
    # delete_s3_prefix(f"instance-*/dt={date}")
```

#### Dask Implementation

```python
import dask.dataframe as dd
import pandas as pd

def deduplicate_daily_dask(date: str):
    """
    Dask-based deduplication - better for memory-constrained environments
    """
    # 1. Read data from all instances
    # Read with dask for lazy evaluation
    ddf = dd.read_parquet(
        [
            f"s3://bucket/instance-1/dt={date}/*.parquet",
            f"s3://bucket/instance-2/dt={date}/*.parquet",
            f"s3://bucket/instance-3/dt={date}/*.parquet",
        ],
        engine='pyarrow',
        # Specify columns if you want to optimize memory
        # columns=['exchange', 'symbol', 'stream_type', ...]
    )
    
    # 2. Add instance preference
    def assign_instance_rank(df):
        """Assign rank based on S3 prefix"""
        conditions = [
            (df['s3_prefix'] == 'instance-1', 1),
            (df['s3_prefix'] == 'instance-2', 2),
        ]
        df['instance_rank'] = 3  # Default
        for condition, rank in conditions:
            df.loc[condition, 'instance_rank'] = rank
        return df
    
    ddf = ddf.map_partitions(assign_instance_rank, meta=ddf)
    
    # 3. Deduplicate by unique key
    # Dask doesn't support window functions, so we use groupby + sort
    dedup_key = ['exchange', 'symbol', 'stream_type', 
                 'exchange_timestamp', 'trade_id']
    
    # Sort within each group to prioritize by instance_rank, then local_timestamp
    ddf = ddf.sort_values(['instance_rank', 'local_timestamp'])
    
    # Method A: Using drop_duplicates (simpler, but less control)
    ddf_dedup = ddf.drop_duplicates(
        subset=dedup_key,
        keep='first'  # Keeps first occurrence (lowest instance_rank)
    )
    
    # Method B: Using groupby (more control, useful for additional aggregations)
    # ddf_dedup = ddf.groupby(dedup_key, dropna=False).first().reset_index()
    
    # 4. Drop helper columns
    ddf_dedup = ddf_dedup.drop(columns=['instance_rank'])
    
    # 5. Write to deduplicated location
    ddf_dedup.to_parquet(
        f"s3://bucket/deduplicated/dt={date}",
        engine='pyarrow',
        compression='zstd',  # or 'snappy' for faster writes
        # Optimize partition size for your use case
        partition_size='128MB'
    )
    
    # 6. (Optional) Delete source data
    # import boto3
    # s3 = boto3.client('s3')
    # for instance in ['instance-1', 'instance-2', 'instance-3']:
    #     delete_s3_prefix(s3, 'bucket', f"{instance}/dt={date}")

# Alternative: More memory-efficient for very large datasets
def deduplicate_daily_dask_streaming(date: str):
    """
    Memory-efficient deduplication using Dask distributed scheduler.
    
    This approach is optimized for:
    - Large datasets that don't fit in memory
    - Multi-node processing (can run on a cluster)
    - Fine-grained control over memory usage
    
    Args:
        date: Date string in format 'YYYY-MM-DD' (e.g., '2024-01-15')
    
    Memory Strategy:
    - Uses small partition sizes (64MB) to keep memory usage low
    - Processes data in parallel across workers
    - Each worker has a 4GB memory limit
    - Total memory: n_workers × memory_limit (16GB in this config)
    """
    import dask
    from dask.distributed import Client
    
    # ============================================================================
    # STEP 1: Initialize Dask Distributed Scheduler
    # ============================================================================
    # The distributed scheduler provides:
    # - Parallel execution across multiple workers
    # - Better memory management than single-threaded scheduler
    # - Real-time dashboard at http://localhost:8787
    # - Automatic task scheduling and load balancing
    #
    # Configuration:
    # - n_workers=4: Creates 4 parallel worker processes
    # - threads_per_worker=2: Each worker uses 2 threads (8 total threads)
    # - memory_limit='4GB': Each worker can use up to 4GB RAM
    #
    # For production, consider:
    # - More workers: n_workers=8 or n_workers=16 for faster processing
    # - Less memory per worker: memory_limit='2GB' on memory-constrained nodes
    # - More threads: threads_per_worker=4 for CPU-bound operations
    client = Client(n_workers=4, threads_per_worker=2, memory_limit='4GB')
    
    try:
        # ========================================================================
        # STEP 2: Read Parquet Files from S3
        # ========================================================================
        # Read data from all 3 instances (geo-distributed deployments)
        # Each instance collected the same data, resulting in ~3x duplication
        #
        # Key parameters:
        # - engine='pyarrow': Uses Apache Arrow for fast columnar I/O
        # - blocksize='64MB': Splits files into 64MB partitions
        #   * Smaller than default (256MB) for better memory efficiency
        #   * More partitions = more parallelism but more overhead
        #   * 64MB is a good balance for 4GB worker memory limit
        #
        # Lazy evaluation: No data is actually read yet, just the metadata
        ddf = dd.read_parquet(
            [
                f"s3://bucket/instance-1/dt={date}/*.parquet",
                f"s3://bucket/instance-2/dt={date}/*.parquet",
                f"s3://bucket/instance-3/dt={date}/*.parquet",
            ],
            engine='pyarrow',
            blocksize='64MB',  # Smaller blocks for memory efficiency
        )
        
        # ========================================================================
        # STEP 3: Add Instance Priority Ranking
        # ========================================================================
        # When multiple instances have the same message, we need to pick one.
        # Priority order: instance-1 (rank=1) > instance-2 (rank=2) > instance-3 (rank=3)
        #
        # Why this order?
        # - instance-1 is typically closest to exchange (lowest latency)
        # - instance-1 has most accurate timestamps
        # - Consistent tie-breaking across all deduplication runs
        #
        # The map() function applies the dictionary mapping to each row
        # meta= specifies the output column type for Dask's type inference
        ddf['instance_rank'] = ddf['s3_prefix'].map({
            'instance-1': 1,  # Highest priority (prefer this data)
            'instance-2': 2,  # Medium priority
            'instance-3': 3   # Lowest priority (use only if others missing)
        }, meta=('instance_rank', 'int64'))
        
        # ========================================================================
        # STEP 4: Define Deduplication Key
        # ========================================================================
        # The composite key that uniquely identifies a market data message
        # If two rows have the same dedup_key, they are duplicates
        #
        # Components:
        # - exchange: "binance", "bybit", etc. (different exchanges, different messages)
        # - symbol: "BTCUSDT", "ETHUSDT", etc. (different symbols, different messages)
        # - stream_type: "trade", "depth", "kline", etc. (different data types)
        # - exchange_timestamp: Unix timestamp from exchange (NOT local receive time!)
        # - trade_id: Unique ID provided by exchange (for trades/aggTrades)
        #
        # Important: This key MUST match your data model and exchange semantics
        # For orderbook updates, replace trade_id with update_id
        # For klines, add 'interval' field and use 'open_time' instead of trade_id
        dedup_key = ['exchange', 'symbol', 'stream_type', 
                     'exchange_timestamp', 'trade_id']
        
        # ========================================================================
        # STEP 5: Set Index for Efficient Grouping
        # ========================================================================
        # Setting the dedup_key as index provides several benefits:
        # 1. Dask can co-locate rows with same key in same partition
        # 2. Enables efficient groupby operations within partitions
        # 3. Reduces shuffling between workers
        #
        # sorted=False: We don't need globally sorted data (faster)
        # 
        # Trade-off:
        # - This operation may cause data shuffling between workers (network I/O)
        # - But it makes subsequent groupby much faster
        # - Overall: Worth it for deduplication workload
        ddf = ddf.set_index(dedup_key, sorted=False)
        
        # ========================================================================
        # STEP 6: Define Per-Partition Deduplication Function
        # ========================================================================
        # This function is applied to each partition independently (parallel)
        # Each worker processes its assigned partitions without coordination
        #
        # Deduplication logic:
        # 1. Sort by instance_rank (prefer instance-1)
        # 2. If tie, sort by local_timestamp (prefer earlier received)
        # 3. Keep only the first row per dedup_key (highest priority instance)
        #
        # Why per-partition?
        # - Avoids global shuffle (no network I/O during dedup)
        # - Each partition fits in worker memory (4GB limit)
        # - Fully parallel (N partitions = N parallel dedup operations)
        def dedup_partition(df):
            """
            Deduplicate a single partition of data.
            
            Args:
                df: Pandas DataFrame (one partition, ~64MB)
                
            Returns:
                Deduplicated DataFrame with one row per unique dedup_key
                
            Performance:
            - Runs in-memory on single worker
            - O(n log n) due to sorting (n = rows in partition)
            - Typical partition: 50k-200k rows, <1 second processing
            """
            # Handle edge case: empty partition (can happen with uneven data distribution)
            if df.empty:
                return df
            
            # Sort by priority:
            # 1. instance_rank: Lower is better (1 < 2 < 3)
            #    - Ensures instance-1 data comes first
            # 2. local_timestamp: Earlier is better
            #    - Tie-breaker when same instance has duplicate (rare but possible)
            #    - Ensures deterministic results across runs
            df = df.sort_values(['instance_rank', 'local_timestamp'])
            
            # Group by the index (which is the dedup_key) and keep first row
            # level=list(range(len(dedup_key))): Groups by all index levels
            # .first(): Takes the first row in each group (highest priority after sort)
            #
            # Example:
            # Before (2 duplicates):
            #   (binance, BTCUSDT, trade, 1610000000000, 12345) -> instance-1, rank=1
            #   (binance, BTCUSDT, trade, 1610000000000, 12345) -> instance-2, rank=2
            # After:
            #   (binance, BTCUSDT, trade, 1610000000000, 12345) -> instance-1, rank=1
            return df.groupby(level=list(range(len(dedup_key)))).first()
        
        # ========================================================================
        # STEP 7: Apply Deduplication Across All Partitions
        # ========================================================================
        # map_partitions() applies dedup_partition() to each partition in parallel
        # - Each worker processes its partitions independently
        # - No data exchange between workers during this step
        # - meta=ddf tells Dask the output schema matches input schema
        ddf_dedup = ddf.map_partitions(dedup_partition, meta=ddf)
        
        # ========================================================================
        # STEP 8: Restore Original DataFrame Structure
        # ========================================================================
        # reset_index(): Move dedup_key from index back to regular columns
        # This restores the original schema for downstream processing
        ddf_dedup = ddf_dedup.reset_index()
        
        # drop(): Remove the helper column we added
        # instance_rank is no longer needed after deduplication
        ddf_dedup = ddf_dedup.drop(columns=['instance_rank'])
        
        # ========================================================================
        # STEP 9: Write Deduplicated Data to S3
        # ========================================================================
        # Write results back to S3 in Parquet format
        #
        # Parameters:
        # - engine='pyarrow': Fast, efficient Parquet writer
        # - compression='zstd': 
        #   * Better compression ratio than snappy (~20-30% smaller files)
        #   * Slightly slower but worth it for S3 storage costs
        #   * Alternative: 'snappy' for faster writes, larger files
        # - write_metadata_file=True:
        #   * Creates _metadata file with partition information
        #   * Enables faster queries (don't need to scan all files)
        #   * Recommended for production datasets
        #
        # Trigger computation: This line actually executes all the lazy operations
        # Progress: Watch the Dask dashboard at http://localhost:8787 for real-time progress
        ddf_dedup.to_parquet(
            f"s3://bucket/deduplicated/dt={date}",
            engine='pyarrow',
            compression='zstd',
            write_metadata_file=True
        )
        
        # ========================================================================
        # STEP 10: Log Completion
        # ========================================================================
        print(f"Deduplication complete for {date}")
        print(f"Output location: s3://bucket/deduplicated/dt={date}")
        print(f"Check Dask dashboard for detailed metrics: http://localhost:8787")
        
    finally:
        # ========================================================================
        # STEP 11: Cleanup Resources
        # ========================================================================
        # Always close the Dask client to:
        # - Release worker processes
        # - Free memory
        # - Close network connections
        # - Shutdown the dashboard server
        #
        # The finally block ensures cleanup even if an error occurs
        client.close()

# Usage example
if __name__ == "__main__":
    import sys
    from datetime import datetime, timedelta
    
    # Default to yesterday
    date = sys.argv[1] if len(sys.argv) > 1 else \
           (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Deduplicating data for {date}")
    deduplicate_daily_dask(date)
```

**Dask vs Spark Comparison**:

| Feature | Spark | Dask |
|---------|-------|------|
| **Setup** | Requires JVM, more complex | Python-native, simpler |
| **Memory** | Higher overhead | More efficient for smaller datasets |
| **Performance** | Better for very large data (TB+) | Good for GB-TB range |
| **Window Functions** | Native support | Requires workarounds |
| **Deployment** | Needs cluster (EMR, Databricks) | Works on single machine or cluster |
| **Cost** | Higher (cluster required) | Lower (can run on modest hardware) |
| **Best For** | Enterprise, huge scale | Small-medium teams, cost-sensitive |
```

**Pros**:
- Perfect accuracy
- Reduces long-term storage costs
- Can add data quality checks

**Cons**:
- Requires batch processing infrastructure
- Temporary 2x storage during dedup
- Complexity (scheduling, monitoring)

**Schedule**: Run daily at T+6 hours (let all instances finish uploading)

### Algorithm 6: Primary-with-Fallback

**Use Case**: When one instance is more reliable/closer to exchange

**Approach**: Always use primary instance, fill gaps from backups.

```rust
struct PrimaryFallbackDeduplicator {
    primary_prefix: String,
    backup_prefixes: Vec<String>,
}

impl PrimaryFallbackDeduplicator {
    fn query(&self, query: &Query) -> Vec<Message> {
        // 1. Query primary instance
        let mut primary_data = self.query_prefix(&self.primary_prefix, query);
        
        // 2. Detect gaps in primary
        let gaps = self.detect_gaps(&primary_data, query);
        
        if gaps.is_empty() {
            return primary_data;  // No gaps, done
        }
        
        // 3. Fill gaps from backups
        for backup_prefix in &self.backup_prefixes {
            let backup_data = self.query_prefix_for_gaps(backup_prefix, &gaps);
            primary_data.extend(backup_data);
            
            // Recheck gaps
            gaps = self.detect_gaps(&primary_data, query);
            if gaps.is_empty() {
                break;
            }
        }
        
        // 4. Sort by timestamp
        primary_data.sort_by_key(|m| m.exchange_timestamp);
        primary_data
    }
    
    fn detect_gaps(&self, data: &[Message], query: &Query) -> Vec<TimeRange> {
        // Look for time ranges with no data
        // Could also look for missing trade_id sequences
        
        let mut gaps = Vec::new();
        let expected_interval_ms = 100;  // Expect data every 100ms
        
        for window in data.windows(2) {
            let gap_ms = window[1].exchange_timestamp - window[0].exchange_timestamp;
            if gap_ms > expected_interval_ms {
                gaps.push(TimeRange {
                    start: window[0].exchange_timestamp,
                    end: window[1].exchange_timestamp,
                });
            }
        }
        
        gaps
    }
}
```

**Pros**:
- Minimal overhead for normal case
- Clear data lineage (prefer primary)
- Automatically handles primary failures

**Cons**:
- More complex query logic
- Gap detection heuristics
- Requires stable primary instance

## Hybrid Strategy (Recommended)

Combine multiple algorithms for best results:

### Real-Time Layer (Streaming Pipeline)

```rust
struct HybridDeduplicator {
    id_deduplicator: IdDeduplicator,      // For messages with IDs
    hash_deduplicator: HashDeduplicator,  // For messages without IDs
}

impl HybridDeduplicator {
    fn is_duplicate(&mut self, message: &Message) -> bool {
        // Try ID-based first (most accurate)
        if message.has_unique_id() {
            return self.id_deduplicator.is_duplicate(message);
        }
        
        // Fall back to hash-based
        self.hash_deduplicator.is_duplicate(message)
    }
}
```

### Storage Layer (Query Time)

```sql
-- View that handles deduplication transparently
CREATE VIEW market_data_dedup AS
SELECT DISTINCT ON (
  exchange,
  symbol,
  stream_type,
  COALESCE(trade_id, update_id),
  exchange_timestamp
)
  *,
  -- Prefer instance-1, then instance-2, then instance-3
  CASE 
    WHEN s3_prefix = 'instance-1' THEN 1
    WHEN s3_prefix = 'instance-2' THEN 2
    ELSE 3
  END as instance_priority
FROM market_data_raw
ORDER BY
  exchange, symbol, stream_type,
  COALESCE(trade_id, update_id),
  exchange_timestamp,
  instance_priority;
```

### Batch Layer (Cost Optimization)

```bash
# Daily cron job at 06:00 UTC
0 6 * * * /usr/local/bin/deduplicate-yesterday.sh
```

**Process**:
1. Real-time: Light deduplication (catch obvious duplicates)
2. Query-time: Exact deduplication (handle remaining duplicates)
3. Batch: Rewrite deduplicated data (reduce storage costs)

## Performance Comparison

| Algorithm | Accuracy | Memory | Speed | Storage Reduction |
|-----------|----------|--------|-------|-------------------|
| Hash-Based | 99.9%+ | O(n) | Fast | None (real-time only) |
| ID-Based | 100% | O(symbols) | Fast | None (real-time only) |
| Bloom Filter | 99%+ | O(bits) | Fast | None (real-time only) |
| Query-Time SQL | 100% | N/A | Slow | None |
| Batch Merge | 100% | O(partition) | Medium | Up to 67% (3 instances → 1) |
| Primary-Fallback | 100% | N/A | Medium | None |

## Configuration Examples

### High-Throughput (Prioritize Speed)

```toml
[deduplication]
# Real-time deduplication
enabled = true
algorithm = "id"  # Fast and accurate

[s3]
# Store duplicates, deduplicate at query time
prefix = "instance-${INSTANCE_ID}"
```

### Cost-Optimized (Reduce Storage)

```toml
[deduplication]
enabled = true
algorithm = "hybrid"

[batch_dedup]
enabled = true
schedule = "0 6 * * *"  # Daily at 6 AM
retention_days = 7  # Keep raw data for 7 days
target_prefix = "deduplicated"
```

### High-Reliability (Maximum Redundancy)

```toml
[deduplication]
# Don't deduplicate in real-time (keep all data)
enabled = false

[s3]
# Separate prefixes for each instance
prefix = "instance-${INSTANCE_ID}"

# Query-time deduplication with instance preference
[query]
dedup_strategy = "primary_fallback"
primary_instance = "instance-1"
```

## Monitoring Deduplication

### Key Metrics

```rust
struct DeduplicationMetrics {
    messages_received: Counter,
    duplicates_detected: Counter,
    duplicate_rate: Gauge,  // duplicates / total
    dedup_latency_us: Histogram,
    
    // By stream type
    duplicates_by_stream: HashMap<String, Counter>,
    
    // By instance
    duplicates_by_source: HashMap<String, Counter>,
}
```

### Expected Duplicate Rates

| Scenario | Expected Rate | Action if Exceeded |
|----------|---------------|-------------------|
| Single instance | < 0.1% | Check exchange behavior |
| 2 instances | ~50% | Normal (active-active) |
| 3 instances | ~66% | Normal (active-active) |
| Any | > 80% | Check configuration error |

### Alerts

```yaml
alerts:
  - name: DuplicateRateTooHigh
    condition: duplicate_rate > 0.90
    duration: 5m
    severity: warning
    description: "May indicate configuration error"
  
  - name: DuplicateRateTooLow
    condition: duplicate_rate < 0.40 AND instance_count > 2
    duration: 10m
    severity: warning
    description: "Expected ~66% with 3 instances"
  
  - name: DeduplicationLatencyHigh
    condition: p99(dedup_latency_us) > 1000
    duration: 5m
    severity: warning
    description: "Dedup taking > 1ms"
```

## Data Quality Validation

Beyond deduplication, validate data quality:

```rust
fn validate_deduplicated_data(data: &[Message]) -> ValidationReport {
    let mut report = ValidationReport::new();
    
    // 1. Check for exact duplicates (should be zero)
    let duplicates = find_exact_duplicates(data);
    report.exact_duplicates = duplicates.len();
    
    // 2. Check for gaps in sequence IDs
    let gaps = find_sequence_gaps(data);
    report.sequence_gaps = gaps;
    
    // 3. Check for timestamp ordering
    let out_of_order = find_out_of_order(data);
    report.out_of_order_count = out_of_order.len();
    
    // 4. Check for suspicious patterns
    let anomalies = detect_anomalies(data);
    report.anomalies = anomalies;
    
    report
}
```

## Best Practices

1. **Use Exchange-Provided IDs**: Always prefer `trade_id`, `update_id` over timestamps
2. **Never Use Local Timestamps**: Only use exchange timestamps for deduplication
3. **Monitor Duplicate Rates**: Track per instance and aggregate
4. **Test Deduplication**: Verify with known duplicates
5. **Document Instance Priority**: Which instance is "primary" for tie-breaking
6. **Automate Batch Dedup**: Don't rely on manual processes
7. **Keep Raw Data**: Retain before deduplication for debugging (7-30 days)
8. **Validate After Dedup**: Check for gaps and anomalies

## Troubleshooting

### Problem: High Duplicate Rate (> 70% with 2 instances)

**Possible Causes**:
- Third instance is running but not configured
- Exchange replaying old data
- WebSocket reconnection loop

**Solution**: Check all running instances, verify subscriptions

### Problem: Duplicates After Deduplication

**Possible Causes**:
- Dedup key missing critical field
- Clock skew causing different timestamps
- Using local timestamp instead of exchange timestamp

**Solution**: Review dedup key, use exchange-provided IDs

### Problem: Data Gaps After Deduplication

**Possible Causes**:
- Too aggressive deduplication
- Bug in dedup algorithm
- Actual missing data from exchange

**Solution**: Cross-reference with raw data, validate dedup logic

### Problem: Deduplication Causing Latency

**Possible Causes**:
- Large hash table lookups
- Bloom filter too large
- Inefficient key computation

**Solution**: Profile code, consider simpler algorithm, optimize hot path

## Conclusion

Effective deduplication is critical for geo-distributed deployments. The recommended approach is:

1. **Real-time**: ID-based or hybrid deduplication (fast, accurate)
2. **Query-time**: SQL-based deduplication (flexible, no data loss)
3. **Batch**: Periodic merge to reduce storage costs

This layered approach balances speed, accuracy, and cost while maintaining data integrity and reliability.
