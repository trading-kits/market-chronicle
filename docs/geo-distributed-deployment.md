# Geo-Distributed Deployment

## Overview

Market Chronicle's stateless architecture enables easy **multi-instance deployment** across different geographic regions or data centers. This document covers deployment patterns, redundancy strategies, rate limit distribution, and duplicate data handling.

## Why Deploy Multiple Instances

### 1. High Availability

**Problem**: Single instance failure = data loss

**Solution**: Multiple independent instances ensure continuous data collection even if one instance fails.

**Example**:
- Instance A crashes at 10:00 AM
- Instance B and C continue collecting data
- No gap in dataset
- Instance A restarts and resumes

### 2. Rate Limit Distribution

**Problem**: Exchange API limits per IP address

**Solution**: Each instance uses different IP, multiplying effective rate limits.

**Binance Example**:
- Limit: 300 WebSocket connections per IP
- Single instance: Max 300 connections
- 3 instances (3 IPs): Max 900 connections
- **3x capacity** for same exchange

### 3. Geographic Distribution

**Problem**: Network latency to exchanges

**Solution**: Deploy instances closer to exchange data centers.

**Example**:
- Binance servers in Tokyo
- Instance in AWS ap-northeast-1 (Tokyo): ~2ms latency
- Instance in AWS us-east-1 (Virginia): ~150ms latency
- Tokyo instance gets data **148ms faster**

### 4. Regulatory Compliance

**Problem**: Data residency requirements

**Solution**: Deploy instances in required regions, store data in local S3.

**Example**:
- EU customers: Instance in eu-west-1 → S3 in eu-west-1
- US customers: Instance in us-east-1 → S3 in us-east-1
- Data never leaves required region

## Deployment Patterns

### Pattern 1: Active-Active Redundancy

**Description**: Multiple instances collect identical data simultaneously.

```
Instance 1 (us-east-1)  ┐
Instance 2 (us-west-2)  ├─→ Binance Spot (All Symbols)
Instance 3 (eu-west-1)  ┘
         ↓
  Separate S3 Prefixes
         ↓
   Query Layer (Dedup)
```

**Configuration**:
```toml
# Instance 1 config
[s3]
prefix = "instance-1"
bucket = "market-data"

# Instance 2 config
[s3]
prefix = "instance-2"
bucket = "market-data"

# Instance 3 config
[s3]
prefix = "instance-3"
bucket = "market-data"
```

**Benefits**:
- Maximum redundancy
- No single point of failure
- 3x data for error detection

**Costs**:
- 3x storage costs
- 3x compute costs
- Deduplication complexity

### Pattern 2: Geographic Distribution

**Description**: Instances in different regions, each responsible for subset of exchanges.

```
Instance 1 (ap-northeast-1) → Binance (Tokyo servers)
Instance 2 (us-east-1)      → Coinbase (US servers)
Instance 3 (eu-west-1)      → Kraken (EU servers)
```

**Configuration**:
```toml
# Instance 1: Binance only
[[exchanges]]
name = "binance"
# ...

# Instance 2: Coinbase only
[[exchanges]]
name = "coinbase"
# ...

# Instance 3: Kraken only
[[exchanges]]
name = "kraken"
# ...
```

**Benefits**:
- Lower latency (closer to exchanges)
- No duplicate data
- Optimized network costs

**Costs**:
- Less redundancy
- Instance failure = exchange gap
- More complex deployment

### Pattern 3: Primary-Backup

**Description**: Primary instance handles all traffic, backup instance ready for failover.

```
Instance 1 (Primary)  → Active collecting
Instance 2 (Backup)   → Standby (or active for redundancy)
```

**Implementation**:
- Both instances always running
- Both collecting data (active-active)
- Query layer prefers primary data
- Automatic failover if primary down

**Benefits**:
- Simple failover logic
- Lower deduplication cost (prefer primary)
- Fast recovery

**Costs**:
- Still 2x storage and compute
- Less redundancy than 3+ instances

### Pattern 4: Rate Limit Multiplication

**Description**: Deploy multiple instances solely to multiply connection capacity.

```
Instance 1 (IP 1) → 300 connections → Binance
Instance 2 (IP 2) → 300 connections → Binance
Instance 3 (IP 3) → 300 connections → Binance
         Total: 900 connections
```

**Use Case**: Need to subscribe to more symbols than single IP allows.

**Configuration**: Identical configs, different IP addresses.

**Benefits**:
- Overcome per-IP rate limits
- Scale to thousands of symbols
- Simple (no config differences)

**Costs**:
- 3x storage for same data
- Must deduplicate

## S3 Prefix Strategy

### Separate Prefixes (Recommended)

**Layout**:
```
s3://bucket/
├── instance-1/
│   ├── exchange=binance/...
│   └── exchange=bybit/...
├── instance-2/
│   ├── exchange=binance/...
│   └── exchange=bybit/...
└── instance-3/
    ├── exchange=binance/...
    └── exchange=bybit/...
```

**Benefits**:
- Easy to identify source instance
- Can query single instance
- Can drop instance if corrupt
- Clear deduplication strategy

**Query Pattern**:
```sql
-- Query all instances
SELECT * FROM market_data WHERE prefix IN ('instance-1', 'instance-2', 'instance-3')

-- Deduplicate by (timestamp, symbol)
SELECT DISTINCT ON (timestamp, symbol, exchange) *
FROM market_data
WHERE prefix IN ('instance-1', 'instance-2', 'instance-3')
ORDER BY timestamp, symbol, exchange, prefix
```

### Separate Buckets

**Layout**:
```
s3://market-data-instance-1/exchange=binance/...
s3://market-data-instance-2/exchange=binance/...
s3://market-data-instance-3/exchange=binance/...
```

**Benefits**:
- Complete isolation
- Independent retention policies
- Easier cost tracking per instance

**Costs**:
- More complex query setup
- Multiple S3 connections

## Deduplication Strategies

### Query-Time Deduplication

**When**: Reading data from S3

**Method**: `SELECT DISTINCT` or `ROW_NUMBER()` window function

**SQL Example**:
```sql
WITH ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY exchange, symbol, stream_type, timestamp 
      ORDER BY s3_prefix
    ) as rn
  FROM market_data
)
SELECT * FROM ranked WHERE rn = 1;
```

**Benefits**:
- Simple implementation
- No additional storage
- Flexible dedup strategy

**Costs**:
- Query overhead
- Slower queries
- Still store duplicates

### Batch Deduplication

**When**: Periodically (e.g., daily)

**Method**: Run batch job to deduplicate and rewrite

**Process**:
1. Read all prefixes for time range
2. Deduplicate by (timestamp, exchange, symbol, stream_type)
3. Write to deduplicated prefix
4. Delete original prefixes

**Benefits**:
- Queries read deduplicated data (faster)
- Reduce storage costs over time

**Costs**:
- Batch job complexity
- Temporary 2x storage during dedup
- Rewrite costs

### Primary Instance Strategy

**When**: One instance is "primary"

**Method**: Always prefer primary instance data, use backups only for gaps

**Process**:
1. Query primary prefix
2. Detect gaps (missing timestamps)
3. Fill gaps from backup prefixes
4. Return combined result

**Benefits**:
- Minimal deduplication
- Fast queries (mostly single prefix)
- Clear data lineage

**Costs**:
- Requires gap detection logic
- More complex query code

## Network Architecture

### Load Balancer NOT Required

Market Chronicle instances do NOT need load balancing:
- No inbound connections
- Each instance is independent
- No state sharing

### VPN/VPC Peering for S3

**Setup**:
```
Instance 1 (VPC A) ──────┐
                         │
Instance 2 (VPC B) ──────┼─→ S3 (Private Endpoint)
                         │
Instance 3 (VPC C) ──────┘
```

**Benefits**:
- Lower latency to S3
- Lower S3 transfer costs
- More secure (private network)

### Multi-Region S3

**Setup**:
```
Instance 1 (us-east-1) → S3 (us-east-1) ──┐
Instance 2 (eu-west-1) → S3 (eu-west-1) ──┼→ S3 Replication
Instance 3 (ap-northeast-1) → S3 (ap-northeast-1) ──┘
```

**Benefits**:
- Lower S3 write latency
- Data residency compliance
- Disaster recovery

**Costs**:
- S3 replication costs
- Cross-region transfer costs

## Monitoring Multi-Instance Deployments

### Key Metrics Per Instance

1. **Message Rate**: Messages/second from WebSocket
2. **Upload Rate**: Successful S3 uploads/second
3. **Health**: Instance running and healthy
4. **Gap Rate**: Missing data compared to other instances

### Aggregate Metrics

1. **Total Coverage**: % of symbols with data from at least one instance
2. **Redundancy**: Average number of instances with data per symbol
3. **Dedup Ratio**: Duplicate messages / total messages

### Alerting

**Critical Alerts**:
- All instances down for exchange
- Instance down > 5 minutes
- Upload failures > 10% for 5 minutes

**Warning Alerts**:
- Single instance down
- Duplicate rate > 3x expected
- Gap rate > 1%

## Cost Analysis

### Example: 3-Instance Deployment

**Assumptions**:
- Message rate: 1,000 msgs/sec
- Message size: 500 bytes
- Compression ratio: 5:1
- S3 storage cost: $0.023/GB/month

**Costs**:

| Item | Single Instance | 3 Instances | Multiplier |
|------|----------------|-------------|------------|
| Compute | $100/month | $300/month | 3x |
| S3 Storage | $50/month | $150/month | 3x |
| S3 Requests | $10/month | $30/month | 3x |
| Total | $160/month | $480/month | **3x** |

**Value**: 3x cost buys:
- High availability (99.99% uptime)
- 3x rate limit capacity
- Data validation (detect corruption)
- No data loss on single instance failure

### Cost Optimization

**1. Deduplicate Storage**
- Run weekly dedup job
- Reduce to 1x storage after dedup
- Saves ~$100/month in example

**2. Different Instance Sizes**
- Primary: Larger instance
- Backups: Smaller instances
- Saves ~$100/month in example

**3. Spot Instances**
- Use spot instances for backups
- Save 60-70% on compute
- Saves ~$120/month in example

**Optimized Cost**: $480 → $260/month (46% reduction)

## Deployment Checklist

### Infrastructure

- [ ] Provision compute instances in target regions
- [ ] Assign unique IP addresses (for rate limits)
- [ ] Setup VPC/networking
- [ ] Create S3 buckets or prefixes
- [ ] Configure S3 private endpoints (optional)

### Configuration

- [ ] Create unique config for each instance
- [ ] Set unique S3 prefix per instance
- [ ] Verify exchange endpoints
- [ ] Test S3 credentials
- [ ] Validate configuration files

### Monitoring

- [ ] Deploy monitoring agents
- [ ] Configure health checks
- [ ] Setup alerting rules
- [ ] Create dashboards per instance
- [ ] Setup aggregate dashboards

### Deployment

- [ ] Deploy instances one at a time
- [ ] Verify data flowing to S3
- [ ] Check for duplicates
- [ ] Test failover (stop one instance)
- [ ] Verify other instances continue

### Post-Deployment

- [ ] Monitor for 24 hours
- [ ] Verify deduplication working
- [ ] Check cost tracking
- [ ] Document any issues
- [ ] Update runbooks

## Failure Scenarios

### Scenario 1: Single Instance Down

**Impact**: No impact if 2+ other instances running

**Detection**: Health check fails

**Response**: Automatic (other instances continue)

**Recovery**: Restart instance, verify catch-up

### Scenario 2: All Instances Down

**Impact**: Data loss for duration of outage

**Detection**: All health checks fail, no S3 uploads

**Response**: Page on-call, immediate investigation

**Recovery**: 
1. Restore any downed instances
2. Identify gap duration
3. Backfill from exchange historical API if needed

### Scenario 3: Network Partition

**Impact**: Some instances cannot reach S3

**Detection**: Upload failures, S3 metrics drop

**Response**: Check network connectivity, S3 health

**Recovery**: Usually auto-recovers when network restored

### Scenario 4: S3 Outage

**Impact**: All instances cannot upload

**Detection**: All instances report upload failures

**Response**: 
- If short: Wait for S3 recovery (backpressure buffers data)
- If long: Consider temporary local disk buffer

**Recovery**: S3 recovers, instances drain buffers

## Best Practices

1. **Always Deploy 2+ Instances** in production
2. **Use Separate S3 Prefixes** for clear attribution
3. **Monitor All Instances** individually and aggregate
4. **Test Failover Regularly** (chaos engineering)
5. **Document Topology** (which instances, regions, purposes)
6. **Automate Deduplication** (don't rely on manual)
7. **Alert on All-Down** scenarios immediately
8. **Plan for Backfill** from exchange APIs as backup

## Conclusion

Geo-distributed deployment is Market Chronicle's superpower:
- Trivial to deploy (stateless)
- Multiplies capacity (rate limits)
- Ensures availability (redundancy)
- Simple operations (independent instances)

The main cost is storage duplication, which can be mitigated through deduplication strategies. For production use cases requiring high availability, deploying 2-3 instances across regions is strongly recommended.
