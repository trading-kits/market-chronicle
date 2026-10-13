# Architecture Philosophy

## Design Principles

Market Chronicle is built on three core design principles:

1. **Statelessness**: No disk-based durability buffer
2. **Memory-only buffering**: All buffering happens in tokio channels
3. **Acceptable data loss**: Trade reliability for simplicity

This document explains the rationale behind these decisions and when this architecture is appropriate.

## Stateless Design

### What It Means

**Stateless** means the service maintains no persistent state on disk. All operational state exists only in memory:

- WebSocket connection state
- Batch aggregator state (active batches)
- Channel buffers (messages and batches)

### Benefits

**1. Operational Simplicity**
- No disk I/O complexity
- No segment file management
- No cleanup tasks or rotation logic
- Easier debugging (less state to track)

**2. Lower Latency**
- No disk write step
- Data uploaded to S3 immediately when batch ready
- Typical latency: 5-10 seconds from message receive to S3

**3. Horizontal Scalability**
- No state coordination between instances
- No distributed locks or consensus
- Can deploy unlimited instances
- Each instance is independent

**4. Deployment Flexibility**
- No persistent volumes needed
- Can run in ephemeral containers
- Easy to move between hosts
- Fast startup/shutdown

### Tradeoffs

**1. Data Loss on Crash**
- Any data in memory is lost if service crashes
- No recovery mechanism for in-flight data
- Cannot replay from local disk

**2. No Local Debugging**
- Cannot inspect data before it reaches S3
- Cannot debug compression or serialization locally
- Must query S3 to see what was uploaded

**3. Requires S3 Availability**
- Service cannot buffer data if S3 is down
- No offline operation mode
- Backpressure will eventually block workers

## Memory-Only Buffering

### Implementation

All buffering uses tokio's `mpsc` channels:

```rust
// Message channel: WebSocket workers → Batch aggregator
let (message_tx, message_rx) = mpsc::channel(100000);

// Batch channel: Batch aggregator → S3 uploaders
let (batch_tx, batch_rx) = mpsc::channel(10000);
```

### Benefits

**1. Automatic Backpressure**
- Channels provide natural flow control
- No manual rate limiting needed
- System self-regulates based on capacity

**2. Bounded Memory Usage**
- Channel sizes limit maximum memory
- Predictable resource usage
- No unbounded growth

**3. Low Overhead**
- In-memory operations are fast
- No serialization until S3 upload
- No disk I/O penalty

### Capacity Planning

**Message Channel** (default: 100,000 messages):
- Average message size: ~500 bytes
- Buffer memory: 100,000 × 500 bytes = **50 MB**

**Batch Channel** (default: 10,000 batches):
- Average batch size: 1,000 messages × 500 bytes = 500 KB
- Buffer memory: 10,000 × 500 KB = **5 GB**

**Active Batches** (variable):
- Depends on number of active partitions
- Each partition = one active batch
- Estimate: 1,000 partitions × 500 messages × 500 bytes = **250 MB**

**Total Memory**: ~**5.3 GB** for buffering (plus Rust overhead)

## When to Accept Data Loss

This architecture is appropriate when:

### ✅ Use This Architecture When...

**1. Data is Redundant**
- Multiple instances collect same data
- Exchange provides historical data API
- Gaps can be backfilled from other sources

**2. 99.9% Reliability is Sufficient**
- Occasional gaps are acceptable
- Perfect data not business-critical
- Statistical accuracy more important than completeness

**3. Operational Simplicity is Valued**
- Small team maintaining service
- Prefer simple architecture over complex reliability
- Less code to maintain and debug

**4. Cost-Sensitive**
- Don't want to pay for persistent disks
- Ephemeral infrastructure is cheaper
- Storage costs matter

**5. Low-Latency Priority**
- Need data in S3 as fast as possible
- Disk writes add unacceptable latency
- Real-time analytics need fresh data

### ❌ Don't Use This Architecture When...

**1. Zero Data Loss Required**
- Financial transactions
- Compliance/audit logs
- Legally required data retention

**2. No Redundancy Available**
- Single instance deployment
- No way to backfill gaps
- Data is unique to this service

**3. Crash-Prone Environment**
- Unstable infrastructure
- Frequent OOM kills
- Unreliable hosts

**4. Large Replay Windows**
- Need to replay hours of data
- Disk buffer would enable replay
- Cannot afford to lose hours of work

## Comparison with Disk-Based Architectures

### Disk-Based Alternative

```
WebSocket → Memory Channel → Disk Buffer → S3 Uploader
                                  ↓
                            Persistent State
```

**Benefits**:
- Crash recovery possible
- Can replay from disk
- No data loss on restart
- Local debugging capability

**Costs**:
- More complex code
- Disk I/O overhead
- Segment management logic
- Cleanup and rotation
- Higher latency
- Requires persistent storage

### Memory-Only (This Architecture)

```
WebSocket → Memory Channel → S3 Uploader
```

**Benefits**:
- Simple implementation
- Lower latency
- No disk management
- Easier operations
- Cheaper infrastructure

**Costs**:
- Data loss on crash
- No local debugging
- Requires S3 availability
- Less fault-tolerant

## Backpressure Strategy

### How Backpressure Works

```
S3 Slow → Batch Channel Fills → Aggregator Blocks → Message Channel Fills → Workers Block → WebSocket Pauses
```

### Natural Flow Control

The system automatically throttles based on capacity:

1. **Normal Operation**: All channels have space, data flows freely
2. **S3 Slowdown**: Batch channel starts filling
3. **Aggregator Blocks**: Cannot send more batches
4. **Message Channel Fills**: Workers keep sending messages
5. **Workers Block**: Cannot send more messages
6. **WebSocket Pauses**: Workers stop receiving from WebSocket
7. **Self-Regulation**: System reaches equilibrium

### No Data Loss During Backpressure

Important: Backpressure does NOT cause data loss. Workers block, but:
- WebSocket connection remains alive
- Messages queue at exchange side
- Workers resume when pressure relieves
- All messages eventually processed

### Recovery from Backpressure

When S3 recovers:
1. Uploaders drain batch channel
2. Aggregator resumes sending batches
3. Message channel drains
4. Workers resume sending messages
5. WebSocket workers catch up
6. System returns to normal

## Mitigation Strategies

While accepting some data loss, you can minimize it:

### 1. Multiple Instances

Deploy 2-3 redundant instances:
- Each instance collects independently
- Each writes to separate S3 prefix
- Query layer deduplicates
- One instance crash ≠ data loss

### 2. Health Monitoring

Aggressive health monitoring:
- Detect crashes immediately
- Alert on service down
- Automatic restart
- Minimize downtime

### 3. S3 Reliability

Ensure S3 is highly available:
- Use AWS S3 (99.99% uptime)
- Multi-region replication
- Monitor S3 health
- Have fallback storage

### 4. Proper Sizing

Size channels appropriately:
- Larger buffers = more data loss on crash
- Larger buffers = better handling of transients
- Balance based on crash frequency

### 5. Graceful Shutdown

Always use graceful shutdown:
- Flush all buffers before exit
- Wait for uploads to complete
- Minimize data loss on restart

## Production Recommendations

For production deployments:

### Minimum Requirements

1. **At least 2 instances** for redundancy
2. **Health checks** and automatic restarts
3. **Monitoring** of crash rates and gaps
4. **Alerting** on service down
5. **Documentation** of acceptable data loss rate

### Optional Enhancements

1. **3+ instances** for higher reliability
2. **Cross-region deployment** for disaster recovery
3. **S3 replication** for data durability
4. **Automated backfill** from exchange historical APIs
5. **Data quality monitoring** to detect gaps

## When to Upgrade Architecture

Consider disk-based architecture if:

1. **Data loss rate > 0.1%** in production
2. **Frequent crashes** (>1 per day)
3. **Regulatory requirements** demand zero loss
4. **No redundancy** options available
5. **Backfill costs** become significant

Signs you need more reliability:
- Customer complaints about gaps
- Failed audits due to missing data
- Business decisions affected by gaps
- Team spending time investigating losses

## Conclusion

The stateless, memory-only architecture is a deliberate design choice that prioritizes:

- **Simplicity** over complexity
- **Latency** over reliability
- **Operations** over features
- **Cost** over perfection

This makes Market Chronicle ideal for:
- Analytics and research (not financial transactions)
- Teams valuing operational simplicity
- Deployments with redundancy available
- Use cases where 99.9% reliability suffices

If your requirements demand higher reliability, this architecture can be extended with disk buffering, but understand the operational complexity cost.
