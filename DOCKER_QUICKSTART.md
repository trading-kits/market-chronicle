# Docker Quickstart Guide

This guide will help you get Market Chronicle running in Docker in under 5 minutes.

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ free disk space
- Internet connection (for pulling images and connecting to exchanges)

## Quick Start

### 1. Start the Complete Stack

```bash
# Start all services (MinIO, LakeFS, Prometheus, Grafana, Market Chronicle)
make docker-up

# Or use docker-compose directly
docker-compose up -d
```

This will start:
- ✅ MinIO (S3 storage) - http://localhost:9000
- ✅ LakeFS (Data versioning) - http://localhost:8000
- ✅ Prometheus (Metrics) - http://localhost:9091
- ✅ Grafana (Dashboards) - http://localhost:3000
- ✅ Market Chronicle (Service) - Metrics at http://localhost:9090

### 2. Configure the Service

Create your configuration file:

```bash
# Copy the Docker example config
cp config.docker.toml config.toml

# Edit with your settings (optional - the defaults work!)
# nano config.toml
```

The default config subscribes to BTCUSDT, ETHUSDT, BNBUSDT on Binance Spot.

### 3. Rebuild and Restart (if you changed config)

```bash
make docker-rebuild

# Or manually
docker-compose build market-chronicle
docker-compose up -d market-chronicle
```

### 4. View Logs

```bash
# Market Chronicle logs only
make docker-logs

# All services
make docker-logs-all

# Or use docker-compose
docker-compose logs -f market-chronicle
```

### 5. Check Metrics

```bash
# Health check
curl http://localhost:9090/health

# Metrics endpoint
curl http://localhost:9090/metrics | head -50

# Specific metrics
curl -s http://localhost:9090/metrics | grep -E "(batch_|worker_|s3_)"
```

### 6. View in Grafana

1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. Go to **Dashboards** → **Import**
4. Upload `grafana-dashboard.json`
5. Select the Prometheus datasource
6. Click **Import**

You'll see real-time metrics!

### 7. View Data in MinIO

1. Open http://localhost:9001
2. Login with `minioadmin` / `minioadmin`
3. Navigate to the `binance-data` bucket
4. Browse the Hive-style partitioned data

## Common Operations

### View Active Containers

```bash
docker-compose ps
```

### Restart a Single Service

```bash
# Restart market-chronicle
docker-compose restart market-chronicle

# Restart prometheus
docker-compose restart prometheus
```

### Update Configuration

```bash
# 1. Edit config.toml
nano config.toml

# 2. Rebuild the service
make docker-rebuild

# 3. Check logs for changes
make docker-logs
```

### Scale Up

```bash
# Run multiple market-chronicle instances
docker-compose up -d --scale market-chronicle=3
```

**Note**: Each instance should use a different S3 prefix to avoid conflicts.

### Stop Everything

```bash
# Stop all services (keeps data)
make docker-down

# Stop and remove everything including data
make docker-clean
```

## Monitoring

### Prometheus Queries

Access Prometheus at http://localhost:9091

Example queries:
```promql
# Message rate
rate(worker_messages_received_total[1m])

# Batch creation rate
rate(batch_created_total[1m])

# S3 upload latency (95th percentile)
histogram_quantile(0.95, rate(s3_upload_duration_seconds_bucket[5m]))

# Active streams
sum(worker_active_streams)
```

### Grafana Dashboards

1. Open http://localhost:3000
2. Import `grafana-dashboard.json`
3. View real-time metrics with beautiful visualizations

### Logs

```bash
# Real-time logs with colors
docker-compose logs -f --tail=100 market-chronicle

# Filter for errors only
docker-compose logs market-chronicle | grep ERROR

# Save logs to file
docker-compose logs market-chronicle > market-chronicle.log
```

## Data Management

### Check S3 Storage Size

```bash
# From MinIO container
docker exec market-chronicle-minio du -sh /data

# Using AWS CLI with MinIO
aws s3 ls s3://binance-data/ \
  --endpoint-url http://localhost:9000 \
  --no-sign-request \
  --recursive \
  --human-readable \
  --summarize
```

### Backup Data

```bash
# Backup volumes
docker run --rm -v market-chronicle_minio_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/minio-backup.tar.gz /data

# Restore
docker run --rm -v market-chronicle_minio_data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/minio-backup.tar.gz -C /
```

### Clear Data

```bash
# Stop services
docker-compose down

# Remove volumes (WARNING: deletes all data!)
docker volume rm market-chronicle_minio_data
docker volume rm market-chronicle_prometheus_data
docker volume rm market-chronicle_grafana_data

# Start fresh
docker-compose up -d
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs market-chronicle

# Check if port 9090 is already in use
lsof -i :9090

# Rebuild from scratch
docker-compose build --no-cache market-chronicle
docker-compose up -d market-chronicle
```

### No Data in MinIO

```bash
# 1. Check service is running
docker-compose ps

# 2. Check logs for S3 errors
docker-compose logs market-chronicle | grep -i "s3\|upload"

# 3. Verify MinIO is accessible
curl http://localhost:9000/minio/health/live

# 4. Check bucket exists
docker exec market-chronicle-minio mc ls myminio/
```

### No Metrics in Prometheus

```bash
# 1. Check metrics endpoint
curl http://localhost:9090/metrics

# 2. Check Prometheus targets
curl http://localhost:9091/api/v1/targets | jq

# 3. Check Prometheus config
docker exec market-chronicle-prometheus cat /etc/prometheus/prometheus.yml

# 4. Restart Prometheus
docker-compose restart prometheus
```

### High CPU/Memory Usage

```bash
# Check resource usage
docker stats market-chronicle

# Reduce load:
# 1. Edit config.toml - reduce number of symbols
# 2. Increase batch_size to reduce I/O
# 3. Set RUST_LOG=warn to reduce logging

# Rebuild
make docker-rebuild
```

## Performance Tuning

### For High Volume (1000+ symbols)

Edit `config.toml`:

```toml
[s3]
batch_size = 5000          # Larger batches
batch_timeout_ms = 30000   # 30 seconds
upload_worker_count = 8    # More upload workers
compression_level = 1      # Faster compression

[[exchanges]]
ws_streams_per_connection = 1024  # More streams per connection
ws_max_batch_size = 200           # Batch subscriptions
ws_subscription_delay_ms = 100    # Rate limit subscriptions
```

### For Low Volume (< 10 symbols)

```toml
[s3]
batch_size = 100           # Smaller batches
batch_timeout_ms = 5000    # 5 seconds
upload_worker_count = 2    # Fewer workers
compression_level = 5      # Better compression

[[exchanges]]
ws_streams_per_connection = 50
ws_max_batch_size = 10
ws_subscription_delay_ms = 500
```

## Next Steps

1. **Customize Configuration**: Edit `config.toml` to add more exchanges/symbols
2. **Set Up Alerts**: Configure Prometheus alerts using `prometheus-alerts.yml`
3. **Create Custom Dashboards**: Build Grafana dashboards for your specific needs
4. **Production Deployment**: See [README.md](README.md) for Kubernetes deployment

## Useful Commands Reference

```bash
# Start
make docker-up

# Stop
make docker-down

# View logs
make docker-logs

# Rebuild
make docker-rebuild

# Clean everything
make docker-clean

# Check health
curl http://localhost:9090/health
curl http://localhost:9091/-/healthy

# View metrics
curl http://localhost:9090/metrics
```

## Getting Help

- Documentation: [docs/](docs/)
- Issues: GitHub Issues
- Logs: `docker-compose logs market-chronicle`
- Health: `curl http://localhost:9090/health`
