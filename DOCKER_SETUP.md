# Docker Compose Setup Guide

## Overview

The `deploy/docker-compose.yml` provides a complete local development and testing environment for Market Chronicle, including:

- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Database for LakeFS metadata
- **LakeFS**: Data lake with Git-like versioning
- **Prometheus**: Metrics collection and storage
- **Grafana**: Metrics visualization and dashboards

## Quick Start

### 1. Start All Services

```bash
# Start all services
docker-compose -f deploy/docker-compose.yml up -d

# Check service status
docker-compose -f deploy/docker-compose.yml ps

# View logs
docker-compose -f deploy/docker-compose.yml logs -f
```

### 2. Access Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **LakeFS UI** | http://localhost:8000 | (setup on first visit) |
| **Prometheus** | http://localhost:9091 | (no auth) |
| **Grafana** | http://localhost:3000 | admin / admin |

### 3. Initial Setup

#### LakeFS Setup (First Time Only)

1. Visit http://localhost:8000
2. Create admin user credentials
3. Create a repository:
   - **Name**: `market-data`
   - **Storage namespace**: `s3://binance-data/`
   - **Default branch**: `main`
4. Save the access credentials

#### Grafana Setup

1. Visit http://localhost:3000
2. Login with `admin` / `admin` (change password on first login)
3. Go to **Configuration** → **Data Sources**
4. Prometheus should be auto-configured at `http://prometheus:9090`
5. Import dashboards (see below)

### 4. Run Market Chronicle

With services running, start the Market Chronicle service on your host:

```bash
# Update config.toml to point to services
# endpoint = "http://localhost:8000"  # LakeFS
# or
# endpoint = "http://localhost:9000"  # MinIO

RUST_LOG=info cargo run --bin market-chronicle -- config.toml
```

The service will:
- Connect to LakeFS/MinIO on port 8000/9000
- Expose metrics on port 9090
- Prometheus will automatically scrape metrics every 10 seconds

## Services Details

### MinIO (S3 Storage)

**Ports:**
- 9000: S3 API
- 9001: Web Console

**Default Credentials:**
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

**Usage:**
```bash
# Create bucket manually
aws s3 mb s3://market-data --endpoint-url http://localhost:9000

# List objects
aws s3 ls s3://market-data/ --endpoint-url http://localhost:9000
```

### LakeFS (Data Lake)

**Ports:**
- 8000: Web UI & API

**Features:**
- Git-like versioning for data
- Branch/merge/commit operations
- S3-compatible gateway
- Backed by MinIO storage

**Usage:**
```bash
# List objects via LakeFS
aws s3 ls s3://market-data/main/ --endpoint-url http://localhost:8000
```

### Prometheus (Metrics)

**Ports:**
- 9091: Web UI (remapped to avoid conflict with market-chronicle:9090)

**Configuration:**
- Scrapes market-chronicle metrics from `host.docker.internal:9090`
- Scrape interval: 10 seconds
- Retention: 15 days (default)

**Useful Queries:**
```promql
# Message rate
rate(worker_messages_received[1m])

# Active streams
sum(worker_streams_active)

# Upload success rate
rate(s3_uploads_total[5m])
```

### Grafana (Visualization)

**Ports:**
- 3000: Web UI

**Default Credentials:**
- Username: `admin`
- Password: `admin`

**Setup:**
1. Prometheus datasource is auto-configured
2. Create dashboards or import provided JSON
3. Set up alerts based on Prometheus metrics

## Docker Compose Commands

### Start Services

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d minio

# Start with logs visible
docker-compose up
```

### Stop Services

```bash
# Stop all services
docker-compose -f deploy/docker-compose.yml stop

# Stop and remove containers
docker-compose -f deploy/docker-compose.yml down

# Stop and remove containers + volumes (⚠️ deletes all data)
docker-compose -f deploy/docker-compose.yml down -v
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f prometheus

# Last 100 lines
docker-compose logs --tail=100 grafana
```

### Restart Services

```bash
# Restart all
docker-compose -f deploy/docker-compose.yml restart

# Restart specific service
docker-compose -f deploy/docker-compose.yml restart prometheus
```

### Check Status

```bash
# List containers
docker-compose ps

# Check resource usage
docker stats
```

## Networking

All services run on a custom bridge network `market-chronicle-net` for:
- Service discovery by name (e.g., `http://prometheus:9090`)
- Isolation from other Docker containers
- DNS resolution between containers

### Accessing Services

**From host machine:**
- Use `localhost` or `127.0.0.1`
- Example: `http://localhost:9091` (Prometheus)

**From market-chronicle on host to services:**
- Use `localhost` for MinIO/LakeFS
- Metrics exposed on host's `localhost:9090` for Prometheus

**Between Docker services:**
- Use service names (e.g., `prometheus`, `minio`)
- Example: `http://prometheus:9090` from Grafana

## Volumes

Data is persisted in Docker volumes:

| Volume | Purpose | Backup Recommended |
|--------|---------|-------------------|
| `postgres-data` | LakeFS metadata | Yes - Critical |
| `minio-data` | Object storage | Yes - All data |
| `lakefs-data` | LakeFS local data | Yes - Important |
| `prometheus-data` | Metrics history | Optional |
| `grafana-data` | Dashboards/config | Optional |

### Backup Volumes

```bash
# Backup MinIO data
docker run --rm -v market-chronicle_minio-data:/data -v $(pwd):/backup alpine tar czf /backup/minio-backup.tar.gz /data

# Restore MinIO data
docker run --rm -v market-chronicle_minio-data:/data -v $(pwd):/backup alpine tar xzf /backup/minio-backup.tar.gz -C /
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose -f deploy/docker-compose.yml logs [service-name]

# Check if port is already in use
lsof -i :9000  # MinIO
lsof -i :8000  # LakeFS
lsof -i :3000  # Grafana
```

### Prometheus Not Scraping Metrics

1. Check market-chronicle is running: `curl http://localhost:9090/metrics`
2. Check Prometheus targets: http://localhost:9091/targets
3. Check Prometheus config: `docker-compose exec prometheus cat /etc/prometheus/prometheus.yml`

### Grafana Can't Connect to Prometheus

1. Check Prometheus is healthy: http://localhost:9091/-/healthy
2. From Grafana container: `docker-compose -f deploy/docker-compose.yml exec grafana ping prometheus`
3. Check datasource config in Grafana UI

### LakeFS Can't Connect to MinIO

1. Check MinIO is running: `docker-compose ps minio`
2. Check MinIO health: http://localhost:9000/minio/health/live
3. Check LakeFS logs: `docker-compose logs lakefs`

### Reset Everything

```bash
# Stop and remove everything
docker-compose -f deploy/docker-compose.yml down -v

# Remove all market-chronicle related data
docker volume rm $(docker volume ls -q | grep market-chronicle)

# Start fresh
docker-compose -f deploy/docker-compose.yml up -d
```

## Production Considerations

⚠️ **This setup is for DEVELOPMENT ONLY**

For production:

1. **Use managed services**:
   - AWS S3 instead of MinIO
   - AWS RDS instead of PostgreSQL
   - Prometheus/Grafana Cloud or managed instances

2. **Security hardening**:
   - Change default passwords
   - Use secrets management (Docker secrets, Vault)
   - Enable TLS/HTTPS
   - Restrict network access
   - Regular security updates

3. **High availability**:
   - Multiple replicas for each service
   - Load balancing
   - Automatic failover
   - Backup and disaster recovery

4. **Resource limits**:
   ```yaml
   services:
     prometheus:
       deploy:
         resources:
           limits:
             cpus: '2'
             memory: 4G
   ```

## Next Steps

1. **Configure Market Chronicle** to use local services
2. **Run the service** and verify metrics are being collected
3. **Create Grafana dashboards** for monitoring
4. **Set up alerts** in Prometheus/Grafana
5. **Test data flow** end-to-end

## Useful Links

- [MinIO Documentation](https://min.io/docs/minio/container/index.html)
- [LakeFS Documentation](https://docs.lakefs.io/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
