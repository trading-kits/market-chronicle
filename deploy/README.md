# Deploy Directory

This directory contains all deployment-specific configuration files for Market Chronicle.

## Files

### Docker

- **`docker-compose.yml`** - Complete Docker Compose stack with all services
- **`Dockerfile`** - Container image for the Market Chronicle service  
- **`.dockerignore`** - Files to exclude from Docker build context

### Monitoring

- **`prometheus.yml`** - Prometheus metrics collection configuration
- **`prometheus-alerts.yml`** - Prometheus alerting rules
- **`grafana-dashboard.json`** - Pre-built Grafana dashboard for metrics (auto-imported)
- **`grafana-provisioning/`** - Auto-provisioned Grafana datasources and dashboards
  - `datasources/prometheus.yml` - Prometheus datasource (auto-configured)
  - `datasources/loki.yml` - Loki logs datasource (auto-configured)
  - `dashboards/dashboard.yml` - Dashboard provisioning config (auto-imports grafana-dashboard.json)
  - `dashboards/grafana-dashboard.json` - Main metrics dashboard

### Log Aggregation

- **`loki-config.yml`** - Loki log storage configuration
- **`vector.toml`** - Vector log collection and routing pipeline

## Quick Start

```bash
# From the project root directory
docker-compose -f deploy/docker-compose.yml up -d

# Or use justfile
just docker-up
```

## Services

The docker-compose stack includes:

| Service | Port | Description |
|---------|------|-------------|
| **MinIO** | 9000, 9001 | S3-compatible object storage |
| **Prometheus** | 9091 | Metrics collection and storage |
| **Grafana** | 3000 | Metrics and logs visualization |
| **Loki** | 3100 | Log aggregation storage |
| **Vector** | 8686 | Log collection and routing |
| **Market Chronicle** | 9090 | Main application |

## Configuration

### Modifying Prometheus Scrape Targets

Edit `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'market-chronicle'
    static_configs:
      - targets: ['market-chronicle:9090']
```

### Adjusting Log Retention

Edit `loki-config.yml`:

```yaml
limits_config:
  retention_period: 744h  # 31 days
```

### Customizing Vector Pipeline

Edit `vector.toml` to:
- Add log filtering
- Route logs to multiple destinations
- Parse custom log formats
- Add labels

## Documentation

- [Docker Quickstart Guide](../DOCKER_QUICKSTART.md)
- [Docker Setup Guide](../DOCKER_SETUP.md)
- [Log Aggregation Guide](../docs/log-aggregation.md)
- [Metrics Guide](../docs/metrics.md)

## Production Deployment

⚠️ **This setup is for development/testing only**

For production:
1. Use managed services (AWS S3, RDS, etc.)
2. Enable TLS/HTTPS
3. Use secrets management
4. Set resource limits
5. Configure high availability
6. Enable backup and disaster recovery

See [Geo-Distributed Deployment Guide](../docs/geo-distributed-deployment.md) for production considerations.
