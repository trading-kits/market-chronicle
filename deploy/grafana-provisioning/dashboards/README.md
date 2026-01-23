# Grafana Dashboard Provisioning

This directory contains dashboard provisioning configuration for automatic dashboard import.

## Files

- **`dashboard.yml`** - Provisioning configuration that tells Grafana to auto-import dashboards from this directory
- **`grafana-dashboard.json`** - The main Market Chronicle metrics dashboard

## How It Works

When Grafana starts, it automatically:
1. Reads `dashboard.yml` which configures the dashboard provider
2. Scans this directory for `*.json` files
3. Imports all JSON dashboards into Grafana
4. Monitors for changes every 10 seconds (configurable via `updateIntervalSeconds`)

## Accessing the Dashboard

1. Open Grafana: http://localhost:3000
2. Login with `admin` / `admin`
3. Go to **Dashboards** → **Browse**
4. Look for "Market Chronicle" dashboard (auto-imported)

## Adding Custom Dashboards

To add more dashboards:
1. Place your dashboard JSON file in this directory
2. Grafana will auto-import it within 10 seconds
3. Or restart Grafana: `docker restart market-chronicle-grafana`

## Configuration

The `dashboard.yml` settings:

```yaml
providers:
  - name: 'Market Chronicle'           # Provider name
    orgId: 1                            # Organization ID
    folder: ''                          # Folder name (empty = General)
    type: file                          # Source type (file-based)
    disableDeletion: false              # Allow deletion from UI
    updateIntervalSeconds: 10           # Check for changes every 10s
    allowUiUpdates: true                # Allow editing from UI
    options:
      path: /etc/grafana/provisioning/dashboards  # Path to scan
```

## Exporting Dashboards

To export a dashboard after making changes in Grafana UI:

1. Open the dashboard in Grafana
2. Click the **Share** icon (top right)
3. Select **Export** → **Save to file**
4. Replace `grafana-dashboard.json` with the exported file
5. Dashboard will update automatically

## Troubleshooting

### Dashboard not appearing

```bash
# Check provisioning logs
docker logs market-chronicle-grafana 2>&1 | grep -i dashboard

# Verify files are mounted
docker exec market-chronicle-grafana ls -la /etc/grafana/provisioning/dashboards/

# Restart Grafana
docker restart market-chronicle-grafana
```

### Dashboard shows "No data"

1. Check Prometheus datasource is configured
2. Verify market-chronicle service is running and exposing metrics
3. Check Prometheus is scraping: http://localhost:9091/targets

## Notes

- Dashboards are provisioned **read-write** by default (`allowUiUpdates: true`)
- Changes made in Grafana UI are **not** persisted to the JSON file
- To persist UI changes, export and replace the JSON file
- Grafana checks for dashboard updates every 10 seconds
