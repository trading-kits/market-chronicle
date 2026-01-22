use crate::metrics::Metrics;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tracing::{error, info};

/// HTTP server for exposing metrics
pub struct MetricsServer {
    metrics: Arc<Metrics>,
    port: u16,
    shutdown: Arc<Notify>,
}

impl MetricsServer {
    pub fn new(metrics: Arc<Metrics>, port: u16, shutdown: Arc<Notify>) -> Self {
        Self { 
            metrics, 
            port,
            shutdown,
        }
    }
    
    /// Start the metrics HTTP server with graceful shutdown
    pub async fn start(self) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .with_state(self.metrics);
        
        let addr = format!("0.0.0.0:{}", self.port);
        info!(addr = %addr, "Starting metrics server");
        
        let listener = TcpListener::bind(&addr).await?;
        
        // Create graceful shutdown future
        let shutdown_signal = self.shutdown.clone();
        let graceful_shutdown = async move {
            shutdown_signal.notified().await;
            info!("Metrics server received shutdown signal");
        };
        
        axum::serve(listener, app)
            .with_graceful_shutdown(graceful_shutdown)
            .await
            .map_err(|e| anyhow::anyhow!("Metrics server error: {}", e))?;
        
        info!("Metrics server stopped");
        Ok(())
    }
}

/// Handler for /metrics endpoint (Prometheus format)
async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> Response {
    match metrics.export_prometheus() {
        Ok(data) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            data,
        )
            .into_response(),
        Err(e) => {
            error!("Failed to export metrics: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to export metrics").into_response()
        }
    }
}

/// Handler for /health endpoint
async fn health_handler() -> Response {
    (StatusCode::OK, "OK").into_response()
}
