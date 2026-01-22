mod worker;
mod worker_pool;
mod symbol_manager;
mod batch_aggregator;
mod s3_uploader;
mod metrics;
mod metrics_server;

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Region, Builder as S3ConfigBuilder};
use market_chronicle_core::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use batch_aggregator::BatchAggregator;
use metrics::Metrics;
use metrics_server::MetricsServer;
use s3_uploader::S3Uploader;
use symbol_manager::SymbolManager;
use worker_pool::WorkerPoolManager;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .json()
        .init();
    
    info!("Market Chronicle starting");
    
    // Load configuration
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    
    let config = Config::from_file(&config_path)?;
    info!(
        exchanges = config.exchanges.len(),
        s3_bucket = %config.s3.bucket,
        "Configuration loaded"
    );
    
    // Initialize metrics
    let metrics = Arc::new(Metrics::new("market-chronicle")?);
    info!("OpenTelemetry metrics initialized");
    
    // Create shutdown signal
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();
    
    // Start metrics HTTP server on port 9090
    let metrics_server = MetricsServer::new(metrics.clone(), 9090, shutdown.clone());
    let metrics_handle = tokio::spawn(async move {
        if let Err(e) = metrics_server.start().await {
            error!("Metrics server failed: {}", e);
        }
    });
    
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdown signal received (Ctrl+C)");
                shutdown_clone.notify_waiters();
            }
            Err(err) => {
                error!("Failed to listen for shutdown signal: {}", err);
            }
        }
    });
    
    // Initialize S3 client
    info!(
        endpoint = %config.s3.endpoint,
        region = %config.s3.region,
        "Initializing S3 client"
    );
    
    let base_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(config.s3.region.clone()))
        .credentials_provider(Credentials::new(
            &config.s3.access_key_id,
            &config.s3.secret_access_key,
            None,
            None,
            "static",
        ))
        .load()
        .await;
    
    // Build S3-specific config with endpoint and force path style
    let s3_config = S3ConfigBuilder::from(&base_config)
        .endpoint_url(&config.s3.endpoint)
        .force_path_style(true)  // Required for MinIO/LakeFS
        .build();
    
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    
    // Create channels
    info!("Setting up communication channels");
    let (message_tx, message_rx) = mpsc::channel(10000);
    let (batch_tx, batch_rx) = mpsc::channel(1000);
    
    // Spawn batch aggregator
    info!("Starting batch aggregator");
    let aggregator = BatchAggregator::new(
        config.s3.clone(),
        message_rx,
        batch_tx.clone(),
        shutdown.clone(),
    );
    let aggregator_handle = tokio::spawn(aggregator.run());
    
    // Resolve S3 upload worker count (may be auto-computed)
    let upload_worker_count = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker);
    let estimated_partitions = config.estimate_partition_count();
    
    // Spawn S3 upload workers with individual channels
    info!(
        worker_count = upload_worker_count,
        estimated_partitions = estimated_partitions,
        partitions_per_worker = config.s3.partitions_per_worker,
        mode = ?config.s3.upload_worker_count,
        "Starting S3 upload workers"
    );
    
    // Create individual channels for each uploader
    let mut uploader_channels = Vec::new();
    for _ in 0..upload_worker_count {
        let (tx, rx) = mpsc::channel(100);
        uploader_channels.push((tx, rx));
    }
    
    // Spawn dispatcher to distribute batches to uploaders (round-robin)
    let dispatcher_shutdown = shutdown.clone();
    let uploader_senders: Vec<_> = uploader_channels.iter().map(|(tx, _)| tx.clone()).collect();
    tokio::spawn(async move {
        let mut current_worker = 0;
        let mut batch_rx = batch_rx;
        
        loop {
            tokio::select! {
                biased;
                
                _ = dispatcher_shutdown.notified() => break,
                
                batch = batch_rx.recv() => {
                    match batch {
                        Some(batch) => {
                            let sender = &uploader_senders[current_worker];
                            if sender.send(batch).await.is_err() {
                                break;
                            }
                            current_worker = (current_worker + 1) % uploader_senders.len();
                        }
                        None => break,
                    }
                }
            }
        }
    });
    
    // Spawn S3 upload workers
    let mut uploader_handles = Vec::new();
    for (worker_id, (_, rx)) in uploader_channels.into_iter().enumerate() {
        let uploader = S3Uploader::new(
            worker_id,
            s3_client.clone(),
            config.s3.clone(),
            rx,
            shutdown.clone(),
        );
        uploader_handles.push(tokio::spawn(uploader.run()));
    }
    
    // Spawn exchange managers
    info!(exchanges = config.exchanges.len(), "Starting exchange managers");
    let mut exchange_handles = Vec::new();
    
    for exchange_config in config.exchanges {
        info!(
            exchange = %exchange_config.name,
            market_type = %exchange_config.market_type,
            mode = ?exchange_config.mode,
            "Starting exchange manager"
        );
        
        let pool_manager = WorkerPoolManager::new(
            exchange_config.clone(),
            message_tx.clone(),
            shutdown.clone(),
        );
        
        let symbol_manager = SymbolManager::new(
            exchange_config,
            pool_manager,
            shutdown.clone(),
        );
        
        exchange_handles.push(tokio::spawn(symbol_manager.run()));
    }
    
    info!("All components started, service is running");
    
    // Wait for shutdown signal
    shutdown.notified().await;
    
    // Graceful shutdown sequence
    info!("=== Starting graceful shutdown ===");
    
    // 1. Abort exchange managers (prevents new worker spawns)
    info!("Step 1: Aborting exchange managers");
    for handle in exchange_handles {
        handle.abort();
    }
    
    // 2. Give WebSocket workers time to drain
    info!("Step 2: Waiting for WebSocket workers to drain");
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 3. Close message channel to signal batch aggregator
    info!("Step 3: Closing message channel");
    drop(message_tx);
    
    // 4. Wait for batch aggregator
    info!("Step 4: Waiting for batch aggregator to complete");
    match tokio::time::timeout(Duration::from_secs(10), aggregator_handle).await {
        Ok(Ok(())) => info!("Batch aggregator completed successfully"),
        Ok(Err(e)) => error!("Batch aggregator task error: {}", e),
        Err(_) => {
            error!("Batch aggregator shutdown timed out");
        }
    }
    
    // 5. Close batch channel to signal uploaders
    info!("Step 5: Closing batch channel");
    drop(batch_tx);
    
    // 6. Wait for S3 uploaders
    info!(
        uploader_count = uploader_handles.len(),
        "Step 6: Waiting for S3 uploaders to complete"
    );
    for (idx, handle) in uploader_handles.into_iter().enumerate() {
        match tokio::time::timeout(Duration::from_secs(300), handle).await {
            Ok(Ok(())) => info!(uploader_id = idx, "S3 uploader completed successfully"),
            Ok(Err(e)) => error!(uploader_id = idx, "S3 uploader task error: {}", e),
            Err(_) => {
                error!(uploader_id = idx, "S3 uploader shutdown timed out");
            }
        }
    }
    
    // 7. Wait for metrics server
    info!("Step 7: Waiting for metrics server to stop");
    match tokio::time::timeout(Duration::from_secs(5), metrics_handle).await {
        Ok(Ok(())) => info!("Metrics server stopped successfully"),
        Ok(Err(e)) => error!("Metrics server task error: {}", e),
        Err(_) => {
            error!("Metrics server shutdown timed out");
        }
    }
    
    // Shutdown metrics
    if let Err(e) = metrics.shutdown() {
        error!("Failed to shutdown metrics: {}", e);
    }
    
    info!("=== Shutdown complete ===");
    Ok(())
}
