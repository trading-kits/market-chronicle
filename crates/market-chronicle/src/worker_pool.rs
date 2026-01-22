use market_chronicle_core::{
    config::ExchangeConfig,
    error::Result,
    rate_limit::ConnectionRateLimiter,
    types::MarketMessage,
    websocket::{ExchangeWebSocket, binance::BinanceWebSocket, bybit::BybitWebSocket},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::worker::{WebSocketWorker, WorkerCommand};

pub struct WorkerPoolManager {
    exchange_config: ExchangeConfig,
    workers: Vec<WorkerHandle>,
    rate_limiter: ConnectionRateLimiter,
    message_tx: mpsc::Sender<MarketMessage>,
    shutdown: Arc<Notify>,
}

struct WorkerHandle {
    worker_id: usize,
    command_tx: mpsc::Sender<WorkerCommand>,
    assigned_streams: Vec<String>,
    join_handle: JoinHandle<()>,
}

impl WorkerPoolManager {
    pub fn new(
        exchange_config: ExchangeConfig,
        message_tx: mpsc::Sender<MarketMessage>,
        shutdown: Arc<Notify>,
    ) -> Self {
        let rate_limiter = ConnectionRateLimiter::new(exchange_config.ws_connection_rate);
        
        Self {
            exchange_config,
            workers: Vec::new(),
            rate_limiter,
            message_tx,
            shutdown,
        }
    }
    
    pub async fn spawn_workers_for_streams(&mut self, streams: Vec<String>) -> Result<()> {
        if streams.is_empty() {
            return Ok(());
        }
        
        let streams_per_worker = self.exchange_config.ws_streams_per_connection;
        
        info!(
            exchange = %self.exchange_config.name,
            market_type = %self.exchange_config.market_type,
            total_streams = streams.len(),
            streams_per_worker = streams_per_worker,
            "Spawning workers for streams"
        );
        
        for chunk in streams.chunks(streams_per_worker) {
            // Wait for rate limit permit before creating connection
            self.rate_limiter.wait_for_permit().await;
            
            let worker_id = self.workers.len();
            let (cmd_tx, cmd_rx) = mpsc::channel(100);
            
            // Create exchange-specific WebSocket client
            let ws_client: Box<dyn ExchangeWebSocket> = match self.exchange_config.name.as_str() {
                "binance" => Box::new(BinanceWebSocket::new(
                    self.exchange_config.ws_endpoint.clone(),
                    self.exchange_config.market_type,
                )),
                "bybit" => Box::new(BybitWebSocket::new(
                    self.exchange_config.ws_endpoint.clone(),
                    self.exchange_config.market_type,
                )),
                _ => {
                    warn!("Unknown exchange: {}", self.exchange_config.name);
                    continue;
                }
            };
            
            let worker = WebSocketWorker::new(
                worker_id,
                self.exchange_config.name.clone(),
                self.exchange_config.market_type,
                ws_client,
                self.message_tx.clone(),
                cmd_rx,
                self.shutdown.clone(),
            );
            
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            
            self.workers.push(WorkerHandle {
                worker_id,
                command_tx: cmd_tx.clone(),
                assigned_streams: chunk.to_vec(),
                join_handle: handle,
            });
            
            // Send initial subscribe commands, batched by ws_max_batch_size
            let max_batch_size = self.exchange_config.ws_max_batch_size;
            let delay_ms = self.exchange_config.ws_subscription_delay_ms;
            let mut total_subscribed = 0;
            let batches: Vec<_> = chunk.chunks(max_batch_size).collect();
            let batch_count = batches.len();
            
            for (idx, batch) in batches.into_iter().enumerate() {
                let _ = cmd_tx.send(WorkerCommand::Subscribe {
                    streams: batch.to_vec(),
                }).await;
                total_subscribed += batch.len();
                
                // Add delay between batches (except after the last one)
                if delay_ms > 0 && idx < batch_count - 1 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
            
            info!(
                exchange = %self.exchange_config.name,
                worker_id = worker_id,
                streams = total_subscribed,
                batches = batch_count,
                delay_ms = delay_ms,
                "Worker spawned and subscribed"
            );
        }
        
        info!(
            exchange = %self.exchange_config.name,
            total_workers = self.workers.len(),
            "All workers spawned"
        );
        
        Ok(())
    }
    
    pub async fn shutdown(&mut self) {
        info!(
            exchange = %self.exchange_config.name,
            workers = self.workers.len(),
            "Shutting down worker pool"
        );
        
        self.shutdown.notify_waiters();
        
        // Wait for all workers with timeout
        let timeout = Duration::from_secs(10);
        for worker in &mut self.workers {
            match tokio::time::timeout(timeout, &mut worker.join_handle).await {
                Ok(_) => {
                    info!(worker_id = worker.worker_id, "Worker stopped gracefully");
                }
                Err(_) => {
                    warn!(worker_id = worker.worker_id, "Worker shutdown timed out");
                    worker.join_handle.abort();
                }
            }
        }
        
        info!(exchange = %self.exchange_config.name, "Worker pool shutdown complete");
    }
    
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}
