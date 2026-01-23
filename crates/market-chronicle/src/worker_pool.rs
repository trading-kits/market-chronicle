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
    
    /// Unsubscribe from streams across workers
    /// This is useful for cleaning up streams for delisted symbols in "all" mode
    /// Workers with no remaining streams will be terminated automatically
    pub async fn unsubscribe_streams(&mut self, streams_to_remove: Vec<String>) -> Result<()> {
        if streams_to_remove.is_empty() {
            return Ok(());
        }
        
        info!(
            exchange = %self.exchange_config.name,
            streams_to_remove = streams_to_remove.len(),
            "Unsubscribing from streams"
        );
        
        let streams_set: std::collections::HashSet<_> = streams_to_remove.iter().collect();
        let mut workers_to_terminate = Vec::new();
        
        // Find which workers have the streams we want to unsubscribe from
        for (idx, worker) in self.workers.iter_mut().enumerate() {
            let streams_in_worker: Vec<String> = worker.assigned_streams
                .iter()
                .filter(|s| streams_set.contains(s))
                .cloned()
                .collect();
            
            if !streams_in_worker.is_empty() {
                info!(
                    exchange = %self.exchange_config.name,
                    worker_id = worker.worker_id,
                    streams = streams_in_worker.len(),
                    "Sending unsubscribe command to worker"
                );
                
                // Send unsubscribe command
                if let Err(e) = worker.command_tx.send(WorkerCommand::Unsubscribe {
                    streams: streams_in_worker.clone(),
                }).await {
                    warn!(
                        worker_id = worker.worker_id,
                        error = %e,
                        "Failed to send unsubscribe command"
                    );
                } else {
                    // Remove streams from assigned_streams
                    worker.assigned_streams.retain(|s| !streams_set.contains(s));
                    
                    // Check if worker now has zero streams
                    if worker.assigned_streams.is_empty() {
                        info!(
                            exchange = %self.exchange_config.name,
                            worker_id = worker.worker_id,
                            "Worker has no streams remaining, marking for termination"
                        );
                        workers_to_terminate.push(idx);
                    }
                }
            }
        }
        
        // Terminate workers with no streams (in reverse order to maintain indices)
        if !workers_to_terminate.is_empty() {
            self.terminate_workers(workers_to_terminate).await;
        }
        
        Ok(())
    }
    
    /// Terminate specific workers by index
    /// Indices must be in ascending order
    async fn terminate_workers(&mut self, mut indices: Vec<usize>) {
        // Sort indices in descending order so removal doesn't affect subsequent indices
        indices.sort_unstable_by(|a, b| b.cmp(a));
        
        info!(
            exchange = %self.exchange_config.name,
            workers_to_terminate = indices.len(),
            "Terminating empty workers"
        );
        
        let timeout = Duration::from_secs(5);
        
        for idx in indices {
            if idx >= self.workers.len() {
                warn!(
                    exchange = %self.exchange_config.name,
                    idx = idx,
                    "Worker index out of bounds, skipping"
                );
                continue;
            }
            
            let worker = self.workers.remove(idx);
            let worker_id = worker.worker_id;
            
            info!(
                exchange = %self.exchange_config.name,
                worker_id = worker_id,
                "Terminating worker"
            );
            
            // Drop command_tx to signal worker to shut down
            // When the worker's command_rx receives None, it will exit gracefully
            drop(worker.command_tx);
            
            // Wait for worker to complete with timeout
            match tokio::time::timeout(timeout, worker.join_handle).await {
                Ok(Ok(())) => {
                    info!(
                        exchange = %self.exchange_config.name,
                        worker_id = worker_id,
                        "Worker terminated gracefully"
                    );
                }
                Ok(Err(e)) => {
                    warn!(
                        exchange = %self.exchange_config.name,
                        worker_id = worker_id,
                        error = %e,
                        "Worker panicked during termination"
                    );
                }
                Err(_) => {
                    warn!(
                        exchange = %self.exchange_config.name,
                        worker_id = worker_id,
                        "Worker termination timed out"
                    );
                    // Note: join_handle is dropped here, which cancels the task
                }
            }
        }
        
        info!(
            exchange = %self.exchange_config.name,
            remaining_workers = self.workers.len(),
            "Worker termination complete"
        );
    }
}
