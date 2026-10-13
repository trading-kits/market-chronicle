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
        let initial_stream_count = streams.len();

        info!(
            exchange = %self.exchange_config.name,
            market_type = %self.exchange_config.market_type,
            total_streams = initial_stream_count,
            existing_workers = self.workers.len(),
            "Starting smart worker allocation"
        );

        let mut remaining_streams = streams;
        let mut streams_distributed_to_existing = 0;

        // Phase 1: Fill existing workers with available capacity
        if !self.workers.is_empty() {
            let mut streams_to_distribute: Vec<(usize, mpsc::Sender<WorkerCommand>, Vec<String>)> = Vec::new();

            for worker in &mut self.workers {
                if remaining_streams.is_empty() {
                    break;
                }

                let current_capacity = worker.assigned_streams.len();
                let available_capacity = streams_per_worker.saturating_sub(current_capacity);

                if available_capacity > 0 {
                    let streams_for_worker: Vec<String> = remaining_streams
                        .drain(..remaining_streams.len().min(available_capacity))
                        .collect();

                    let stream_count = streams_for_worker.len();
                    streams_distributed_to_existing += stream_count;

                    info!(
                        exchange = %self.exchange_config.name,
                        worker_id = worker.worker_id,
                        existing_streams = current_capacity,
                        new_streams = stream_count,
                        capacity_remaining = available_capacity - stream_count,
                        "Adding streams to existing worker"
                    );

                    // Update assigned_streams tracking
                    worker.assigned_streams.extend(streams_for_worker.iter().cloned());

                    streams_to_distribute.push((
                        worker.worker_id,
                        worker.command_tx.clone(),
                        streams_for_worker,
                    ));
                }
            }

            // Send subscribe commands to existing workers
            let max_batch_size = self.exchange_config.ws_max_batch_size;
            let delay_ms = self.exchange_config.ws_subscription_delay_ms;

            for (worker_id, cmd_tx, streams) in streams_to_distribute {
                let batches: Vec<_> = streams.chunks(max_batch_size).collect();
                let batch_count = batches.len();

                for (idx, batch) in batches.into_iter().enumerate() {
                    if let Err(e) = cmd_tx.send(WorkerCommand::Subscribe {
                        streams: batch.to_vec(),
                    }).await {
                        warn!(
                            exchange = %self.exchange_config.name,
                            worker_id = worker_id,
                            error = %e,
                            "Failed to send subscribe command to existing worker"
                        );
                    }

                    // Add delay between batches (except after the last one)
                    if delay_ms > 0 && idx < batch_count - 1 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }

            info!(
                exchange = %self.exchange_config.name,
                streams_distributed = streams_distributed_to_existing,
                streams_remaining = remaining_streams.len(),
                "Phase 1 complete: filled existing workers"
            );
        }

        // Phase 2: Spawn new workers for remaining streams
        let mut new_workers_spawned = 0;
        if !remaining_streams.is_empty() {
            for chunk in remaining_streams.chunks(streams_per_worker) {
                self.spawn_new_worker(chunk.to_vec()).await?;
                new_workers_spawned += 1;
            }

            info!(
                exchange = %self.exchange_config.name,
                new_workers = new_workers_spawned,
                "Phase 2 complete: spawned new workers"
            );
        }

        info!(
            exchange = %self.exchange_config.name,
            total_workers = self.workers.len(),
            streams_added = initial_stream_count,
            streams_to_existing = streams_distributed_to_existing,
            new_workers = new_workers_spawned,
            "Worker allocation complete"
        );

        Ok(())
    }

    /// Spawn a new worker with the given streams
    async fn spawn_new_worker(&mut self, streams: Vec<String>) -> Result<()> {
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
                return Err(market_chronicle_core::error::Error::Config(
                    format!("Unknown exchange: {}", self.exchange_config.name)
                ));
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
            assigned_streams: streams.clone(),
            join_handle: handle,
        });

        // Send initial subscribe commands, batched by ws_max_batch_size
        let max_batch_size = self.exchange_config.ws_max_batch_size;
        let delay_ms = self.exchange_config.ws_subscription_delay_ms;
        let mut total_subscribed = 0;
        let batches: Vec<_> = streams.chunks(max_batch_size).collect();
        let batch_count = batches.len();

        for (idx, batch) in batches.into_iter().enumerate() {
            let _ = cmd_tx.send(WorkerCommand::Subscribe {
                streams: batch.to_vec(),
            }).await;
            total_subscribed += batch.len();

            // Add delay between batches (except after the last one)
            if delay_ms > 0 && idx < batch_count - 1 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use market_chronicle_core::config::{ExchangeConfig, MarketType, SubscriptionMode};
    use tokio::sync::mpsc;

    fn create_test_config(streams_per_connection: usize) -> ExchangeConfig {
        ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://stream.binance.com:9443/stream".to_string(),
            rest_endpoint: "https://api.binance.com".to_string(),
            mode: SubscriptionMode::All,
            symbols: None,
            symbol_refresh_interval_s: Some(300),
            streams: vec!["trade".to_string()],
            kline_intervals: None,
            ws_connection_rate: 10,
            ws_streams_per_connection: streams_per_connection,
            ws_max_connections: 100,
            ws_max_batch_size: 50,
            ws_subscription_delay_ms: 0,
        }
    }

    fn create_test_manager(streams_per_connection: usize) -> WorkerPoolManager {
        let config = create_test_config(streams_per_connection);
        let (message_tx, _message_rx) = mpsc::channel(100);
        let shutdown = Arc::new(Notify::new());
        WorkerPoolManager::new(config, message_tx, shutdown)
    }

    #[tokio::test]
    async fn test_empty_streams_returns_ok() {
        let mut manager = create_test_manager(10);
        let result = manager.spawn_workers_for_streams(vec![]).await;
        assert!(result.is_ok());
        assert_eq!(manager.workers.len(), 0);
    }

    #[tokio::test]
    async fn test_spawn_workers_from_empty_pool() {
        let mut manager = create_test_manager(10);
        let streams: Vec<String> = (0..5).map(|i| format!("stream_{}", i)).collect();

        let result = manager.spawn_workers_for_streams(streams.clone()).await;
        assert!(result.is_ok());

        // Should spawn 1 worker with 5 streams
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 5);
        assert_eq!(manager.workers[0].assigned_streams, streams);
    }

    #[tokio::test]
    async fn test_spawn_multiple_workers_when_exceeds_capacity() {
        let mut manager = create_test_manager(10);
        let streams: Vec<String> = (0..25).map(|i| format!("stream_{}", i)).collect();

        let result = manager.spawn_workers_for_streams(streams).await;
        assert!(result.is_ok());

        // Should spawn 3 workers: 10 + 10 + 5
        assert_eq!(manager.workers.len(), 3);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10);
        assert_eq!(manager.workers[1].assigned_streams.len(), 10);
        assert_eq!(manager.workers[2].assigned_streams.len(), 5);
    }

    #[tokio::test]
    async fn test_smart_allocation_reuses_existing_worker() {
        let mut manager = create_test_manager(10);

        // First allocation: 5 streams -> Worker 0
        let first_streams: Vec<String> = (0..5).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(first_streams).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 5);

        // Second allocation: 3 more streams -> should go to Worker 0 (has capacity for 5 more)
        let second_streams: Vec<String> = (5..8).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(second_streams).await.unwrap();

        // Should still have 1 worker with 8 streams
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 8);

        // Verify all streams are present
        let expected_streams: Vec<String> = (0..8).map(|i| format!("stream_{}", i)).collect();
        assert_eq!(manager.workers[0].assigned_streams, expected_streams);
    }

    #[tokio::test]
    async fn test_smart_allocation_fills_then_spawns() {
        let mut manager = create_test_manager(10);

        // First allocation: 8 streams -> Worker 0 (capacity: 2 remaining)
        let first_streams: Vec<String> = (0..8).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(first_streams).await.unwrap();
        assert_eq!(manager.workers.len(), 1);

        // Second allocation: 5 streams
        // Should fill Worker 0 (2 streams) and spawn Worker 1 (3 streams)
        let second_streams: Vec<String> = (8..13).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(second_streams).await.unwrap();

        assert_eq!(manager.workers.len(), 2);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[1].assigned_streams.len(), 3);

        // Total streams: 13
        let total_streams: usize = manager.workers.iter()
            .map(|w| w.assigned_streams.len())
            .sum();
        assert_eq!(total_streams, 13);
    }

    #[tokio::test]
    async fn test_smart_allocation_with_full_worker() {
        let mut manager = create_test_manager(10);

        // Fill Worker 0 completely
        let first_streams: Vec<String> = (0..10).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(first_streams).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10);

        // Try to add more streams - should spawn new worker
        let second_streams: Vec<String> = (10..15).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(second_streams).await.unwrap();

        assert_eq!(manager.workers.len(), 2);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10); // Still full
        assert_eq!(manager.workers[1].assigned_streams.len(), 5);  // New worker
    }

    #[tokio::test]
    async fn test_smart_allocation_distributes_across_multiple_workers() {
        let mut manager = create_test_manager(10);

        // With smart allocation, workers fill up sequentially
        // First spawn: 7 streams -> Worker 0 (7 streams, 3 capacity)
        manager.spawn_workers_for_streams((0..7).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 7);

        // Second spawn: 5 streams -> Worker 0 gets 3 (now 10), Worker 1 gets 2 (2 streams, 8 capacity)
        manager.spawn_workers_for_streams((7..12).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 2);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[1].assigned_streams.len(), 2);

        // Third spawn: 10 streams -> Worker 1 gets 8 (now 10), Worker 2 gets 2
        manager.spawn_workers_for_streams((12..22).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 3);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[1].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[2].assigned_streams.len(), 2);

        // Fourth spawn: 15 streams
        // Worker 2 gets 8 (now 10), Worker 3 gets 7
        let new_streams: Vec<String> = (22..37).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(new_streams).await.unwrap();

        assert_eq!(manager.workers.len(), 4);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[1].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[2].assigned_streams.len(), 10); // Full
        assert_eq!(manager.workers[3].assigned_streams.len(), 7);  // New worker with remaining

        // Verify total streams
        let total_streams: usize = manager.workers.iter()
            .map(|w| w.assigned_streams.len())
            .sum();
        assert_eq!(total_streams, 37);
    }

    #[tokio::test]
    async fn test_smart_allocation_single_stream() {
        let mut manager = create_test_manager(10);

        // Add 9 streams to Worker 0
        manager.spawn_workers_for_streams((0..9).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 1);

        // Add single stream - should go to existing worker
        manager.spawn_workers_for_streams(vec!["stream_9".to_string()]).await.unwrap();

        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 10);
    }

    #[tokio::test]
    async fn test_capacity_calculation() {
        let mut manager = create_test_manager(100);

        // Add 75 streams
        manager.spawn_workers_for_streams((0..75).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 75);

        // Available capacity should be 25
        let capacity = manager.exchange_config.ws_streams_per_connection
            - manager.workers[0].assigned_streams.len();
        assert_eq!(capacity, 25);

        // Add exactly 25 more streams
        manager.spawn_workers_for_streams((75..100).map(|i| format!("stream_{}", i)).collect()).await.unwrap();

        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 100); // Exactly full

        // Add one more - should spawn new worker
        manager.spawn_workers_for_streams(vec!["stream_100".to_string()]).await.unwrap();

        assert_eq!(manager.workers.len(), 2);
        assert_eq!(manager.workers[1].assigned_streams.len(), 1);
    }

    #[tokio::test]
    async fn test_no_stream_migration() {
        let mut manager = create_test_manager(10);

        // Initial allocation
        let initial_streams: Vec<String> = (0..5).map(|i| format!("stream_{}", i)).collect();
        manager.spawn_workers_for_streams(initial_streams.clone()).await.unwrap();

        let worker_0_streams_before = manager.workers[0].assigned_streams.clone();

        // Add more streams
        manager.spawn_workers_for_streams((5..8).map(|i| format!("stream_{}", i)).collect()).await.unwrap();

        // Original streams should still be in Worker 0, in the same order
        assert!(manager.workers[0].assigned_streams.starts_with(&worker_0_streams_before));

        // Worker 0 should have all 8 streams now
        assert_eq!(manager.workers[0].assigned_streams.len(), 8);
    }

    #[tokio::test]
    async fn test_large_allocation_scenario() {
        let mut manager = create_test_manager(1024);

        // Simulate Binance Spot scenario
        // Initial: 1000 streams
        manager.spawn_workers_for_streams((0..1000).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 1000);

        // Discovery: 15 new streams (should fit in Worker 0)
        manager.spawn_workers_for_streams((1000..1015).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 1);
        assert_eq!(manager.workers[0].assigned_streams.len(), 1015);

        // Discovery: 20 more streams (9 fit in Worker 0, 11 need new worker)
        manager.spawn_workers_for_streams((1015..1035).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.workers.len(), 2);
        assert_eq!(manager.workers[0].assigned_streams.len(), 1024); // Full
        assert_eq!(manager.workers[1].assigned_streams.len(), 11);

        // Verify no capacity wasted in Worker 0
        let capacity_0 = 1024 - manager.workers[0].assigned_streams.len();
        assert_eq!(capacity_0, 0);
    }

    #[tokio::test]
    async fn test_worker_count_method() {
        let mut manager = create_test_manager(10);
        assert_eq!(manager.worker_count(), 0);

        manager.spawn_workers_for_streams((0..5).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.worker_count(), 1);

        manager.spawn_workers_for_streams((5..15).map(|i| format!("stream_{}", i)).collect()).await.unwrap();
        assert_eq!(manager.worker_count(), 2);
    }

    #[test]
    fn test_exchange_config_creation() {
        let config = create_test_config(100);
        assert_eq!(config.ws_streams_per_connection, 100);
        assert_eq!(config.name, "binance");
        assert_eq!(config.market_type, MarketType::Spot);
    }
}
