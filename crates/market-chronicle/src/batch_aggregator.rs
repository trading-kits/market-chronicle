use market_chronicle_core::{
    config::S3Config,
    types::{Batch, MarketMessage, PartitionKey},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

pub struct BatchAggregator {
    config: S3Config,
    message_rx: mpsc::Receiver<MarketMessage>,
    batch_tx: mpsc::Sender<Batch>,
    active_batches: HashMap<PartitionKey, Batch>,
    shutdown: Arc<Notify>,
}

impl BatchAggregator {
    pub fn new(
        config: S3Config,
        message_rx: mpsc::Receiver<MarketMessage>,
        batch_tx: mpsc::Sender<Batch>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            message_rx,
            batch_tx,
            active_batches: HashMap::new(),
            shutdown,
        }
    }
    
    pub async fn run(mut self) {
        info!("Batch aggregator starting");
        
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);
        let mut timeout_check = tokio::time::interval(Duration::from_millis(100));
        
        let mut message_count = 0u64;
        let mut batch_count = 0u64;
        
        loop {
            tokio::select! {
                biased;
                
                _ = self.shutdown.notified() => {
                    info!(
                        messages_processed = message_count,
                        batches_created = batch_count,
                        "Batch aggregator shutting down, flushing remaining batches"
                    );
                    self.flush_all_batches().await;
                    break;
                }
                
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(market_msg) => {
                            message_count += 1;
                            if let Some(batch) = self.process_message(market_msg) {
                                batch_count += 1;
                                if self.batch_tx.send(batch).await.is_err() {
                                    warn!("Batch channel closed, exiting");
                                    break;
                                }
                            }
                        }
                        None => {
                            info!(
                                messages_processed = message_count,
                                batches_created = batch_count,
                                "Message channel closed, flushing and exiting"
                            );
                            self.flush_all_batches().await;
                            break;
                        }
                    }
                }
                
                _ = timeout_check.tick() => {
                    let flushed = self.flush_old_batches(batch_timeout).await;
                    if flushed > 0 {
                        batch_count += flushed as u64;
                    }
                }
            }
        }
        
        info!(
            messages_processed = message_count,
            batches_created = batch_count,
            "Batch aggregator stopped"
        );
    }
    
    fn process_message(&mut self, msg: MarketMessage) -> Option<Batch> {
        let partition_key = PartitionKey::from_message(&msg);
        
        let batch = self.active_batches.entry(partition_key.clone())
            .or_insert_with(|| Batch::with_capacity(
                partition_key.clone(), 
                Instant::now(),
                self.config.batch_size,
            ));
        
        batch.messages.push(msg);
        
        // Check if batch is full
        if batch.messages.len() >= self.config.batch_size {
            debug!(
                exchange = %batch.partition_key.exchange,
                market_type = %batch.partition_key.market_type,
                stream_type = %batch.partition_key.stream_type,
                symbol = %batch.partition_key.symbol,
                size = batch.messages.len(),
                "Batch full, flushing"
            );
            return self.active_batches.remove(&partition_key);
        }
        
        None
    }
    
    async fn flush_old_batches(&mut self, max_age: Duration) -> usize {
        let now = Instant::now();
        let mut to_flush = Vec::new();
        
        for (key, batch) in &self.active_batches {
            if now.duration_since(batch.created_at) >= max_age {
                to_flush.push(key.clone());
            }
        }
        
        let count = to_flush.len();
        
        for key in to_flush {
            if let Some(batch) = self.active_batches.remove(&key) {
                debug!(
                    exchange = %batch.partition_key.exchange,
                    market_type = %batch.partition_key.market_type,
                    stream_type = %batch.partition_key.stream_type,
                    symbol = %batch.partition_key.symbol,
                    size = batch.messages.len(),
                    age_ms = now.duration_since(batch.created_at).as_millis(),
                    "Batch timeout, flushing"
                );
                
                if self.batch_tx.send(batch).await.is_err() {
                    warn!("Batch channel closed during timeout flush");
                    break;
                }
            }
        }
        
        count
    }
    
    async fn flush_all_batches(&mut self) {
        let batch_count = self.active_batches.len();
        
        if batch_count > 0 {
            info!(remaining_batches = batch_count, "Flushing all remaining batches");
        }
        
        for (_, batch) in self.active_batches.drain() {
            debug!(
                exchange = %batch.partition_key.exchange,
                market_type = %batch.partition_key.market_type,
                stream_type = %batch.partition_key.stream_type,
                symbol = %batch.partition_key.symbol,
                size = batch.messages.len(),
                "Flushing batch on shutdown"
            );
            
            if self.batch_tx.send(batch).await.is_err() {
                warn!("Batch channel closed during final flush");
                break;
            }
        }
    }
}
