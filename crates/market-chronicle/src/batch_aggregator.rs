use market_chronicle_core::{
    config::S3Config,
    types::{Batch, MarketMessage, PartitionKey},
};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
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
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout_ms,
            "Batch aggregator starting"
        );
        
        // Initialize metrics (OpenTelemetry will add _total suffix to counters)
        let meter = opentelemetry::global::meter("batch_aggregator");
        let messages_processed = meter
            .u64_counter("batch_messages_processed")
            .with_description("Total messages processed into batches")
            .init();
        let batches_created = meter
            .u64_counter("batch_created")
            .with_description("Total batches created")
            .init();
        let batch_size_histogram = meter
            .u64_histogram("batch_size_messages")
            .with_description("Distribution of batch sizes in messages")
            .init();
        let batch_active_gauge = meter
            .i64_up_down_counter("batch_active")
            .with_description("Number of active batches being aggregated")
            .init();
        
        // Initialize gauge to 0
        batch_active_gauge.add(0, &[]);
        
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);
        let mut timeout_check = tokio::time::interval(Duration::from_millis(100));
        let mut stats_interval = tokio::time::interval(Duration::from_secs(60));
        
        let mut message_count = 0u64;
        let mut batch_count = 0u64;
        let mut last_message_count = 0u64;
        let mut last_batch_count = 0u64;
        
        loop {
            tokio::select! {
                biased;
                
                _ = self.shutdown.notified() => {
                    info!(
                        messages_processed = message_count,
                        batches_created = batch_count,
                        "Batch aggregator shutting down, flushing remaining batches"
                    );
                    self.flush_all_batches(&batches_created, &batch_size_histogram).await;
                    break;
                }
                
                msg = self.message_rx.recv() => {
                    match msg {
                        Some(market_msg) => {
                            message_count += 1;
                            messages_processed.add(1, &[
                                KeyValue::new("exchange", market_msg.exchange.clone()),
                                KeyValue::new("stream_type", market_msg.stream_type.to_string()),
                            ]);
                            
                            let batches_before = self.active_batches.len();
                            if let Some(batch) = self.process_message(market_msg) {
                                batch_count += 1;
                                record_flushed_batch(&batch, &batches_created, &batch_size_histogram);
                                
                                // Update gauge: batch was removed
                                let batches_after = self.active_batches.len();
                                batch_active_gauge.add((batches_after as i64) - (batches_before as i64), &[]);
                                
                                if self.batch_tx.send(batch).await.is_err() {
                                    warn!("Batch channel closed, exiting");
                                    break;
                                }
                            } else {
                                // Update gauge: potentially new batch was created
                                let batches_after = self.active_batches.len();
                                if batches_after > batches_before {
                                    batch_active_gauge.add((batches_after as i64) - (batches_before as i64), &[]);
                                }
                            }
                        }
                        None => {
                            info!(
                                messages_processed = message_count,
                                batches_created = batch_count,
                                "Message channel closed, flushing and exiting"
                            );
                            self.flush_all_batches(&batches_created, &batch_size_histogram).await;
                            break;
                        }
                    }
                }
                
                _ = timeout_check.tick() => {
                    let batches_before = self.active_batches.len();
                    let flushed = self.flush_old_batches(
                        batch_timeout,
                        &batches_created,
                        &batch_size_histogram,
                    ).await;
                    if flushed > 0 {
                        batch_count += flushed as u64;
                        let batches_after = self.active_batches.len();
                        batch_active_gauge.add((batches_after as i64) - (batches_before as i64), &[]);
                        debug!(
                            batches_flushed = flushed,
                            reason = "timeout",
                            "Flushed batches due to timeout"
                        );
                    }
                }
                
                _ = stats_interval.tick() => {
                    let messages_since_last = message_count - last_message_count;
                    let batches_since_last = batch_count - last_batch_count;
                    let msg_per_sec = messages_since_last as f64 / 60.0;
                    let avg_batch_size = if batches_since_last > 0 {
                        messages_since_last as f64 / batches_since_last as f64
                    } else {
                        0.0
                    };
                    
                    // Count unique partition types
                    let mut partition_types = std::collections::HashMap::new();
                    for key in self.active_batches.keys() {
                        let partition_type = format!("{}:{}:{}", key.exchange, key.market_type, key.stream_type);
                        *partition_types.entry(partition_type).or_insert(0) += 1;
                    }
                    
                    let partition_summary = if partition_types.len() <= 5 {
                        // Show all if 5 or fewer
                        partition_types.iter()
                            .map(|(k, v)| format!("{} ({})", k, v))
                            .collect::<Vec<_>>()
                            .join(", ")
                    } else {
                        // Show first 5 and indicate there are more
                        let shown: Vec<String> = partition_types.iter()
                            .take(5)
                            .map(|(k, v)| format!("{} ({})", k, v))
                            .collect();
                        format!("{}, ... and {} more", shown.join(", "), partition_types.len() - 5)
                    };
                    
                    info!(
                        total_messages = message_count,
                        total_batches = batch_count,
                        messages_last_min = messages_since_last,
                        batches_last_min = batches_since_last,
                        msg_per_sec = format!("{:.2}", msg_per_sec),
                        avg_batch_size = format!("{:.1}", avg_batch_size),
                        active_batches = self.active_batches.len(),
                        partition_types = partition_types.len(),
                        partitions = partition_summary,
                        "Batch aggregator statistics"
                    );
                    
                    last_message_count = message_count;
                    last_batch_count = batch_count;
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
    
    async fn flush_old_batches(
        &mut self,
        max_age: Duration,
        batches_created: &Counter<u64>,
        batch_size_histogram: &Histogram<u64>,
    ) -> usize {
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
                
                record_flushed_batch(&batch, batches_created, batch_size_histogram);
                if self.batch_tx.send(batch).await.is_err() {
                    warn!("Batch channel closed during timeout flush");
                    break;
                }
            }
        }
        
        count
    }
    
    async fn flush_all_batches(
        &mut self,
        batches_created: &Counter<u64>,
        batch_size_histogram: &Histogram<u64>,
    ) {
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
            
            record_flushed_batch(&batch, batches_created, batch_size_histogram);
            if self.batch_tx.send(batch).await.is_err() {
                warn!("Batch channel closed during final flush");
                break;
            }
        }
    }
}

fn record_flushed_batch(
    batch: &Batch,
    batches_created: &Counter<u64>,
    batch_size_histogram: &Histogram<u64>,
) {
    let attributes = [
        KeyValue::new("exchange", batch.partition_key.exchange.clone()),
        KeyValue::new("stream_type", batch.partition_key.stream_type.to_string()),
    ];
    batches_created.add(1, &attributes);
    batch_size_histogram.record(batch.messages.len() as u64, &attributes);
}
