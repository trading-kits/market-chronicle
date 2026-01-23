use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Datelike, Timelike};
use market_chronicle_core::{
    config::S3Config,
    types::Batch,
};
use opentelemetry::KeyValue;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

pub struct S3Uploader {
    worker_id: usize,
    s3_client: S3Client,
    config: S3Config,
    batch_rx: mpsc::Receiver<Batch>,
    shutdown: Arc<Notify>,
}

impl S3Uploader {
    pub fn new(
        worker_id: usize,
        s3_client: S3Client,
        config: S3Config,
        batch_rx: mpsc::Receiver<Batch>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            worker_id,
            s3_client,
            config,
            batch_rx,
            shutdown,
        }
    }
    
    pub async fn run(mut self) {
        info!(
            worker_id = self.worker_id,
            bucket = %self.config.bucket,
            compression_level = self.config.compression_level,
            "S3 uploader starting"
        );
        
        // Initialize metrics (OpenTelemetry will add _total suffix to counters)
        let meter = opentelemetry::global::meter("s3_uploader");
        let uploads_total = meter
            .u64_counter("s3_uploads")
            .with_description("Successful S3 uploads")
            .init();
        let upload_failures = meter
            .u64_counter("s3_upload_failures")
            .with_description("Failed S3 uploads")
            .init();
        let upload_duration = meter
            .f64_histogram("s3_upload_duration_seconds")
            .with_description("S3 upload latency distribution")
            .init();
        let upload_bytes = meter
            .u64_counter("s3_upload_bytes")
            .with_description("Total bytes uploaded (compressed)")
            .init();
        let compression_ratio = meter
            .f64_histogram("s3_compression_ratio")
            .with_description("Compression ratio distribution")
            .init();
        
        let worker_id_str = self.worker_id.to_string();
        
        let mut upload_count = 0u64;
        let mut total_messages = 0u64;
        let mut total_bytes_compressed = 0u64;
        let mut total_bytes_uncompressed = 0u64;
        let mut stats_interval = tokio::time::interval(Duration::from_secs(60));
        let mut last_upload_count = 0u64;
        let mut last_message_count = 0u64;
        
        loop {
            tokio::select! {
                biased;
                
                _ = self.shutdown.notified() => {
                    info!(
                        worker_id = self.worker_id,
                        uploads = upload_count,
                        messages = total_messages,
                        "S3 uploader shutting down, processing remaining batches"
                    );
                    let (remaining_uploads, remaining_messages) = self.drain_remaining_batches(Duration::from_secs(30)).await;
                    upload_count += remaining_uploads;
                    total_messages += remaining_messages;
                    break;
                }
                
                batch = self.batch_rx.recv() => {
                    match batch {
                        Some(batch) => {
                            let message_count = batch.messages.len() as u64;
                            total_messages += message_count;
                            
                            // Calculate uncompressed size
                            let uncompressed_size = serde_json::to_vec(&batch.messages)
                                .map(|v| v.len() as u64)
                                .unwrap_or(0);
                            total_bytes_uncompressed += uncompressed_size;
                            
                            let start = Instant::now();
                            let exchange = batch.partition_key.exchange.clone();
                            let stream_type = batch.partition_key.stream_type.to_string();
                            
                            match self.upload_batch(batch).await {
                                Ok(compressed_size) => {
                                    upload_count += 1;
                                    total_bytes_compressed += compressed_size;
                                    
                                    let duration = start.elapsed().as_secs_f64();
                                    let ratio = compressed_size as f64 / uncompressed_size as f64;
                                    
                                    uploads_total.add(1, &[
                                        KeyValue::new("worker_id", worker_id_str.clone()),
                                        KeyValue::new("exchange", exchange.clone()),
                                        KeyValue::new("stream_type", stream_type.clone()),
                                    ]);
                                    upload_duration.record(duration, &[
                                        KeyValue::new("worker_id", worker_id_str.clone()),
                                    ]);
                                    upload_bytes.add(compressed_size, &[
                                        KeyValue::new("worker_id", worker_id_str.clone()),
                                        KeyValue::new("exchange", exchange.clone()),
                                    ]);
                                    compression_ratio.record(ratio, &[
                                        KeyValue::new("exchange", exchange.clone()),
                                        KeyValue::new("stream_type", stream_type),
                                    ]);
                                }
                                Err(()) => {
                                    upload_failures.add(1, &[
                                        KeyValue::new("worker_id", worker_id_str.clone()),
                                        KeyValue::new("exchange", exchange),
                                        KeyValue::new("error_type", "upload_failed"),
                                    ]);
                                }
                            }
                        }
                        None => {
                            info!(
                                worker_id = self.worker_id,
                                uploads = upload_count,
                                messages = total_messages,
                                "Batch channel closed, exiting"
                            );
                            break;
                        }
                    }
                }
                
                _ = stats_interval.tick() => {
                    let uploads_since_last = upload_count - last_upload_count;
                    let messages_since_last = total_messages - last_message_count;
                    let uploads_per_min = uploads_since_last as f64;
                    let compression_ratio = if total_bytes_compressed > 0 {
                        total_bytes_uncompressed as f64 / total_bytes_compressed as f64
                    } else {
                        0.0
                    };
                    
                    info!(
                        worker_id = self.worker_id,
                        total_uploads = upload_count,
                        total_messages = total_messages,
                        uploads_last_min = uploads_since_last,
                        messages_last_min = messages_since_last,
                        uploads_per_min = format!("{:.1}", uploads_per_min),
                        compressed_mb = format!("{:.2}", total_bytes_compressed as f64 / 1024.0 / 1024.0),
                        uncompressed_mb = format!("{:.2}", total_bytes_uncompressed as f64 / 1024.0 / 1024.0),
                        compression_ratio = format!("{:.2}x", compression_ratio),
                        "S3 uploader statistics"
                    );
                    
                    last_upload_count = upload_count;
                    last_message_count = total_messages;
                }
            }
        }
        
        info!(
            worker_id = self.worker_id,
            uploads = upload_count,
            messages = total_messages,
            "S3 uploader stopped"
        );
    }
    
    async fn upload_batch(&self, batch: Batch) -> Result<u64, ()> {
        let key = self.build_s3_key(&batch);
        let message_count = batch.messages.len();
        
        debug!(
            worker_id = self.worker_id,
            exchange = %batch.partition_key.exchange,
            market_type = %batch.partition_key.market_type,
            stream_type = %batch.partition_key.stream_type,
            symbol = %batch.partition_key.symbol,
            messages = message_count,
            key = %key,
            "Compressing and uploading batch"
        );
        
        let compressed_data = match self.compress_batch(&batch) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    worker_id = self.worker_id,
                    key = %key,
                    error = %e,
                    "Failed to compress batch"
                );
                return Err(());
            }
        };
        
        let compressed_size = compressed_data.len() as u64;
        
        // Retry logic with exponential backoff
        let mut backoff = Duration::from_secs(1);
        let max_retries = 5;
        
        for attempt in 0..max_retries {
            match self.s3_client
                .put_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .body(ByteStream::from(compressed_data.clone()))
                .content_type("application/x-ndjson")
                .content_encoding("zstd")
                .send()
                .await
            {
                Ok(_) => {
                    debug!(
                        worker_id = self.worker_id,
                        key = %key,
                        size_bytes = compressed_size,
                        messages = message_count,
                        exchange = %batch.partition_key.exchange,
                        symbol = %batch.partition_key.symbol,
                        stream_type = %batch.partition_key.stream_type,
                        "Successfully uploaded batch to S3"
                    );
                    return Ok(compressed_size);
                }
                Err(e) => {
                    warn!(
                        worker_id = self.worker_id,
                        key = %key,
                        attempt = attempt + 1,
                        max_retries = max_retries,
                        error = %e,
                        "S3 upload failed, retrying"
                    );
                    
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
        }
        
        error!(
            worker_id = self.worker_id,
            key = %key,
            messages = message_count,
            exchange = %batch.partition_key.exchange,
            symbol = %batch.partition_key.symbol,
            "Failed to upload batch after {} retries", max_retries
        );
        
        Err(())
    }
    
    fn build_s3_key(&self, batch: &Batch) -> String {
        let dt = chrono::DateTime::from_timestamp(batch.partition_key.hour / 1000, 0)
            .expect("Invalid timestamp");
        let uuid = uuid::Uuid::new_v4();
        
        format!(
            "{}/exchange={}/market-type={}/stream-type={}/symbol={}/year={:04}/month={:02}/day={:02}/hour={:02}/{}.ndjson.zst",
            self.config.prefix,
            batch.partition_key.exchange,
            batch.partition_key.market_type,
            batch.partition_key.stream_type,
            batch.partition_key.symbol,
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            uuid
        )
    }
    
    fn compress_batch(&self, batch: &Batch) -> Result<Vec<u8>, std::io::Error> {
        // Serialize to NDJSON
        let mut ndjson = Vec::new();
        for msg in &batch.messages {
            serde_json::to_writer(&mut ndjson, msg)?;
            ndjson.push(b'\n');
        }
        
        // Compress with zstd
        let mut encoder = zstd::Encoder::new(Vec::new(), self.config.compression_level)?;
        encoder.write_all(&ndjson)?;
        encoder.finish()
    }
    
    async fn drain_remaining_batches(&mut self, timeout: Duration) -> (u64, u64) {
        let deadline = Instant::now() + timeout;
        let mut upload_count = 0u64;
        let mut message_count = 0u64;
        
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(100), self.batch_rx.recv()).await {
                Ok(Some(batch)) => {
                    message_count += batch.messages.len() as u64;
                    if self.upload_batch(batch).await.is_ok() {
                        upload_count += 1;
                    }
                }
                _ => break,
            }
        }
        
        if upload_count > 0 {
            info!(
                worker_id = self.worker_id,
                uploads = upload_count,
                messages = message_count,
                "Processed remaining batches during shutdown"
            );
        }
        
        (upload_count, message_count)
    }
}
