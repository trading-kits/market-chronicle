use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Datelike, Timelike};
use market_chronicle_core::{
    config::S3Config,
    types::Batch,
};
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
        info!(worker_id = self.worker_id, "S3 uploader starting");
        
        let mut upload_count = 0u64;
        let mut total_messages = 0u64;
        
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
                            total_messages += batch.messages.len() as u64;
                            if self.upload_batch(batch).await.is_ok() {
                                upload_count += 1;
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
            }
        }
        
        info!(
            worker_id = self.worker_id,
            uploads = upload_count,
            messages = total_messages,
            "S3 uploader stopped"
        );
    }
    
    async fn upload_batch(&self, batch: Batch) -> Result<(), ()> {
        let key = self.build_s3_key(&batch);
        
        debug!(
            worker_id = self.worker_id,
            exchange = %batch.partition_key.exchange,
            market_type = %batch.partition_key.market_type,
            stream_type = %batch.partition_key.stream_type,
            symbol = %batch.partition_key.symbol,
            messages = batch.messages.len(),
            "Compressing and uploading batch"
        );
        
        let compressed_data = match self.compress_batch(&batch) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    worker_id = self.worker_id,
                    key = %key,
                    "Failed to compress batch: {}", e
                );
                return Err(());
            }
        };
        
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
                        size_bytes = compressed_data.len(),
                        messages = batch.messages.len(),
                        "Uploaded batch successfully"
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        worker_id = self.worker_id,
                        key = %key,
                        attempt = attempt + 1,
                        "Upload failed: {}", e
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
            messages = batch.messages.len(),
            "Failed to upload batch after {} retries", max_retries
        );
        
        Err(())
    }
    
    fn build_s3_key(&self, batch: &Batch) -> String {
        let dt = chrono::DateTime::from_timestamp(batch.partition_key.hour / 1000, 0)
            .expect("Invalid timestamp");
        let uuid = uuid::Uuid::new_v4();
        
        format!(
            "{}/exchange={}/market_type={}/stream_type={}/symbol={}/year={:04}/month={:02}/day={:02}/hour={:02}/{}.ndjson.zst",
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
