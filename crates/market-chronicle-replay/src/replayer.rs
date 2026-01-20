use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, TimeZone, Utc};
use market_chronicle_core::{types::MarketMessage, S3Config};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::Args;

pub struct Replayer {
    s3_client: S3Client,
    config: S3Config,
}

impl Replayer {
    pub async fn new(config: S3Config) -> Result<Self> {
        let s3_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(&config.endpoint)
            .region(Region::new(config.region.clone()))
            .credentials_provider(Credentials::new(
                &config.access_key_id,
                &config.secret_access_key,
                None,
                None,
                "static",
            ))
            .load()
            .await;
        
        let s3_client = aws_sdk_s3::Client::new(&s3_config);
        
        Ok(Self { s3_client, config })
    }
    
    pub async fn replay(&self, args: Args) -> Result<()> {
        // Parse time range
        let start_time = DateTime::parse_from_rfc3339(&args.start_time)
            .context("Invalid start_time format")?
            .with_timezone(&Utc);
        
        let end_time = DateTime::parse_from_rfc3339(&args.end_time)
            .context("Invalid end_time format")?
            .with_timezone(&Utc);
        
        info!(
            start = %start_time,
            end = %end_time,
            "Replay time range"
        );
        
        // List matching S3 objects
        let objects = self.list_objects(&args, &start_time, &end_time).await?;
        
        info!(files = objects.len(), "Found S3 objects to replay");
        
        if objects.is_empty() {
            warn!("No data found for specified filters");
            return Ok(());
        }
        
        // Open output file if specified
        let mut output_writer: Option<BufWriter<File>> = if let Some(output_path) = &args.output {
            let file = File::create(output_path)
                .with_context(|| format!("Failed to create output file: {}", output_path))?;
            Some(BufWriter::new(file))
        } else {
            None
        };
        
        let mut total_messages = 0u64;
        let replay_start = Instant::now();
        
        // Process each file
        for (idx, key) in objects.iter().enumerate() {
            info!(
                file = idx + 1,
                total = objects.len(),
                key = %key,
                "Processing file"
            );
            
            let messages = self.download_and_decompress(key).await?;
            
            if !args.stats_only {
                self.replay_messages(
                    messages,
                    args.speed,
                    &mut output_writer,
                    &mut total_messages,
                ).await?;
            } else {
                total_messages += messages.len() as u64;
            }
        }
        
        let elapsed = replay_start.elapsed();
        
        info!(
            total_messages = total_messages,
            elapsed_secs = elapsed.as_secs(),
            "Replay complete"
        );
        
        Ok(())
    }
    
    async fn list_objects(
        &self,
        args: &Args,
        start_time: &DateTime<Utc>,
        end_time: &DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let mut all_objects = Vec::new();
        
        // Build prefix based on filters
        let mut prefix = format!("{}/", self.config.prefix);
        
        if let Some(exchange) = &args.exchange {
            prefix.push_str(&format!("exchange={}/", exchange));
            
            // Could add more specific filtering, but for now we'll list and filter
        }
        
        info!(prefix = %prefix, bucket = %self.config.bucket, "Listing S3 objects");
        
        let mut continuation_token = None;
        
        loop {
            let mut request = self.s3_client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&prefix);
            
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            
            let response = request.send().await
                .context("Failed to list S3 objects")?;
            
            for object in response.contents() {
                if let Some(key) = object.key() {
                    // Filter by time range (parse from key path)
                    if self.matches_filters(key, args, start_time, end_time) {
                        all_objects.push(key.to_string());
                    }
                }
            }
            
            if !response.is_truncated().unwrap_or(false) {
                break;
            }
            
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        }
        
        all_objects.sort();
        Ok(all_objects)
    }
    
    fn matches_filters(
        &self,
        key: &str,
        args: &Args,
        start_time: &DateTime<Utc>,
        end_time: &DateTime<Utc>,
    ) -> bool {
        // Parse Hive-style partitioning
        // Format: prefix/exchange=X/market_type=Y/stream_type=Z/symbol=S/year=YYYY/month=MM/day=DD/hour=HH/file.ndjson.zst
        
        // Filter by exchange
        if let Some(exchange) = &args.exchange {
            if !key.contains(&format!("exchange={}/", exchange)) {
                return false;
            }
        }
        
        // Filter by symbol
        if let Some(symbol) = &args.symbol {
            if !key.contains(&format!("symbol={}/", symbol)) {
                return false;
            }
        }
        
        // Filter by stream type
        if let Some(stream_type) = &args.stream_type {
            if !key.contains(&format!("stream_type={}/", stream_type)) {
                return false;
            }
        }
        
        // Parse time from path
        if let Some(time) = self.parse_time_from_key(key) {
            if time < start_time.timestamp() || time > end_time.timestamp() {
                return false;
            }
        }
        
        true
    }
    
    fn parse_time_from_key(&self, key: &str) -> Option<i64> {
        // Extract year, month, day, hour from key
        let parts: Vec<&str> = key.split('/').collect();
        
        let mut year = None;
        let mut month = None;
        let mut day = None;
        let mut hour = None;
        
        for part in parts {
            if let Some(val) = part.strip_prefix("year=") {
                year = val.parse::<i32>().ok();
            } else if let Some(val) = part.strip_prefix("month=") {
                month = val.parse::<u32>().ok();
            } else if let Some(val) = part.strip_prefix("day=") {
                day = val.parse::<u32>().ok();
            } else if let Some(val) = part.strip_prefix("hour=") {
                hour = val.parse::<u32>().ok();
            }
        }
        
        if let (Some(y), Some(m), Some(d), Some(h)) = (year, month, day, hour) {
            if let Some(dt) = Utc.with_ymd_and_hms(y, m, d, h, 0, 0).earliest() {
                return Some(dt.timestamp());
            }
        }
        
        None
    }
    
    async fn download_and_decompress(&self, key: &str) -> Result<Vec<MarketMessage>> {
        // Download from S3
        let response = self.s3_client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("Failed to download S3 object: {}", key))?;
        
        let data = response.body.collect().await
            .context("Failed to read S3 object body")?;
        
        // Decompress
        let decompressed = zstd::decode_all(&data.into_bytes()[..])
            .context("Failed to decompress zstd data")?;
        
        // Parse NDJSON
        let text = String::from_utf8(decompressed)
            .context("Invalid UTF-8 in decompressed data")?;
        
        let mut messages = Vec::new();
        for (line_num, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            
            match serde_json::from_str::<MarketMessage>(line) {
                Ok(msg) => messages.push(msg),
                Err(e) => {
                    warn!(
                        key = %key,
                        line = line_num + 1,
                        "Failed to parse message: {}", e
                    );
                }
            }
        }
        
        Ok(messages)
    }
    
    async fn replay_messages(
        &self,
        mut messages: Vec<MarketMessage>,
        speed: f64,
        output_writer: &mut Option<BufWriter<File>>,
        total_count: &mut u64,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        
        // Sort by timestamp
        messages.sort_by_key(|m| m.timestamp);
        
        let mut last_ts = messages[0].timestamp;
        
        for msg in messages {
            // Calculate delay based on speed
            let delay_ms = ((msg.timestamp - last_ts) as f64 / speed) as u64;
            if delay_ms > 0 && speed > 0.0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            
            // Output message
            let json = serde_json::to_string(&msg)?;
            
            if let Some(writer) = output_writer {
                writeln!(writer, "{}", json)?;
            } else {
                println!("{}", json);
            }
            
            last_ts = msg.timestamp;
            *total_count += 1;
        }
        
        Ok(())
    }
}
