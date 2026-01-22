use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub s3: S3Config,
    pub exchanges: Vec<ExchangeConfig>,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Failed to read config file: {}", e)))?;
        
        let config: Config = toml::from_str(&contents)
            .map_err(|e| Error::Config(format!("Failed to parse config: {}", e)))?;
        
        config.validate()?;
        Ok(config)
    }
    
    fn validate(&self) -> Result<()> {
        // Validate S3 config
        if self.s3.bucket.is_empty() {
            return Err(Error::Config("S3 bucket cannot be empty".to_string()));
        }
        
        let resolved_worker_count = self.s3.upload_worker_count.resolve(&self.exchanges, self.s3.partitions_per_worker);
        if resolved_worker_count == 0 {
            return Err(Error::Config("S3 upload_worker_count must be > 0".to_string()));
        }
        
        if self.s3.partitions_per_worker == 0 {
            return Err(Error::Config("S3 partitions_per_worker must be > 0".to_string()));
        }
        
        if self.s3.batch_size == 0 {
            return Err(Error::Config("S3 batch_size must be > 0".to_string()));
        }
        
        // Validate each exchange config
        for exchange in &self.exchanges {
            exchange.validate()?;
        }
        
        Ok(())
    }
    
    /// Calculate the total expected number of unique partitions across all exchanges
    pub fn estimate_partition_count(&self) -> usize {
        self.exchanges.iter()
            .map(|ex| ex.estimate_partition_count())
            .sum()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default = "default_upload_worker_count")]
    pub upload_worker_count: WorkerCount,
    #[serde(default = "default_partitions_per_worker")]
    pub partitions_per_worker: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub compression_level: i32,
}

fn default_upload_worker_count() -> WorkerCount {
    WorkerCount::Auto("auto".to_string())
}

fn default_partitions_per_worker() -> usize {
    75
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum WorkerCount {
    Manual(usize),
    Auto(String),
}

impl WorkerCount {
    /// Resolve worker count based on configuration
    /// 
    /// Formula: Similar to websocket workers, compute based on expected workload
    /// - Each partition (exchange + market_type + stream_type + symbol) creates separate batches
    /// - Workers process batches in round-robin
    /// - Rule of thumb: 1 worker per 50-100 active partitions, min 2, max 16
    pub fn resolve(&self, exchanges: &[ExchangeConfig], partitions_per_worker: usize) -> usize {
        match self {
            WorkerCount::Manual(n) => *n,
            WorkerCount::Auto(s) if s == "auto" => {
                let total_partitions: usize = exchanges.iter()
                    .map(|ex| ex.estimate_partition_count())
                    .sum();
                
                // Auto-compute based on partition count and configurable ratio
                // 
                // Formula: workers = total_partitions / partitions_per_worker
                // 
                // The partitions_per_worker ratio (default: 75) is based on:
                // - Batch creation: ~1-10 batches/sec per partition (depends on message volume)
                // - S3 upload: ~0.1-1 sec per batch (depends on size, compression, network)
                // - Queue depth: 100 batches buffer
                // - Safety margin: Better slightly over- than under-provisioned
                //
                // Tune via config:
                // - High volume + small batches → use 50 (more workers)
                // - Low volume + large batches → use 100 (fewer workers)
                //
                // Min 2 workers (for low volume), max 16 (diminishing returns beyond this)
                if total_partitions == 0 {
                    2 // Default when no exchanges configured yet
                } else {
                    let divisor = partitions_per_worker.max(1); // Prevent division by zero
                    let calculated = (total_partitions + divisor - 1) / divisor; // ceiling division
                    calculated.max(2).min(16)
                }
            }
            WorkerCount::Auto(_) => 4, // Fallback for invalid string values
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    pub market_type: MarketType,
    pub ws_endpoint: String,
    pub rest_endpoint: String,
    pub mode: SubscriptionMode,
    pub symbols: Vec<String>,
    pub symbol_refresh_interval_s: u64,
    pub streams: Vec<String>,
    #[serde(default)]
    pub kline_intervals: Vec<String>,
    pub ws_connection_rate: u32,
    pub ws_streams_per_connection: usize,
    pub ws_max_connections: usize,
    pub ws_max_batch_size: usize,
    #[serde(default)]
    pub ws_subscription_delay_ms: u64,
}

impl ExchangeConfig {
    /// Estimate the number of unique partitions this exchange will produce
    /// 
    /// Partitions are created per (exchange, market_type, stream_type, symbol, hour)
    /// - Most streams create 1 partition per symbol (trade, depth, aggTrade, etc.)
    /// - Kline streams create N partitions per symbol (one per interval)
    /// - MarkPrice is global (not per symbol), creates 1 partition total
    pub fn estimate_partition_count(&self) -> usize {
        let symbol_count = self.symbols.len().max(1); // min 1 for safety
        let mut partitions = 0;
        
        for stream in &self.streams {
            match stream.as_str() {
                "kline" => {
                    // Each kline interval creates a separate partition per symbol
                    partitions += symbol_count * self.kline_intervals.len();
                }
                "markPrice" => {
                    // markPrice is global, not per-symbol
                    partitions += 1;
                }
                _ => {
                    // trade, depth, aggTrade, publicTrade, orderbook: 1 per symbol
                    partitions += symbol_count;
                }
            }
        }
        
        partitions
    }
    
    fn validate(&self) -> Result<()> {
        // Validate name
        if self.name.is_empty() {
            return Err(Error::Config("Exchange name cannot be empty".to_string()));
        }
        
        // Validate endpoints
        if self.ws_endpoint.is_empty() {
            return Err(Error::Config(format!("WebSocket endpoint cannot be empty for exchange {}", self.name)));
        }
        
        if self.rest_endpoint.is_empty() {
            return Err(Error::Config(format!("REST endpoint cannot be empty for exchange {}", self.name)));
        }
        
        // Validate mode-specific requirements
        match self.mode {
            SubscriptionMode::Explicit => {
                if self.symbols.is_empty() {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='explicit' but symbols list is empty", 
                        self.name
                    )));
                }
            }
            SubscriptionMode::All => {
                if self.symbol_refresh_interval_s == 0 {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='all' but symbol_refresh_interval_s is 0", 
                        self.name
                    )));
                }
            }
        }
        
        // Validate streams
        if self.streams.is_empty() {
            return Err(Error::Config(format!("Exchange {} has no streams configured", self.name)));
        }
        
        // Validate kline intervals if kline stream is requested
        if self.streams.contains(&"kline".to_string()) && self.kline_intervals.is_empty() {
            return Err(Error::Config(format!(
                "Exchange {} has 'kline' stream but no kline_intervals configured", 
                self.name
            )));
        }
        
        // Validate rate limits
        if self.ws_connection_rate == 0 {
            return Err(Error::Config(format!("Exchange {} ws_connection_rate must be > 0", self.name)));
        }
        
        if self.ws_streams_per_connection == 0 {
            return Err(Error::Config(format!("Exchange {} ws_streams_per_connection must be > 0", self.name)));
        }
        
        if self.ws_max_connections == 0 {
            return Err(Error::Config(format!("Exchange {} ws_max_connections must be > 0", self.name)));
        }
        
        if self.ws_max_batch_size == 0 {
            return Err(Error::Config(format!("Exchange {} ws_max_batch_size must be > 0", self.name)));
        }
        
        if self.ws_max_batch_size > self.ws_streams_per_connection {
            return Err(Error::Config(format!(
                "Exchange {} ws_max_batch_size ({}) cannot exceed ws_streams_per_connection ({})", 
                self.name, self.ws_max_batch_size, self.ws_streams_per_connection
            )));
        }
        
        if self.ws_subscription_delay_ms > 10000 {
            return Err(Error::Config(format!(
                "Exchange {} ws_subscription_delay_ms ({}) is too high (max 10000ms)", 
                self.name, self.ws_subscription_delay_ms
            )));
        }
        
        Ok(())
    }
    
    pub fn exchange_key(&self) -> String {
        format!("{}-{}", self.name, self.market_type.as_str())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum MarketType {
    Spot,
    Perpetual,
}

impl MarketType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketType::Spot => "spot",
            MarketType::Perpetual => "perpetual",
        }
    }
}

impl std::fmt::Display for MarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionMode {
    Explicit,
    All,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_mode_validation() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec![],  // Empty - should fail
            symbol_refresh_interval_s: 0,
            streams: vec!["trade".to_string()],
            kline_intervals: vec![],
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_all_mode_validation() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::All,
            symbols: vec![],
            symbol_refresh_interval_s: 0,  // 0 - should fail
            streams: vec!["trade".to_string()],
            kline_intervals: vec![],
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_kline_without_intervals() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["kline".to_string()],
            kline_intervals: vec![],  // Empty - should fail
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
}
