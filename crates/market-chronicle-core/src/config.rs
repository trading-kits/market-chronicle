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
    
    pub fn validate(&self) -> Result<()> {
        // Validate S3 config first
        self.s3.validate()?;
        
        // Validate exchanges list
        if self.exchanges.is_empty() {
            return Err(Error::Config("At least one exchange must be configured".to_string()));
        }
        
        // Check for duplicate exchange keys (name + market_type)
        let mut exchange_keys = std::collections::HashSet::new();
        for exchange in &self.exchanges {
            let key = exchange.exchange_key();
            if !exchange_keys.insert(key.clone()) {
                return Err(Error::Config(format!(
                    "Duplicate exchange configuration found: '{}'. Each exchange name + market_type combination must be unique",
                    key
                )));
            }
        }
        
        // Validate each exchange config
        for exchange in &self.exchanges {
            exchange.validate()?;
        }
        
        // Validate worker count after exchange validation (depends on exchanges)
        let resolved_worker_count = self.s3.upload_worker_count.resolve(&self.exchanges, self.s3.partitions_per_worker)?;
        if resolved_worker_count == 0 {
            return Err(Error::Config("S3 upload_worker_count must be > 0".to_string()));
        }
        
        if resolved_worker_count > 100 {
            return Err(Error::Config(format!(
                "S3 upload_worker_count ({}) is unreasonably high (max 100)",
                resolved_worker_count
            )));
        }
        
        // Validate S3 worker count against partition count
        let total_partitions = self.estimate_partition_count();
        if total_partitions > 0 {
            let partitions_per_worker = total_partitions / resolved_worker_count;
            
            if partitions_per_worker > 200 {
                return Err(Error::Config(format!(
                    "Configuration conflict: {} total partitions with {} workers = {} partitions/worker (max recommended: 200). Increase upload_worker_count or reduce symbols/streams/exchanges",
                    total_partitions, resolved_worker_count, partitions_per_worker
                )));
            }
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
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

fn default_compression_level() -> i32 {
    3
}

impl S3Config {
    pub fn validate(&self) -> Result<()> {
        // Validate required fields
        if self.endpoint.is_empty() {
            return Err(Error::Config("S3 endpoint cannot be empty".to_string()));
        }
        
        if self.bucket.is_empty() {
            return Err(Error::Config("S3 bucket cannot be empty".to_string()));
        }
        
        if self.region.is_empty() {
            return Err(Error::Config("S3 region cannot be empty".to_string()));
        }
        
        if self.access_key_id.is_empty() {
            return Err(Error::Config("S3 access_key_id cannot be empty".to_string()));
        }
        
        if self.secret_access_key.is_empty() {
            return Err(Error::Config("S3 secret_access_key cannot be empty".to_string()));
        }
        
        // Validate numeric constraints
        if self.partitions_per_worker == 0 {
            return Err(Error::Config("S3 partitions_per_worker must be > 0".to_string()));
        }
        
        if self.batch_size == 0 {
            return Err(Error::Config("S3 batch_size must be > 0".to_string()));
        }
        
        if self.batch_size > 100000 {
            return Err(Error::Config(format!(
                "S3 batch_size ({}) is unreasonably large (max 100000)",
                self.batch_size
            )));
        }
        
        if self.batch_timeout_ms == 0 {
            return Err(Error::Config("S3 batch_timeout_ms must be > 0".to_string()));
        }
        
        if self.batch_timeout_ms > 3600000 {
            return Err(Error::Config(format!(
                "S3 batch_timeout_ms ({}) is too high (max 3600000ms / 1 hour)",
                self.batch_timeout_ms
            )));
        }
        
        // Validate compression level (zstd range: -7 to 22, where 3 is default)
        if self.compression_level < -7 || self.compression_level > 22 {
            return Err(Error::Config(format!(
                "S3 compression_level ({}) must be between -7 and 22",
                self.compression_level
            )));
        }
        
        // Warn about potentially conflicting settings
        if self.batch_size < 100 && self.batch_timeout_ms > 60000 {
            return Err(Error::Config(format!(
                "Conflicting S3 config: Small batch_size ({}) with large batch_timeout_ms ({}ms) may cause excessive delays. Consider increasing batch_size or decreasing timeout",
                self.batch_size, self.batch_timeout_ms
            )));
        }
        
        if self.batch_size > 10000 && self.batch_timeout_ms < 1000 {
            return Err(Error::Config(format!(
                "Conflicting S3 config: Large batch_size ({}) with small batch_timeout_ms ({}ms) may never fill batches. Consider decreasing batch_size or increasing timeout",
                self.batch_size, self.batch_timeout_ms
            )));
        }
        
        Ok(())
    }
}

fn default_upload_worker_count() -> WorkerCount {
    WorkerCount::Manual(4)
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
    pub fn resolve(&self, exchanges: &[ExchangeConfig], partitions_per_worker: usize) -> Result<usize> {
        match self {
            WorkerCount::Manual(n) => Ok(*n),
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
                    Ok(2) // Default when no exchanges configured yet
                } else {
                    let divisor = partitions_per_worker.max(1); // Prevent division by zero
                    let calculated = (total_partitions + divisor - 1) / divisor; // ceiling division
                    Ok(calculated.max(2).min(16))
                }
            }
            WorkerCount::Auto(s) => {
                Err(Error::Config(format!(
                    "Invalid upload_worker_count value '{}'. Must be a number or 'auto'", 
                    s
                )))
            }
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
    
    pub fn validate(&self) -> Result<()> {
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
        
        // Validate mode-specific requirements and conflicts
        match self.mode {
            SubscriptionMode::Explicit => {
                if self.symbols.is_empty() {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='explicit' but symbols list is empty", 
                        self.name
                    )));
                }
                
                // In explicit mode, symbol_refresh_interval_s should not be set (it's ignored)
                if self.symbol_refresh_interval_s > 0 {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='explicit' but symbol_refresh_interval_s is {} (should be 0 as it's not used in explicit mode)",
                        self.name, self.symbol_refresh_interval_s
                    )));
                }
            }
            SubscriptionMode::All => {
                if self.symbol_refresh_interval_s == 0 {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='all' but symbol_refresh_interval_s is 0 (must be > 0)", 
                        self.name
                    )));
                }
                
                if self.symbol_refresh_interval_s > 86400 {
                    return Err(Error::Config(format!(
                        "Exchange {} symbol_refresh_interval_s ({}) is too high (max 86400s / 24 hours)", 
                        self.name, self.symbol_refresh_interval_s
                    )));
                }
                
                // In all mode, explicit symbols should not be provided (they're ignored)
                if !self.symbols.is_empty() {
                    return Err(Error::Config(format!(
                        "Exchange {} has mode='all' but {} explicit symbols are provided (should be empty as all symbols are discovered automatically)",
                        self.name, self.symbols.len()
                    )));
                }
            }
        }
        
        // Check for duplicate symbols
        if !self.symbols.is_empty() {
            let mut seen = std::collections::HashSet::new();
            for symbol in &self.symbols {
                if !seen.insert(symbol) {
                    return Err(Error::Config(format!(
                        "Exchange {} has duplicate symbol: '{}'",
                        self.name, symbol
                    )));
                }
            }
        }
        
        // Validate streams
        if self.streams.is_empty() {
            return Err(Error::Config(format!("Exchange {} has no streams configured", self.name)));
        }
        
        // Check for duplicate streams
        let mut seen_streams = std::collections::HashSet::new();
        for stream in &self.streams {
            if !seen_streams.insert(stream) {
                return Err(Error::Config(format!(
                    "Exchange {} has duplicate stream: '{}'",
                    self.name, stream
                )));
            }
        }
        
        // Validate known stream types
        const VALID_STREAMS: &[&str] = &[
            "trade", "depth", "aggTrade", "publicTrade", "orderbook", "kline", "markPrice"
        ];
        for stream in &self.streams {
            if !VALID_STREAMS.contains(&stream.as_str()) {
                return Err(Error::Config(format!(
                    "Exchange {} has invalid stream '{}'. Valid streams: {}",
                    self.name, stream, VALID_STREAMS.join(", ")
                )));
            }
        }
        
        // Validate kline intervals
        let has_kline_stream = self.streams.contains(&"kline".to_string());
        
        if has_kline_stream && self.kline_intervals.is_empty() {
            return Err(Error::Config(format!(
                "Exchange {} has 'kline' stream but no kline_intervals configured", 
                self.name
            )));
        }
        
        if !has_kline_stream && !self.kline_intervals.is_empty() {
            return Err(Error::Config(format!(
                "Exchange {} has {} kline_intervals configured but no 'kline' stream. Remove kline_intervals or add 'kline' to streams",
                self.name, self.kline_intervals.len()
            )));
        }
        
        // Check for duplicate kline intervals
        if !self.kline_intervals.is_empty() {
            let mut seen_intervals = std::collections::HashSet::new();
            for interval in &self.kline_intervals {
                if !seen_intervals.insert(interval) {
                    return Err(Error::Config(format!(
                        "Exchange {} has duplicate kline_interval: '{}'",
                        self.name, interval
                    )));
                }
            }
            
            // Validate known kline intervals
            const VALID_INTERVALS: &[&str] = &[
                "1m", "3m", "5m", "15m", "30m",
                "1h", "2h", "4h", "6h", "8h", "12h",
                "1d", "3d", "1w", "1M"
            ];
            for interval in &self.kline_intervals {
                if !VALID_INTERVALS.contains(&interval.as_str()) {
                    return Err(Error::Config(format!(
                        "Exchange {} has invalid kline_interval '{}'. Valid intervals: {}",
                        self.name, interval, VALID_INTERVALS.join(", ")
                    )));
                }
            }
        }
        
        // Validate rate limits
        if self.ws_connection_rate == 0 {
            return Err(Error::Config(format!("Exchange {} ws_connection_rate must be > 0", self.name)));
        }
        
        if self.ws_connection_rate > 100 {
            return Err(Error::Config(format!(
                "Exchange {} ws_connection_rate ({}) is unreasonably high (max 100 connections/sec)",
                self.name, self.ws_connection_rate
            )));
        }
        
        if self.ws_streams_per_connection == 0 {
            return Err(Error::Config(format!("Exchange {} ws_streams_per_connection must be > 0", self.name)));
        }
        
        if self.ws_streams_per_connection > 2000 {
            return Err(Error::Config(format!(
                "Exchange {} ws_streams_per_connection ({}) exceeds reasonable limit (max 2000)",
                self.name, self.ws_streams_per_connection
            )));
        }
        
        if self.ws_max_connections == 0 {
            return Err(Error::Config(format!("Exchange {} ws_max_connections must be > 0", self.name)));
        }
        
        if self.ws_max_connections > 1000 {
            return Err(Error::Config(format!(
                "Exchange {} ws_max_connections ({}) is unreasonably high (max 1000)",
                self.name, self.ws_max_connections
            )));
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
        
        // Validate logical consistency of connection settings
        let estimated_partitions = self.estimate_partition_count();
        let max_possible_streams = self.ws_max_connections * self.ws_streams_per_connection;
        
        if estimated_partitions > max_possible_streams {
            return Err(Error::Config(format!(
                "Exchange {} configuration conflict: Estimated {} partitions exceeds maximum possible WebSocket streams ({} connections × {} streams/conn = {}). Increase ws_max_connections or ws_streams_per_connection, or reduce symbols/streams/kline_intervals",
                self.name, estimated_partitions, self.ws_max_connections, 
                self.ws_streams_per_connection, max_possible_streams
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
    
    #[test]
    fn test_explicit_mode_with_refresh_interval() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 300,  // Should be 0 for explicit mode
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
    fn test_all_mode_with_explicit_symbols() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::All,
            symbols: vec!["BTCUSDT".to_string()],  // Should be empty for all mode
            symbol_refresh_interval_s: 300,
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
    fn test_kline_intervals_without_kline_stream() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["trade".to_string()],
            kline_intervals: vec!["1m".to_string()],  // No kline stream
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_duplicate_symbols() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string(), "BTCUSDT".to_string()],  // Duplicate
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
    fn test_duplicate_streams() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["trade".to_string(), "trade".to_string()],  // Duplicate
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
    fn test_duplicate_kline_intervals() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["kline".to_string()],
            kline_intervals: vec!["1m".to_string(), "1m".to_string()],  // Duplicate
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_invalid_stream_name() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["invalid_stream".to_string()],  // Invalid
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
    fn test_invalid_kline_interval() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["kline".to_string()],
            kline_intervals: vec!["10m".to_string()],  // Invalid interval
            ws_connection_rate: 5,
            ws_streams_per_connection: 1024,
            ws_max_connections: 300,
            ws_max_batch_size: 200,
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_ws_max_batch_exceeds_streams_per_connection() {
        let config = ExchangeConfig {
            name: "binance".to_string(),
            market_type: MarketType::Spot,
            ws_endpoint: "wss://example.com".to_string(),
            rest_endpoint: "https://example.com".to_string(),
            mode: SubscriptionMode::Explicit,
            symbols: vec!["BTCUSDT".to_string()],
            symbol_refresh_interval_s: 0,
            streams: vec!["trade".to_string()],
            kline_intervals: vec![],
            ws_connection_rate: 5,
            ws_streams_per_connection: 100,
            ws_max_connections: 300,
            ws_max_batch_size: 200,  // Exceeds ws_streams_per_connection
            ws_subscription_delay_ms: 0,
        };
        
        assert!(config.validate().is_err());
    }
}
