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
        
        if self.s3.upload_worker_count == 0 {
            return Err(Error::Config("S3 upload_worker_count must be > 0".to_string()));
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub upload_worker_count: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub compression_level: i32,
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
}

impl ExchangeConfig {
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
        };
        
        assert!(config.validate().is_err());
    }
}
