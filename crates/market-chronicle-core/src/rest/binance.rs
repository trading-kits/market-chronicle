use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, warn};

use crate::config::MarketType;
use crate::error::{Error, Result};
use super::RestClient;

pub struct BinanceRestClient {
    client: Client,
    base_url: String,
}

impl BinanceRestClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        
        Self { client, base_url }
    }
    
    pub async fn get_spot_symbols(&self) -> Result<Vec<String>> {
        let url = format!("{}/api/v3/exchangeInfo", self.base_url);
        
        debug!("Fetching Binance spot symbols from {}", url);
        
        let response = self.retry_request(&url).await?;
        let info: ExchangeInfo = response.json().await
            .map_err(|e| Error::RestApi(format!("Failed to parse Binance spot response: {}", e)))?;
        
        let symbols: Vec<String> = info.symbols
            .into_iter()
            .filter(|s| s.status == "TRADING")
            .map(|s| s.symbol)
            .collect();
        
        debug!("Found {} active Binance spot symbols", symbols.len());
        Ok(symbols)
    }
    
    pub async fn get_perpetual_symbols(&self) -> Result<Vec<String>> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.base_url.replace("api.binance", "fapi.binance"));
        
        debug!("Fetching Binance perpetual symbols from {}", url);
        
        let response = self.retry_request(&url).await?;
        let info: ExchangeInfo = response.json().await
            .map_err(|e| Error::RestApi(format!("Failed to parse Binance perpetual response: {}", e)))?;
        
        let symbols: Vec<String> = info.symbols
            .into_iter()
            .filter(|s| s.status == "TRADING")
            .map(|s| s.symbol)
            .collect();
        
        debug!("Found {} active Binance perpetual symbols", symbols.len());
        Ok(symbols)
    }
    
    async fn retry_request(&self, url: &str) -> Result<reqwest::Response> {
        let mut backoff = Duration::from_secs(1);
        let max_retries = 3;
        
        for attempt in 0..max_retries {
            match self.client.get(url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(response);
                    } else {
                        let status = response.status();
                        let text = response.text().await.unwrap_or_default();
                        warn!("HTTP error {} (attempt {}): {}", status, attempt + 1, text);
                    }
                }
                Err(e) => {
                    warn!("Request failed (attempt {}): {}", attempt + 1, e);
                }
            }
            
            if attempt < max_retries - 1 {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
        
        Err(Error::RestApi(format!("Failed to fetch from {} after {} retries", url, max_retries)))
    }
}

#[async_trait]
impl RestClient for BinanceRestClient {
    async fn get_symbols(&self, market_type: MarketType) -> Result<Vec<String>> {
        match market_type {
            MarketType::Spot => self.get_spot_symbols().await,
            MarketType::Perpetual => self.get_perpetual_symbols().await,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<Symbol>,
}

#[derive(Debug, Deserialize)]
struct Symbol {
    symbol: String,
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires network access
    async fn test_get_spot_symbols() {
        let client = BinanceRestClient::new("https://api.binance.com".to_string());
        let symbols = client.get_spot_symbols().await.unwrap();
        assert!(!symbols.is_empty());
        assert!(symbols.contains(&"BTCUSDT".to_string()));
    }
}
