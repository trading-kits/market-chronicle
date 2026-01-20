use market_chronicle_core::{
    config::{ExchangeConfig, SubscriptionMode},
    error::Result,
    rest::{RestClient, binance::BinanceRestClient, bybit::BybitRestClient},
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn, error};

use crate::worker_pool::WorkerPoolManager;

pub struct SymbolManager {
    config: ExchangeConfig,
    rest_client: Box<dyn RestClient>,
    current_symbols: HashSet<String>,
    worker_pool: WorkerPoolManager,
    shutdown: Arc<Notify>,
}

impl SymbolManager {
    pub fn new(
        config: ExchangeConfig,
        worker_pool: WorkerPoolManager,
        shutdown: Arc<Notify>,
    ) -> Self {
        let rest_client: Box<dyn RestClient> = match config.name.as_str() {
            "binance" => Box::new(BinanceRestClient::new(config.rest_endpoint.clone())),
            "bybit" => Box::new(BybitRestClient::new(config.rest_endpoint.clone())),
            _ => panic!("Unknown exchange: {}", config.name),
        };
        
        Self {
            config,
            rest_client,
            current_symbols: HashSet::new(),
            worker_pool,
            shutdown,
        }
    }
    
    pub async fn run(mut self) {
        info!(
            exchange = %self.config.name,
            market_type = %self.config.market_type,
            mode = ?self.config.mode,
            "Symbol manager starting"
        );
        
        match self.config.mode {
            SubscriptionMode::Explicit => {
                self.subscribe_explicit_symbols().await;
                // Wait for shutdown
                self.shutdown.notified().await;
            }
            SubscriptionMode::All => {
                self.run_dynamic_discovery().await;
            }
        }
        
        // Shutdown worker pool
        self.worker_pool.shutdown().await;
        
        info!(
            exchange = %self.config.name,
            "Symbol manager stopped"
        );
    }
    
    async fn subscribe_explicit_symbols(&mut self) {
        info!(
            exchange = %self.config.name,
            symbols = self.config.symbols.len(),
            "Subscribing to explicit symbol list"
        );
        
        let streams = self.build_streams(&self.config.symbols);
        
        info!(
            exchange = %self.config.name,
            total_streams = streams.len(),
            "Built streams for explicit symbols"
        );
        
        if let Err(e) = self.worker_pool.spawn_workers_for_streams(streams).await {
            error!(
                exchange = %self.config.name,
                "Failed to spawn workers: {}", e
            );
        }
        
        self.current_symbols = self.config.symbols.iter().cloned().collect();
    }
    
    async fn run_dynamic_discovery(&mut self) {
        let mut interval = tokio::time::interval(
            Duration::from_secs(self.config.symbol_refresh_interval_s)
        );
        
        // Initial fetch
        if let Err(e) = self.refresh_symbols().await {
            error!(
                exchange = %self.config.name,
                "Initial symbol fetch failed: {}", e
            );
        }
        
        loop {
            tokio::select! {
                _ = self.shutdown.notified() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.refresh_symbols().await {
                        error!(
                            exchange = %self.config.name,
                            "Symbol refresh failed: {}", e
                        );
                    }
                }
            }
        }
    }
    
    async fn refresh_symbols(&mut self) -> Result<()> {
        info!(
            exchange = %self.config.name,
            market_type = %self.config.market_type,
            "Fetching symbols from REST API"
        );
        
        let new_symbols = self.rest_client.get_symbols(self.config.market_type).await?;
        let new_symbols_set: HashSet<_> = new_symbols.into_iter().collect();
        
        info!(
            exchange = %self.config.name,
            total_symbols = new_symbols_set.len(),
            "Fetched symbols"
        );
        
        // Find added symbols
        let added: Vec<_> = new_symbols_set.difference(&self.current_symbols).cloned().collect();
        if !added.is_empty() {
            info!(
                exchange = %self.config.name,
                new_symbols = added.len(),
                "Discovered new symbols"
            );
            
            let streams = self.build_streams(&added);
            info!(
                exchange = %self.config.name,
                new_streams = streams.len(),
                "Building streams for new symbols"
            );
            
            self.worker_pool.spawn_workers_for_streams(streams).await?;
        }
        
        // Find removed symbols (delisted)
        let removed: Vec<_> = self.current_symbols.difference(&new_symbols_set).cloned().collect();
        if !removed.is_empty() {
            warn!(
                exchange = %self.config.name,
                delisted_symbols = removed.len(),
                "Symbols delisted (workers will continue)"
            );
            // Note: We don't unsubscribe or shutdown workers (workers never scale down)
        }
        
        self.current_symbols = new_symbols_set;
        
        Ok(())
    }
    
    fn build_streams(&self, symbols: &[String]) -> Vec<String> {
        let mut streams = Vec::new();
        
        for symbol in symbols {
            for stream_type in &self.config.streams {
                match stream_type.as_str() {
                    "kline" => {
                        // Kline streams need intervals
                        for interval in &self.config.kline_intervals {
                            streams.push(self.format_stream(symbol, "kline", Some(interval)));
                        }
                    }
                    other => {
                        streams.push(self.format_stream(symbol, other, None));
                    }
                }
            }
        }
        
        streams
    }
    
    fn format_stream(&self, symbol: &str, stream_type: &str, interval: Option<&String>) -> String {
        match self.config.name.as_str() {
            "binance" => {
                // Binance format: btcusdt@trade, btcusdt@kline_1m
                if let Some(interval) = interval {
                    format!("{}@{}_{}", symbol.to_lowercase(), stream_type, interval)
                } else {
                    format!("{}@{}", symbol.to_lowercase(), stream_type)
                }
            }
            "bybit" => {
                // Bybit format: publicTrade.BTCUSDT, kline.1.BTCUSDT
                if stream_type == "kline" {
                    if let Some(interval) = interval {
                        format!("kline.{}.{}", interval, symbol)
                    } else {
                        format!("kline.1.{}", symbol) // Default to 1m
                    }
                } else {
                    format!("{}.{}", stream_type, symbol)
                }
            }
            _ => format!("{}@{}", symbol.to_lowercase(), stream_type),
        }
    }
}
