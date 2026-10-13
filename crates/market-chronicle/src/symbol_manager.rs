use market_chronicle_core::{
    config::{ExchangeConfig, SubscriptionMode},
    error::Result,
    rest::{RestClient, binance::BinanceRestClient, bybit::BybitRestClient},
};
use opentelemetry::KeyValue;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tracing::{debug, info, warn, error};

use crate::worker_pool::WorkerPoolManager;

pub struct SymbolManager {
    config: ExchangeConfig,
    rest_client: Box<dyn RestClient>,
    current_symbols: HashSet<String>,
    worker_pool: WorkerPoolManager,
    shutdown: Arc<Notify>,
    symbols_active_gauge: opentelemetry::metrics::UpDownCounter<i64>,
    exchange_label: Vec<KeyValue>,
    streams_len: Option<oneshot::Sender<usize>>
}

impl SymbolManager {
    pub fn new(
        config: ExchangeConfig,
        worker_pool: WorkerPoolManager,
        shutdown: Arc<Notify>,
        streams_len: oneshot::Sender<usize>
    ) -> Self {
        let rest_client: Box<dyn RestClient> = match config.name.as_str() {
            "binance" => Box::new(BinanceRestClient::new(config.rest_endpoint.clone())),
            "bybit" => Box::new(BybitRestClient::new(config.rest_endpoint.clone())),
            _ => panic!("Unknown exchange: {}", config.name),
        };

        // Initialize metrics
        let meter = opentelemetry::global::meter("symbol_manager");
        let symbols_active_gauge = meter
            .i64_up_down_counter("symbols_active")
            .with_description("Number of active trading symbols")
            .init();

        let exchange_label = vec![
            KeyValue::new("exchange", config.name.clone()),
            KeyValue::new("market_type", config.market_type.to_string()),
        ];

        // Initialize gauge to 0
        symbols_active_gauge.add(0, &exchange_label);

        Self {
            config,
            rest_client,
            current_symbols: HashSet::new(),
            worker_pool,
            shutdown,
            symbols_active_gauge,
            exchange_label,
            streams_len: Some(streams_len)
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
        let symbols = self.config.symbols.as_ref()
            .expect("symbols must be set for mode='explicit' (validated at startup)");

        info!(
            exchange = %self.config.name,
            symbols = symbols.len(),
            "Subscribing to explicit symbol list"
        );

        let streams = self.build_streams(symbols);

        if let Some(streams_len_tx) = self.streams_len.take() {  // take() consumes the Option
            let _ = streams_len_tx.send(streams.len());         // ignore error if receiver dropped
        }

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

        self.current_symbols = symbols.iter().cloned().collect();
        self.symbols_active_gauge.add(self.current_symbols.len() as i64, &self.exchange_label);
    }

    async fn run_dynamic_discovery(&mut self) {
        let refresh_interval = self.config.symbol_refresh_interval_s
            .expect("symbol_refresh_interval_s must be set for mode='all' (validated at startup)");
        let mut interval = tokio::time::interval(
            Duration::from_secs(refresh_interval)
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
                symbols = ?added.iter().take(10).collect::<Vec<_>>(),
                "Discovered new symbols"
            );

            let streams = self.build_streams(&added);

            if let Some(streams_len_tx) = self.streams_len.take() {  // take() consumes the Option
                let _ = streams_len_tx.send(streams.len());         // ignore error if receiver dropped
            }

            info!(
                exchange = %self.config.name,
                new_streams = streams.len(),
                "Building streams for new symbols"
            );

            self.worker_pool.spawn_workers_for_streams(streams).await?;
        } else {
            debug!(
                exchange = %self.config.name,
                "No new symbols discovered"
            );
        }

        // Find removed symbols (delisted)
        let removed: Vec<_> = self.current_symbols.difference(&new_symbols_set).cloned().collect();
        if !removed.is_empty() {
            warn!(
                exchange = %self.config.name,
                delisted_symbols = removed.len(),
                symbols = ?removed.iter().take(10).collect::<Vec<_>>(),
                "Symbols delisted, unsubscribing from streams"
            );

            // Build streams for delisted symbols and unsubscribe
            let streams_to_remove = self.build_streams(&removed);
            info!(
                exchange = %self.config.name,
                streams_to_remove = streams_to_remove.len(),
                "Unsubscribing from delisted symbol streams"
            );

            if let Err(e) = self.worker_pool.unsubscribe_streams(streams_to_remove).await {
                error!(
                    exchange = %self.config.name,
                    error = %e,
                    "Failed to unsubscribe from delisted streams"
                );
            }
        }

        let old_count = self.current_symbols.len();
        self.current_symbols = new_symbols_set;
        let new_count = self.current_symbols.len();

        // Update gauge
        self.symbols_active_gauge.add((new_count as i64) - (old_count as i64), &self.exchange_label);

        Ok(())
    }

    fn build_streams(&self, symbols: &[String]) -> Vec<String> {
        let mut streams = Vec::new();

        for symbol in symbols {
            for stream_type in &self.config.streams {
                match stream_type.as_str() {
                    "kline" => {
                        // Kline streams need intervals
                        if let Some(ref intervals) = self.config.kline_intervals {
                            for interval in intervals {
                                streams.push(self.format_stream(symbol, "kline", Some(interval)));
                            }
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

    fn format_stream(&self, symbol: &str, mut stream_type: &str, interval: Option<&String>) -> String {
        match self.config.name.as_str() {
            "binance" => {
                // Binance format: btcusdt@trade, btcusdt@depth, btcusdt@kline_1m
                if let Some(interval) = interval {
                    format!("{}@{}_{}", symbol.to_lowercase(), stream_type, interval)
                } else {
                    format!("{}@{}", symbol.to_lowercase(), stream_type)
                }
            }
            "bybit" => {
                match stream_type {
                    "trade" => {
                        stream_type = "publicTrade"
                    }
                    "depth" => {
                        stream_type = "orderbook"
                    }
                    "kline" => {}
                    _  => unreachable!("stream_type should only be one of the known values"),
                }
                // Bybit format: publicTrade.BTCUSDT, kline.1.BTCUSDT, orderbook.1.BTCUSDT
                match stream_type {
                    "kline" => {
                        if let Some(interval) = interval {
                            format!("kline.{}.{}", interval, symbol)
                        } else {
                            format!("kline.1.{}", symbol) // Default to 1m
                        }
                    }
                    "orderbook" => {
                        format!("{}.50.{}", stream_type, symbol)
                    }
                    _ => format!("{}.{}", stream_type, symbol)
                }
            }
            _ => format!("{}@{}", symbol.to_lowercase(), stream_type),
        }
    }
}
