use market_chronicle_core::{
    error::{Error, Result},
    types::MarketMessage,
    websocket::ExchangeWebSocket,
    config::MarketType,
};
use opentelemetry::KeyValue;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

pub enum WorkerCommand {
    Subscribe { streams: Vec<String> },
    Unsubscribe { streams: Vec<String> },
}

pub struct WebSocketWorker {
    worker_id: usize,
    exchange: String,
    market_type: MarketType,
    ws_client: Box<dyn ExchangeWebSocket>,
    message_tx: mpsc::Sender<MarketMessage>,
    command_rx: mpsc::Receiver<WorkerCommand>,
    shutdown: Arc<Notify>,
}

impl WebSocketWorker {
    pub fn new(
        worker_id: usize,
        exchange: String,
        market_type: MarketType,
        ws_client: Box<dyn ExchangeWebSocket>,
        message_tx: mpsc::Sender<MarketMessage>,
        command_rx: mpsc::Receiver<WorkerCommand>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            worker_id,
            exchange,
            market_type,
            ws_client,
            message_tx,
            command_rx,
            shutdown,
        }
    }

    pub async fn run(mut self) {
        info!(
            worker_id = self.worker_id,
            exchange = %self.exchange,
            market_type = %self.market_type,
            "WebSocket worker starting"
        );

        // Initialize metrics (OpenTelemetry will add _total suffix to counters)
        let meter = opentelemetry::global::meter("websocket_worker");
        let messages_received = meter
            .u64_counter("worker_messages_received")
            .with_description("Total messages received from WebSocket")
            .init();
        let reconnects = meter
            .u64_counter("worker_reconnects")
            .with_description("Total WebSocket reconnections")
            .init();
        let active_streams_gauge = meter
            .i64_up_down_counter("worker_active_streams")
            .with_description("Number of active WebSocket streams")
            .init();

        let worker_id_str = self.worker_id.to_string();
        let labels = [
            KeyValue::new("worker_id", worker_id_str.clone()),
            KeyValue::new("exchange", self.exchange.clone()),
            KeyValue::new("market_type", self.market_type.to_string()),
        ];

        // Initialize gauge to 0
        active_streams_gauge.add(0, &labels);

        // Initial connection
        if let Err(e) = self.connect_with_backoff().await {
            error!(worker_id = self.worker_id, "Failed to establish initial connection: {}", e);
            return;
        }

        let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
        let mut stats_interval = tokio::time::interval(Duration::from_secs(60));
        let mut message_count = 0u64;
        let mut last_message_count = 0u64;

        loop {
            tokio::select! {
                biased;

                // Handle shutdown signal
                _ = self.shutdown.notified() => {
                    info!(worker_id = self.worker_id, "Worker shutting down");
                    break;
                }

                // Handle commands (subscribe/unsubscribe)
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Subscribe { streams }) => {
                            let stream_count = streams.len();
                            debug!(
                                worker_id = self.worker_id,
                                streams = stream_count,
                                "Subscribing to streams"
                            );
                            if let Err(e) = self.ws_client.subscribe(streams).await {
                                error!(
                                    worker_id = self.worker_id,
                                    error = %e,
                                    "Subscribe failed"
                                );
                            } else {
                                active_streams_gauge.add(stream_count as i64, &labels);
                                info!(
                                    worker_id = self.worker_id,
                                    streams = stream_count,
                                    "Successfully subscribed to streams"
                                );
                            }
                        }
                        Some(WorkerCommand::Unsubscribe { streams }) => {
                            let stream_count = streams.len();
                            debug!(
                                worker_id = self.worker_id,
                                streams = stream_count,
                                "Unsubscribing from streams"
                            );
                            if let Err(e) = self.ws_client.unsubscribe(streams).await {
                                error!(
                                    worker_id = self.worker_id,
                                    error = %e,
                                    "Unsubscribe failed"
                                );
                            } else {
                                active_streams_gauge.add(-(stream_count as i64), &labels);
                            }
                        }
                        None => {
                            debug!(worker_id = self.worker_id, "Command channel closed");
                            break;
                        }
                    }
                }

                // Receive messages from WebSocket
                msg_result = self.ws_client.recv() => {
                    match msg_result {
                        Ok(Some(market_msg)) => {
                            message_count += 1;
                            messages_received.add(1, &labels);
                            if self.message_tx.send(market_msg).await.is_err() {
                                warn!(
                                    worker_id = self.worker_id,
                                    total_messages = message_count,
                                    "Message channel closed, exiting"
                                );
                                break;
                            }
                        }
                        Ok(None) => {
                            // No message, continue
                        }
                        Err(Error::ConnectionClosed) => {
                            reconnects.add(1, &labels);
                            warn!(
                                worker_id = self.worker_id,
                                messages_processed = message_count,
                                "Connection closed, reconnecting"
                            );
                            if let Err(e) = self.reconnect_with_backoff().await {
                                error!(
                                    worker_id = self.worker_id,
                                    error = %e,
                                    "Failed to reconnect after connection closed"
                                );
                                break;
                            }
                            info!(worker_id = self.worker_id, "Reconnected successfully");
                        }
                        Err(e) => {
                            reconnects.add(1, &labels);
                            error!(
                                worker_id = self.worker_id,
                                error = %e,
                                messages_processed = message_count,
                                "WebSocket error, attempting reconnect"
                            );
                            if let Err(e) = self.reconnect_with_backoff().await {
                                error!(
                                    worker_id = self.worker_id,
                                    error = %e,
                                    "Failed to reconnect after error"
                                );
                                break;
                            }
                            info!(worker_id = self.worker_id, "Reconnected after error");
                        }
                    }
                }

                // Send periodic pings
                _ = ping_interval.tick() => {
                    if let Err(e) = self.ws_client.ping().await {
                        warn!(
                            worker_id = self.worker_id,
                            error = %e,
                            "Ping failed"
                        );
                    } else {
                        debug!(worker_id = self.worker_id, "Ping sent successfully");
                    }
                }

                // Log periodic statistics
                _ = stats_interval.tick() => {
                    let messages_since_last = message_count - last_message_count;
                    let msg_per_sec = messages_since_last as f64 / 60.0;
                    info!(
                        worker_id = self.worker_id,
                        exchange = %self.exchange,
                        total_messages = message_count,
                        messages_last_min = messages_since_last,
                        msg_per_sec = format!("{:.2}", msg_per_sec),
                        active_streams = self.ws_client.subscribed_streams().len(),
                        "Worker statistics"
                    );
                    last_message_count = message_count;
                }
            }
        }

        info!(
            worker_id = self.worker_id,
            exchange = %self.exchange,
            total_messages_processed = message_count,
            "Worker stopped"
        );
    }

    async fn connect_with_backoff(&mut self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        loop {
            match self.ws_client.connect().await {
                Ok(_) => {
                    info!(worker_id = self.worker_id, "Connected successfully");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        worker_id = self.worker_id,
                        "Connection failed: {}, retrying in {:?}", e, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }
    }

    async fn reconnect_with_backoff(&mut self) -> Result<()> {
        let subscribed = self.ws_client.subscribed_streams();

        self.connect_with_backoff().await?;

        // Resubscribe to previous streams
        if !subscribed.is_empty() {
            info!(
                worker_id = self.worker_id,
                streams = subscribed.len(),
                "Resubscribing after reconnect"
            );
            self.ws_client.subscribe(subscribed).await?;
        }

        Ok(())
    }
}
