use market_chronicle_core::{
    error::{Error, Result},
    types::MarketMessage,
    websocket::ExchangeWebSocket,
    config::MarketType,
};
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
        
        // Initial connection
        if let Err(e) = self.connect_with_backoff().await {
            error!(worker_id = self.worker_id, "Failed to establish initial connection: {}", e);
            return;
        }
        
        let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
        
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
                            if let Err(e) = self.ws_client.subscribe(streams).await {
                                error!(worker_id = self.worker_id, "Subscribe failed: {}", e);
                            }
                        }
                        Some(WorkerCommand::Unsubscribe { streams }) => {
                            if let Err(e) = self.ws_client.unsubscribe(streams).await {
                                error!(worker_id = self.worker_id, "Unsubscribe failed: {}", e);
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
                            if self.message_tx.send(market_msg).await.is_err() {
                                warn!(worker_id = self.worker_id, "Message channel closed, exiting");
                                break;
                            }
                        }
                        Ok(None) => {
                            // No message, continue
                        }
                        Err(Error::ConnectionClosed) => {
                            warn!(worker_id = self.worker_id, "Connection closed, reconnecting");
                            if let Err(e) = self.reconnect_with_backoff().await {
                                error!(worker_id = self.worker_id, "Failed to reconnect: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!(worker_id = self.worker_id, "WebSocket error: {}", e);
                            if let Err(e) = self.reconnect_with_backoff().await {
                                error!(worker_id = self.worker_id, "Failed to reconnect: {}", e);
                                break;
                            }
                        }
                    }
                }
                
                // Send periodic pings
                _ = ping_interval.tick() => {
                    if let Err(e) = self.ws_client.ping().await {
                        warn!(worker_id = self.worker_id, "Ping failed: {}", e);
                    }
                }
            }
        }
        
        info!(worker_id = self.worker_id, "Worker stopped");
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
