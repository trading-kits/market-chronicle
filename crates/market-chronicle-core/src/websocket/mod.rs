pub mod binance;
pub mod bybit;

use async_trait::async_trait;
use crate::error::Result;
use crate::types::MarketMessage;

#[async_trait]
pub trait ExchangeWebSocket: Send + Sync {
    /// Connect to exchange WebSocket endpoint
    async fn connect(&mut self) -> Result<()>;
    
    /// Subscribe to streams
    async fn subscribe(&mut self, streams: Vec<String>) -> Result<()>;
    
    /// Unsubscribe from streams
    async fn unsubscribe(&mut self, streams: Vec<String>) -> Result<()>;
    
    /// Receive next message
    async fn recv(&mut self) -> Result<Option<MarketMessage>>;
    
    /// Send ping/pong to keep connection alive
    async fn ping(&mut self) -> Result<()>;
    
    /// Check if connection is healthy
    fn is_connected(&self) -> bool;
    
    /// Get the list of currently subscribed streams
    fn subscribed_streams(&self) -> Vec<String>;
}
