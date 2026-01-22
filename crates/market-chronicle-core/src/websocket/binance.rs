use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, trace, warn};

use crate::config::MarketType;
use crate::error::{Error, Result};
use crate::types::{MarketMessage, StreamType};
use super::ExchangeWebSocket;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct BinanceWebSocket {
    ws_stream: Option<WsStream>,
    endpoint: String,
    market_type: MarketType,
    subscribed_streams: HashSet<String>,
    last_ping: Instant,
    request_id: u64,
}

impl BinanceWebSocket {
    pub fn new(endpoint: String, market_type: MarketType) -> Self {
        Self {
            ws_stream: None,
            endpoint,
            market_type,
            subscribed_streams: HashSet::new(),
            last_ping: Instant::now(),
            request_id: 1,
        }
    }
    
    fn next_request_id(&mut self) -> u64 {
        let id = self.request_id;
        self.request_id += 1;
        id
    }
    
    fn parse_message(&self, text: &str) -> Result<Option<MarketMessage>> {
        // Try to parse as stream data
        if let Ok(data) = serde_json::from_str::<StreamData>(text) {
            return self.convert_stream_data(data);
        }
        
        // Try to parse as response (subscribe/unsubscribe confirmation)
        if let Ok(_response) = serde_json::from_str::<SubscriptionResponse>(text) {
            trace!("Received subscription response");
            return Ok(None);
        }
        
        // Unknown message format
        trace!("Received unknown message: {}", text);
        Ok(None)
    }
    
    fn convert_stream_data(&self, data: StreamData) -> Result<Option<MarketMessage>> {
        let stream_parts: Vec<&str> = data.stream.split('@').collect();
        if stream_parts.len() < 2 {
            return Ok(None);
        }
        
        let symbol = stream_parts[0].to_uppercase();
        let stream_name = stream_parts[1];
        
        let stream_type = if stream_name.starts_with("kline_") {
            StreamType::Kline
        } else if stream_name == "trade" {
            StreamType::Trade
        } else if stream_name == "depth" {
            StreamType::Depth
        } else if stream_name == "aggTrade" {
            StreamType::AggTrade
        } else if stream_name.contains("markPrice") {
            StreamType::MarkPrice
        } else {
            return Ok(None);
        };
        
        // Extract timestamp from data
        let timestamp = match stream_type {
            StreamType::Trade | StreamType::AggTrade => {
                data.data.get("T")
                    .or_else(|| data.data.get("E"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
            }
            StreamType::Kline => {
                data.data.get("E")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
            }
            _ => {
                data.data.get("E")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
            }
        };
        
        Ok(Some(MarketMessage {
            local_timestamp: timestamp,
            exchange: "binance".to_string(),
            market_type: self.market_type,
            stream_type,
            symbol,
            data: data.data,
        }))
    }
}

#[async_trait]
impl ExchangeWebSocket for BinanceWebSocket {
    async fn connect(&mut self) -> Result<()> {
        debug!("Connecting to Binance WebSocket: {}", self.endpoint);
        
        let (ws_stream, _) = connect_async(&self.endpoint)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect: {}", e)))?;
        
        self.ws_stream = Some(ws_stream);
        self.last_ping = Instant::now();
        
        debug!("Connected to Binance WebSocket");
        Ok(())
    }
    
    async fn subscribe(&mut self, streams: Vec<String>) -> Result<()> {
        if streams.is_empty() {
            return Ok(());
        }
        
        let request_id = self.next_request_id();
        
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        let request = SubscriptionRequest {
            method: "SUBSCRIBE".to_string(),
            params: streams.clone(),
            id: request_id,
        };
        
        let msg = serde_json::to_string(&request)
            .map_err(|e| Error::WebSocket(format!("Failed to serialize subscribe request: {}", e)))?;
        
        ws_stream.send(Message::Text(msg)).await
            .map_err(|e| Error::WebSocket(format!("Failed to send subscribe: {}", e)))?;
        
        for stream in streams {
            self.subscribed_streams.insert(stream);
        }
        
        debug!("Subscribed to {} streams", self.subscribed_streams.len());
        Ok(())
    }
    
    async fn unsubscribe(&mut self, streams: Vec<String>) -> Result<()> {
        if streams.is_empty() {
            return Ok(());
        }
        
        let request_id = self.next_request_id();
        
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        let request = SubscriptionRequest {
            method: "UNSUBSCRIBE".to_string(),
            params: streams.clone(),
            id: request_id,
        };
        
        let msg = serde_json::to_string(&request)
            .map_err(|e| Error::WebSocket(format!("Failed to serialize unsubscribe request: {}", e)))?;
        
        ws_stream.send(Message::Text(msg)).await
            .map_err(|e| Error::WebSocket(format!("Failed to send unsubscribe: {}", e)))?;
        
        for stream in streams {
            self.subscribed_streams.remove(&stream);
        }
        
        debug!("Unsubscribed, remaining streams: {}", self.subscribed_streams.len());
        Ok(())
    }
    
    async fn recv(&mut self) -> Result<Option<MarketMessage>> {
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => {
                self.parse_message(&text)
            }
            Some(Ok(Message::Ping(_))) => {
                // Server sent ping, we'll automatically reply with pong
                Ok(None)
            }
            Some(Ok(Message::Pong(_))) => {
                Ok(None)
            }
            Some(Ok(Message::Close(_))) => {
                warn!("WebSocket connection closed by server");
                Err(Error::ConnectionClosed)
            }
            Some(Err(e)) => {
                error!("WebSocket error: {}", e);
                Err(Error::WebSocket(format!("Receive error: {}", e)))
            }
            None => {
                warn!("WebSocket stream ended");
                Err(Error::ConnectionClosed)
            }
            _ => Ok(None),
        }
    }
    
    async fn ping(&mut self) -> Result<()> {
        // Binance doesn't require client-initiated pings, but we can send them
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        ws_stream.send(Message::Ping(vec![]))
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to send ping: {}", e)))?;
        
        self.last_ping = Instant::now();
        Ok(())
    }
    
    fn is_connected(&self) -> bool {
        self.ws_stream.is_some()
    }
    
    fn subscribed_streams(&self) -> Vec<String> {
        self.subscribed_streams.iter().cloned().collect()
    }
}

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    method: String,
    params: Vec<String>,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    #[allow(dead_code)]
    result: Option<serde_json::Value>,
    #[allow(dead_code)]
    id: u64,
}

#[derive(Debug, Deserialize)]
struct StreamData {
    stream: String,
    data: serde_json::Value,
}
