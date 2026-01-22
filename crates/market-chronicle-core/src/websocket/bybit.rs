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

pub struct BybitWebSocket {
    ws_stream: Option<WsStream>,
    endpoint: String,
    market_type: MarketType,
    subscribed_streams: HashSet<String>,
    last_pong: Instant,
}

impl BybitWebSocket {
    pub fn new(endpoint: String, market_type: MarketType) -> Self {
        Self {
            ws_stream: None,
            endpoint,
            market_type,
            subscribed_streams: HashSet::new(),
            last_pong: Instant::now(),
        }
    }
    
    fn parse_message(&self, text: &str) -> Result<Option<MarketMessage>> {
        // Try to parse as V5 message
        if let Ok(msg) = serde_json::from_str::<V5Message>(text) {
            return self.convert_v5_message(msg);
        }
        
        // Try to parse as operation response
        if let Ok(_response) = serde_json::from_str::<OperationResponse>(text) {
            trace!("Received operation response");
            return Ok(None);
        }
        
        // Try to parse as ping
        if let Ok(ping) = serde_json::from_str::<PingMessage>(text) {
            if ping.op == "ping" {
                trace!("Received ping from server");
                // We'll handle pong in the recv method
            }
            return Ok(None);
        }
        
        trace!("Received unknown message: {}", text);
        Ok(None)
    }
    
    fn convert_v5_message(&self, msg: V5Message) -> Result<Option<MarketMessage>> {
        if msg.topic.is_empty() || msg.data.is_null() {
            return Ok(None);
        }
        
        let topic_parts: Vec<&str> = msg.topic.split('.').collect();
        if topic_parts.is_empty() {
            return Ok(None);
        }
        
        let stream_name = topic_parts[0];
        let symbol = if topic_parts.len() > 1 {
            topic_parts[topic_parts.len() - 1].to_uppercase()
        } else {
            String::new()
        };
        
        let stream_type = match stream_name {
            "publicTrade" => StreamType::PublicTrade,
            "kline" => StreamType::Kline,
            "orderbook" => StreamType::Orderbook,
            _ => return Ok(None),
        };
        
        // Extract timestamp
        let timestamp = msg.ts.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        
        Ok(Some(MarketMessage {
            local_timestamp: timestamp,
            exchange: "bybit".to_string(),
            market_type: self.market_type,
            stream_type,
            symbol,
            data: msg.data,
        }))
    }
}

#[async_trait]
impl ExchangeWebSocket for BybitWebSocket {
    async fn connect(&mut self) -> Result<()> {
        debug!("Connecting to Bybit WebSocket: {}", self.endpoint);
        
        let (ws_stream, _) = connect_async(&self.endpoint)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect: {}", e)))?;
        
        self.ws_stream = Some(ws_stream);
        self.last_pong = Instant::now();
        
        debug!("Connected to Bybit WebSocket");
        Ok(())
    }
    
    async fn subscribe(&mut self, streams: Vec<String>) -> Result<()> {
        if streams.is_empty() {
            return Ok(());
        }
        
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        let request = SubscriptionRequest {
            op: "subscribe".to_string(),
            args: streams.clone(),
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
        
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        let request = SubscriptionRequest {
            op: "unsubscribe".to_string(),
            args: streams.clone(),
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
                // Check if it's a ping message
                if text.contains(r#""op":"ping""#) {
                    // Send pong response
                    let pong = PongMessage {
                        op: "pong".to_string(),
                    };
                    let pong_msg = serde_json::to_string(&pong).unwrap();
                    let _ = ws_stream.send(Message::Text(pong_msg)).await;
                    self.last_pong = Instant::now();
                    return Ok(None);
                }
                
                self.parse_message(&text)
            }
            Some(Ok(Message::Ping(data))) => {
                ws_stream.send(Message::Pong(data)).await.ok();
                Ok(None)
            }
            Some(Ok(Message::Pong(_))) => {
                self.last_pong = Instant::now();
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
        let ws_stream = self.ws_stream.as_mut()
            .ok_or_else(|| Error::WebSocket("Not connected".to_string()))?;
        
        let ping = PingMessage {
            op: "ping".to_string(),
        };
        
        let msg = serde_json::to_string(&ping)
            .map_err(|e| Error::WebSocket(format!("Failed to serialize ping: {}", e)))?;
        
        ws_stream.send(Message::Text(msg))
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to send ping: {}", e)))?;
        
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
    op: String,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OperationResponse {
    #[allow(dead_code)]
    success: bool,
    #[allow(dead_code)]
    ret_msg: String,
    #[allow(dead_code)]
    op: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct PingMessage {
    op: String,
}

#[derive(Debug, Serialize)]
struct PongMessage {
    op: String,
}

#[derive(Debug, Deserialize)]
struct V5Message {
    topic: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    msg_type: Option<String>,
    data: serde_json::Value,
    ts: Option<i64>,
}
