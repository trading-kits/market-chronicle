use serde::{Deserialize, Serialize};
use std::time::Instant;
use crate::config::MarketType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMessage {
    /// Unix timestamp in milliseconds (local receive time)
    pub local_timestamp: i64,
    pub exchange: String,
    pub market_type: MarketType,
    pub stream_type: StreamType,
    pub symbol: String,
    /// Raw exchange-specific data
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum StreamType {
    Trade,
    Depth,
    Kline,
    #[serde(rename = "aggTrade")]
    AggTrade,
    #[serde(rename = "markPrice")]
    MarkPrice,
    #[serde(rename = "publicTrade")]
    PublicTrade,
    #[serde(rename = "orderbook")]
    Orderbook,
}

impl StreamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamType::Trade => "trade",
            StreamType::Depth => "depth",
            StreamType::Kline => "kline",
            StreamType::AggTrade => "aggTrade",
            StreamType::MarkPrice => "markPrice",
            StreamType::PublicTrade => "publicTrade",
            StreamType::Orderbook => "orderbook",
        }
    }
    
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "trade" => Some(StreamType::Trade),
            "depth" => Some(StreamType::Depth),
            "kline" => Some(StreamType::Kline),
            "aggTrade" => Some(StreamType::AggTrade),
            "markPrice" => Some(StreamType::MarkPrice),
            "publicTrade" => Some(StreamType::PublicTrade),
            "orderbook" => Some(StreamType::Orderbook),
            _ => None,
        }
    }
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub exchange: String,
    pub market_type: MarketType,
    pub stream_type: StreamType,
    pub symbol: String,
    /// Unix timestamp rounded to hour
    pub hour: i64,
}

impl PartitionKey {
    pub fn from_message(msg: &MarketMessage) -> Self {
        // Round timestamp to hour
        let hour = (msg.local_timestamp / 3600000) * 3600000;
        
        Self {
            exchange: msg.exchange.clone(),
            market_type: msg.market_type,
            stream_type: msg.stream_type,
            symbol: msg.symbol.clone(),
            hour,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Batch {
    pub partition_key: PartitionKey,
    pub messages: Vec<MarketMessage>,
    pub created_at: Instant,
}

impl Batch {
    pub fn new(partition_key: PartitionKey, created_at: Instant) -> Self {
        Self {
            partition_key,
            messages: Vec::new(),
            created_at,
        }
    }
    
    pub fn with_capacity(partition_key: PartitionKey, created_at: Instant, capacity: usize) -> Self {
        Self {
            partition_key,
            messages: Vec::with_capacity(capacity),
            created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_hour_rounding() {
        let msg = MarketMessage {
            local_timestamp: 1705334567890, // 2024-01-15 12:42:47.890
            exchange: "binance".to_string(),
            market_type: MarketType::Spot,
            stream_type: StreamType::Trade,
            symbol: "BTCUSDT".to_string(),
            data: serde_json::json!({}),
        };
        
        let key = PartitionKey::from_message(&msg);
        
        // Should be rounded down to hour: 2024-01-15 12:00:00.000
        assert_eq!(key.hour, 1705330800000);
    }
    
    #[test]
    fn test_stream_type_serialization() {
        let st = StreamType::AggTrade;
        let json = serde_json::to_string(&st).unwrap();
        assert_eq!(json, r#""aggTrade""#);
        
        let deserialized: StreamType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, StreamType::AggTrade);
    }
}
