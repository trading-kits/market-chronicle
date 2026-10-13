use market_chronicle_core::types::*;
use market_chronicle_core::config::MarketType;

#[test]
fn test_partition_key_hour_rounding() {
    let msg = MarketMessage {
        local_timestamp: 1705334567890, // Some arbitrary timestamp with ms
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        stream_type: StreamType::Trade,
        symbol: "BTCUSDT".to_string(),
        data: serde_json::json!({"test": "data"}),
    };
    
    let key = PartitionKey::from_message(&msg);
    
    // Should be rounded down to nearest hour
    assert_eq!(key.hour % 3600000, 0);
    assert_eq!(key.exchange, "binance");
    assert_eq!(key.symbol, "BTCUSDT");
    assert_eq!(key.stream_type, StreamType::Trade);
}

#[test]
fn test_stream_type_display() {
    assert_eq!(StreamType::Trade.to_string(), "trade");
    assert_eq!(StreamType::Depth.to_string(), "depth");
    assert_eq!(StreamType::Kline.to_string(), "kline");
}

#[test]
fn test_batch_creation() {
    use std::time::Instant;
    
    let key = PartitionKey {
        exchange: "binance".to_string(),
        market_type: MarketType::Spot,
        stream_type: StreamType::Trade,
        symbol: "BTCUSDT".to_string(),
        hour: 1705334400000,
    };
    
    let batch = Batch::with_capacity(key.clone(), Instant::now(), 100);
    
    assert_eq!(batch.partition_key.exchange, "binance");
    assert_eq!(batch.messages.len(), 0);
    assert_eq!(batch.messages.capacity(), 100);
}

#[test]
fn test_stream_type_from_str() {
    assert_eq!(StreamType::from_str("trade"), Some(StreamType::Trade));
    assert_eq!(
        StreamType::from_str("depth"),
        Some(StreamType::Depth)
    );
    assert_eq!(StreamType::from_str("kline"), Some(StreamType::Kline));
    assert_eq!(StreamType::from_str("unknown"), None);
}
