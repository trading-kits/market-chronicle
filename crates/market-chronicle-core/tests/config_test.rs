use market_chronicle_core::config::*;

#[test]
fn test_config_from_toml() {
    let toml_content = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = 4
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://stream.binance.com:9443/stream"
rest_endpoint = "https://api.binance.com"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade", "depth"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
"#;
    
    let config: Config = toml::from_str(toml_content).expect("Failed to parse config");
    
    assert_eq!(config.s3.bucket, "test-bucket");
    assert_eq!(config.exchanges.len(), 1);
    assert_eq!(config.exchanges[0].name, "binance");
    assert_eq!(config.exchanges[0].market_type, MarketType::Spot);
    assert_eq!(config.exchanges[0].mode, SubscriptionMode::Explicit);
    assert_eq!(config.exchanges[0].symbols.len(), 2);
}

#[test]
fn test_market_type_serialization() {
    assert_eq!(MarketType::Spot.as_str(), "spot");
    assert_eq!(MarketType::Perpetual.as_str(), "perpetual");
}

#[test]
fn test_exchange_key() {
    let config = ExchangeConfig {
        name: "binance".to_string(),
        market_type: MarketType::Spot,
        ws_endpoint: "wss://test".to_string(),
        rest_endpoint: "https://test".to_string(),
        mode: SubscriptionMode::Explicit,
        symbols: vec!["BTCUSDT".to_string()],
        symbol_refresh_interval_s: 0,
        streams: vec!["trade".to_string()],
        kline_intervals: vec![],
        ws_connection_rate: 5,
        ws_streams_per_connection: 1024,
        ws_max_connections = 300,
    };
    
    assert_eq!(config.exchange_key(), "binance-spot");
}
