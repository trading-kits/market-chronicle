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
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
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
        ws_max_connections: 300,
        ws_max_batch_size: 200,
        ws_subscription_delay_ms: 0,
    };
    
    assert_eq!(config.exchange_key(), "binance-spot");
}

#[test]
fn test_worker_count_auto() {
    let toml_auto = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = "auto"
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "1h"]
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_auto).expect("Failed to parse auto config");
    let worker_count = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker)
        .expect("Failed to resolve worker count");
    
    // 3 symbols × (1 trade + 1 depth + 3 klines) = 15 partitions
    // 15 / 75 = 0.2, rounds up to min of 2
    assert_eq!(worker_count, 2);
}

#[test]
fn test_worker_count_manual() {
    let toml_manual = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = 8
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_manual).expect("Failed to parse manual config");
    let worker_count = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker)
        .expect("Failed to resolve worker count");
    
    // Manual count should be used directly
    assert_eq!(worker_count, 8);
}

#[test]
fn test_partition_estimation() {
    let config = ExchangeConfig {
        name: "binance".to_string(),
        market_type: MarketType::Spot,
        ws_endpoint: "wss://test".to_string(),
        rest_endpoint: "https://test".to_string(),
        mode: SubscriptionMode::Explicit,
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        symbol_refresh_interval_s: 0,
        streams: vec!["trade".to_string(), "depth".to_string(), "kline".to_string()],
        kline_intervals: vec!["1m".to_string(), "5m".to_string()],
        ws_connection_rate: 5,
        ws_streams_per_connection: 1024,
        ws_max_connections: 300,
        ws_max_batch_size: 200,
        ws_subscription_delay_ms: 0,
    };
    
    // 2 symbols × (1 trade + 1 depth + 2 klines) = 8 partitions
    assert_eq!(config.estimate_partition_count(), 8);
}

#[test]
fn test_partition_estimation_with_markprice() {
    let config = ExchangeConfig {
        name: "binance".to_string(),
        market_type: MarketType::Perpetual,
        ws_endpoint: "wss://test".to_string(),
        rest_endpoint: "https://test".to_string(),
        mode: SubscriptionMode::Explicit,
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        symbol_refresh_interval_s: 0,
        streams: vec!["aggTrade".to_string(), "markPrice".to_string()],
        kline_intervals: vec![],
        ws_connection_rate: 5,
        ws_streams_per_connection: 1024,
        ws_max_connections: 300,
        ws_max_batch_size: 200,
        ws_subscription_delay_ms: 0,
    };
    
    // 2 symbols × 1 aggTrade + 1 markPrice (global) = 3 partitions
    assert_eq!(config.estimate_partition_count(), 3);
}

#[test]
fn test_worker_count_custom_ratio() {
    let toml_custom = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = "auto"
partitions_per_worker = 50
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "1h"]
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_custom).expect("Failed to parse custom ratio config");
    let worker_count = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker)
        .expect("Failed to resolve worker count");
    
    // 3 symbols × (1 trade + 1 depth + 3 klines) = 15 partitions
    // 15 / 50 = 0.3, rounds up to min of 2
    assert_eq!(config.s3.partitions_per_worker, 50);
    assert_eq!(worker_count, 2);
}

#[test]
fn test_worker_count_high_partition_ratio() {
    let toml_high = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = "auto"
partitions_per_worker = 25
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTC1", "BTC2", "BTC3", "BTC4", "BTC5"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_high).expect("Failed to parse high ratio config");
    let worker_count = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker)
        .expect("Failed to resolve worker count");
    
    // 5 symbols × 1 trade = 5 partitions
    // 5 / 25 = 0.2, rounds up to min of 2
    assert_eq!(config.s3.partitions_per_worker, 25);
    assert_eq!(worker_count, 2);
}

#[test]
fn test_worker_count_invalid_string() {
    let toml_invalid = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = "invalid"
batch_size = 1000
batch_timeout_ms = 5000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_invalid).expect("Failed to parse invalid config");
    
    // Should return an error for invalid string value
    let result = config.s3.upload_worker_count.resolve(&config.exchanges, config.s3.partitions_per_worker);
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Invalid upload_worker_count value"));
    assert!(err_msg.contains("invalid"));
}

#[test]
fn test_duplicate_exchange_keys() {
    let toml_duplicate = r#"
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
ws_endpoint = "wss://test1"
rest_endpoint = "https://test1"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test2"
rest_endpoint = "https://test2"
mode = "explicit"
symbols = ["ETHUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_duplicate).expect("Failed to parse config");
    let result = config.validate();
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Duplicate exchange"));
    assert!(err_msg.contains("binance-spot"));
}

#[test]
fn test_s3_batch_conflict_small_batch_large_timeout() {
    let toml_conflict = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = 4
batch_size = 50
batch_timeout_ms = 120000
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_conflict).expect("Failed to parse config");
    let result = config.validate();
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Conflicting S3 config"));
    assert!(err_msg.contains("Small batch_size"));
}

#[test]
fn test_s3_batch_conflict_large_batch_small_timeout() {
    let toml_conflict = r#"
[s3]
endpoint = "http://localhost:9000"
bucket = "test-bucket"
region = "us-east-1"
prefix = "test"
access_key_id = "test"
secret_access_key = "test"
upload_worker_count = 4
batch_size = 15000
batch_timeout_ms = 500
compression_level = 3

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_conflict).expect("Failed to parse config");
    let result = config.validate();
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Conflicting S3 config"));
    assert!(err_msg.contains("Large batch_size"));
}

#[test]
fn test_s3_invalid_compression_level() {
    let toml_invalid = r#"
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
compression_level = 25

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_invalid).expect("Failed to parse config");
    let result = config.validate();
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("compression_level"));
    assert!(err_msg.contains("between -7 and 22"));
}

#[test]
fn test_partitions_exceed_websocket_capacity() {
    let toml_exceed = r#"
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
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTC1", "BTC2", "BTC3", "BTC4", "BTC5"]
symbol_refresh_interval_s = 0
streams = ["trade", "depth", "kline"]
kline_intervals = ["1m", "5m", "15m", "1h"]
ws_connection_rate = 5
ws_streams_per_connection = 10
ws_max_connections = 2
ws_max_batch_size = 5
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_exceed).expect("Failed to parse config");
    let result = config.validate();
    assert!(result.is_err());
    
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("configuration conflict"));
    assert!(err_msg.contains("partitions exceeds maximum possible WebSocket streams"));
}

#[test]
fn test_compression_level_default() {
    let toml_no_compression = r#"
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

[[exchanges]]
name = "binance"
market_type = "spot"
ws_endpoint = "wss://test"
rest_endpoint = "https://test"
mode = "explicit"
symbols = ["BTCUSDT"]
symbol_refresh_interval_s = 0
streams = ["trade"]
kline_intervals = []
ws_connection_rate = 5
ws_streams_per_connection = 1024
ws_max_connections = 300
ws_max_batch_size = 200
ws_subscription_delay_ms = 0
"#;
    
    let config: Config = toml::from_str(toml_no_compression).expect("Failed to parse config");
    
    // Should default to 3
    assert_eq!(config.s3.compression_level, 3);
}
