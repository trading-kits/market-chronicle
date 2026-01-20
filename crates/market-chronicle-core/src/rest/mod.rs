pub mod binance;
pub mod bybit;

use async_trait::async_trait;
use crate::config::MarketType;
use crate::error::Result;

#[async_trait]
pub trait RestClient: Send + Sync {
    async fn get_symbols(&self, market_type: MarketType) -> Result<Vec<String>>;
}
