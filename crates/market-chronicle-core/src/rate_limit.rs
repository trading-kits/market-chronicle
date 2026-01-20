use governor::{Quota, RateLimiter as GovernorRateLimiter, state::InMemoryState, clock::DefaultClock};
use governor::state::direct::NotKeyed;
use std::num::NonZeroU32;
use std::sync::Arc;

pub struct ConnectionRateLimiter {
    limiter: Arc<GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl ConnectionRateLimiter {
    pub fn new(connections_per_minute: u32) -> Self {
        let quota = Quota::per_minute(
            NonZeroU32::new(connections_per_minute)
                .expect("connections_per_minute must be > 0")
        );
        
        Self {
            limiter: Arc::new(GovernorRateLimiter::direct(quota)),
        }
    }
    
    pub async fn wait_for_permit(&self) {
        self.limiter.until_ready().await;
    }
    
    pub fn check_permit(&self) -> bool {
        self.limiter.check().is_ok()
    }
}

impl Clone for ConnectionRateLimiter {
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = ConnectionRateLimiter::new(60); // 60 per minute = 1 per second
        
        let start = Instant::now();
        
        // First request should be immediate
        limiter.wait_for_permit().await;
        let elapsed1 = start.elapsed();
        assert!(elapsed1.as_millis() < 100);
        
        // Second request should wait ~1 second
        limiter.wait_for_permit().await;
        let elapsed2 = start.elapsed();
        assert!(elapsed2.as_secs() >= 1);
    }
    
    #[test]
    fn test_check_permit() {
        let limiter = ConnectionRateLimiter::new(1);
        
        // First check should succeed
        assert!(limiter.check_permit());
        
        // Second check should fail (rate limited)
        assert!(!limiter.check_permit());
    }
}
