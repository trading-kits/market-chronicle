mod replayer;

use anyhow::Result;
use clap::Parser;
use market_chronicle_core::Config;
use tracing_subscriber::EnvFilter;

use replayer::Replayer;

#[derive(Parser)]
#[command(name = "market-chronicle-replay")]
#[command(about = "Replay market data from S3 storage", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    
    /// Start time (RFC3339 format, e.g., 2024-01-15T00:00:00Z)
    #[arg(short, long)]
    start_time: String,
    
    /// End time (RFC3339 format, e.g., 2024-01-15T23:59:59Z)
    #[arg(short, long)]
    end_time: String,
    
    /// Filter by exchange name (optional)
    #[arg(short = 'x', long)]
    exchange: Option<String>,
    
    /// Filter by symbol (optional)
    #[arg(short = 'y', long)]
    symbol: Option<String>,
    
    /// Filter by stream type (optional)
    #[arg(short = 't', long)]
    stream_type: Option<String>,
    
    /// Replay speed multiplier (1.0 = real-time, 2.0 = 2x speed, 0.5 = half speed)
    #[arg(long, default_value = "1.0")]
    speed: f64,
    
    /// Output file path (optional, defaults to stdout)
    #[arg(short, long)]
    output: Option<String>,
    
    /// Print statistics only (don't output messages)
    #[arg(long)]
    stats_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
    
    let args = Args::parse();
    
    // Load configuration
    let config = Config::from_file(&args.config)?;
    
    // Create replayer
    let replayer = Replayer::new(config.s3).await?;
    
    // Run replay
    replayer.replay(args).await?;
    
    Ok(())
}
