use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use eyre::{Result, WrapErr};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_tree::HierarchicalLayer;

mod config;
mod forward;
mod transport;
mod tunnel;

use config::ClientConfig;

#[derive(Parser, Debug)]
#[command(name = "houdini-client", version, about)]
struct Cli {
    #[arg(short, long, env = "HOUDINI_CLIENT_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install().wrap_err("Failed to install color-eyre report hook")?;
    init_tracing();

    // Pre-install ring as the default rustls CryptoProvider. rustls auto-installs
    // when exactly one provider feature is enabled, but doing it eagerly avoids
    // a runtime panic if any future dep bumps that invariant.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let config = ClientConfig::load(&cli.config)
        .wrap_err_with(|| format!("Failed to load client config from '{}'", cli.config.display()))?;

    tracing::info!(server = %config.server_url(), local_target = %config.local_target(), "houdini-client starting");

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    install_shutdown_handler(shutdown_tx)?;

    let min_backoff = config.min_backoff();
    let max_backoff = config.max_backoff();
    // A session that lived longer than this is treated as a real, stable run;
    // its reconnect should restart from the floor instead of inheriting the
    // accumulated backoff from earlier failures.
    let stable_threshold = min_backoff * 10;
    let mut backoff = min_backoff;

    loop {
        let started = Instant::now();
        tokio::select! {
            res = tunnel::run_session(&config) => {
                match res {
                    Ok(()) => tracing::info!("session ended; reconnecting in {:?}", backoff),
                    Err(err) => tracing::warn!(?err, "session error; reconnecting in {:?}", backoff),
                }
            }
            _ = shutdown_rx.changed() => {
                tracing::info!("shutdown signal received");
                return Ok(());
            }
        }

        if started.elapsed() >= stable_threshold {
            backoff = min_backoff;
        }

        tokio::select! {
            () = sleep(backoff) => {}
            _ = shutdown_rx.changed() => {
                tracing::info!("shutdown signal received during backoff");
                return Ok(());
            }
        }
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "houdini_client=info,houdini_protocol=info".into());

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_error::ErrorLayer::default())
        .with(
            HierarchicalLayer::new(2)
                .with_targets(true)
                .with_bracketed_fields(true),
        )
        .init();
}

fn install_shutdown_handler(tx: watch::Sender<bool>) -> Result<()> {
    ctrlc::set_handler(move || {
        let _ = tx.send(true);
    })
    .wrap_err("Failed to install Ctrl+C / SIGTERM handler")?;
    Ok(())
}
