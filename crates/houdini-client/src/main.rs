use std::path::PathBuf;

use clap::Parser;
use tokio::time::sleep;

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
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let config = ClientConfig::load(&cli.config)?;
    tracing::info!(server = %config.server_url, local_target = %config.local_target, "houdini-client starting");

    let mut backoff = config.min_backoff();
    let max_backoff = config.max_backoff();

    loop {
        tokio::select! {
            res = tunnel::run_session(&config) => {
                match res {
                    Ok(()) => {
                        tracing::info!("session ended; reconnecting after {:?}", backoff);
                    }
                    Err(err) => {
                        tracing::warn!(?err, "session error; reconnecting after {:?}", backoff);
                    }
                }
            }
            () = shutdown_signal() => {
                tracing::info!("shutdown signal received");
                return Ok(());
            }
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "houdini_client=info,houdini_protocol=info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
}

