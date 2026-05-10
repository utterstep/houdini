use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use eyre::{Result, WrapErr};
use tokio::sync::watch;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_tree::HierarchicalLayer;

mod config;
mod control;
mod error;
mod proxy;
mod state;
mod transport;

use config::ServerConfig;
use state::AppState;

#[derive(Parser, Debug)]
#[command(name = "houdini-server", version, about)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, env = "HOUDINI_SERVER_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install().wrap_err("Failed to install color-eyre report hook")?;
    init_tracing();

    let cli = Cli::parse();
    let config = ServerConfig::load(&cli.config)
        .wrap_err_with(|| format!("Failed to load server config from '{}'", cli.config.display()))?;
    let listen = *config.listen();
    let control_path = config.control_path().clone();
    let server_name = config.server_name().clone();

    tracing::info!(%server_name, %listen, %control_path, "houdini-server starting");

    let state = AppState::new(config);

    let app = axum::Router::new()
        .route(
            &control_path,
            axum::routing::any(control::handle_control),
        )
        .fallback(proxy::handle_public)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .wrap_err_with(|| format!("Failed to bind houdini-server listener on {listen}"))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    install_shutdown_handler(shutdown_tx)?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
    .await
    .wrap_err("Failed while serving public traffic")?;

    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "houdini_server=info,houdini_protocol=info,axum=info".into());

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

async fn wait_for_shutdown(mut rx: watch::Receiver<bool>) {
    let _ = rx.changed().await;
    tracing::info!("shutdown signal received");
}
