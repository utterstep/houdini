use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

mod config;
mod control;
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
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let config = ServerConfig::load(&cli.config)?;
    let listen = config.listen;
    let control_path = config.control_path.clone();
    tracing::info!(server_name = %config.server_name, %listen, control_path = %control_path, "houdini-server starting");

    let state = AppState::new(config);

    let app = axum::Router::new()
        .route(
            &control_path,
            axum::routing::any(control::handle_control),
        )
        .fallback(proxy::handle_public)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "houdini_server=info,houdini_protocol=info,axum=info".into());
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
    tracing::info!("shutdown signal received");
}
