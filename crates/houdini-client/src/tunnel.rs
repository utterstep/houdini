use std::convert::Infallible;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use houdini_protocol::{Hello, HelloAck, Mux, PROTOCOL_VERSION, Role};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use thiserror::Error;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

use crate::config::ClientConfig;
use crate::forward::Forwarder;
use crate::transport::{TungsteniteWsTransport, WsClient};

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("websocket connect: {0}")]
    Connect(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("server rejected handshake: {0}")]
    Rejected(String),
    #[error("server closed before HelloAck")]
    NoAck,
    #[error("malformed HelloAck: {0}")]
    BadAck(#[from] postcard::Error),
}

pub async fn run_session(config: &ClientConfig) -> Result<(), SessionError> {
    let request = config.server_url.as_str().into_client_request()?;
    let (ws, response) = tokio_tungstenite::connect_async(request).await?;
    tracing::debug!(status = %response.status(), "ws connected");

    let mut ws: WsClient = ws;

    // Send Hello.
    let hello = Hello {
        protocol_version: PROTOCOL_VERSION,
        token: config.auth_token.clone(),
        client_name: config.client_name.clone(),
    };
    ws.send(Message::Binary(hello.encode())).await?;

    // Wait for HelloAck.
    let ack = loop {
        let Some(msg) = ws.next().await else {
            return Err(SessionError::NoAck);
        };
        match msg? {
            Message::Binary(bytes) => break HelloAck::decode(&bytes)?,
            Message::Close(_) => return Err(SessionError::NoAck),
            _ => continue,
        }
    };

    let server_name = match ack {
        HelloAck::Ok { server_name } => server_name,
        HelloAck::Err { kind, message } => {
            return Err(SessionError::Rejected(format!("{kind:?}: {message}")));
        }
    };
    tracing::info!(server = %server_name, local_target = %config.local_target, "tunnel up");

    let transport = TungsteniteWsTransport(ws);
    let mut mux = Mux::start(transport, Role::Acceptor);
    let forwarder = Arc::new(Forwarder::new(config.local_target.clone()));

    while let Some(stream) = mux.accept().await {
        let forwarder = Arc::clone(&forwarder);
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let f = Arc::clone(&forwarder);
                async move { Ok::<_, Infallible>(f.handle(req).await) }
            });
            if let Err(err) = http1::Builder::new()
                .keep_alive(false)
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(?err, "tunnel http1 server connection ended");
            }
        });
    }

    tracing::info!("tunnel closed");
    Ok(())
}
