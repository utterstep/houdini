use std::convert::Infallible;
use std::sync::Arc;

use futures::{SinkExt as _, StreamExt as _};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use secrecy::ExposeSecret;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use houdini_protocol::{Hello, HelloAck, Mux, PROTOCOL_VERSION, Role};

use crate::config::ClientConfig;
use crate::forward::Forwarder;
use crate::transport::{TungsteniteWsTransport, WsClient};

#[derive(Debug, displaydoc::Display, thiserror::Error)]
#[allow(clippy::doc_markdown)] // doc-strings here become user-facing Display output, not API docs
pub(crate) enum SessionError {
    /// failed to dial server: {0}
    Dial(#[source] WsError),
    /// websocket I/O failed during handshake: {0}
    Handshake(#[source] WsError),
    /// server rejected handshake: {0}
    Rejected(String),
    /// server closed before HelloAck arrived
    NoAck,
    /// malformed HelloAck: {0}
    BadAck(#[source] postcard::Error),
    /// internal: {0}
    Internal(#[from] eyre::Report),
}

#[tracing::instrument(skip_all, fields(server = %config.server_url(), local = %config.local_target()), err)]
pub(crate) async fn run_session(config: &ClientConfig) -> Result<(), SessionError> {
    let request = config
        .server_url()
        .as_str()
        .into_client_request()
        .map_err(SessionError::Dial)?;

    let (ws, response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(SessionError::Dial)?;
    tracing::debug!(status = %response.status(), "ws connected");

    let mut ws: WsClient = ws;

    let hello = Hello {
        protocol_version: PROTOCOL_VERSION,
        token: config.auth_token().expose_secret().to_owned(),
        client_name: config.client_name().clone(),
    };
    ws.send(Message::Binary(hello.encode()))
        .await
        .map_err(SessionError::Handshake)?;

    let ack = loop {
        let Some(msg) = ws.next().await else {
            return Err(SessionError::NoAck);
        };
        match msg.map_err(SessionError::Handshake)? {
            Message::Binary(bytes) => {
                break HelloAck::decode(&bytes).map_err(SessionError::BadAck)?;
            }
            Message::Close(_) => return Err(SessionError::NoAck),
            _ => {}
        }
    };

    let server_name = match ack {
        HelloAck::Ok { server_name } => server_name,
        HelloAck::Err { kind, message } => {
            return Err(SessionError::Rejected(format!("{kind:?}: {message}")));
        }
    };
    tracing::info!(server = %server_name, "tunnel up");

    let transport = TungsteniteWsTransport(ws);
    let mut mux = Mux::start(transport, Role::Acceptor);
    let forwarder = Arc::new(Forwarder::new(config.local_target().clone()));

    while let Some(stream) = mux.accept().await {
        spawn_stream_handler(stream, Arc::clone(&forwarder));
    }

    tracing::info!("tunnel closed");
    Ok(())
}

fn spawn_stream_handler(stream: houdini_protocol::MuxStream, forwarder: Arc<Forwarder>) {
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

