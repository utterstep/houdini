use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use bytes::Bytes;
use eyre::{Result, WrapErr};
use futures::StreamExt as _;
use secrecy::ExposeSecret;
use subtle::ConstantTimeEq;

use houdini_protocol::{Hello, HelloAck, HelloError, Mux, PROTOCOL_VERSION, Role};

use crate::state::{ActiveTunnel, AppState};
use crate::transport::AxumWsTransport;

#[tracing::instrument(skip_all)]
pub(crate) async fn handle_control(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(report) = run_session(socket, state).await {
            tracing::warn!(?report, "control session ended with error");
        }
    })
}

#[tracing::instrument(skip_all, err)]
async fn run_session(mut socket: WebSocket, state: AppState) -> Result<()> {
    let Some(hello) = recv_hello(&mut socket)
        .await
        .wrap_err("Failed to read Hello frame from incoming control connection")?
    else {
        tracing::debug!("client closed before sending Hello");
        return Ok(());
    };

    if hello.protocol_version != PROTOCOL_VERSION {
        send_ack(
            &mut socket,
            &HelloAck::Err {
                kind: HelloError::UnsupportedVersion,
                message: format!(
                    "server speaks v{PROTOCOL_VERSION}, client tried v{}",
                    hello.protocol_version
                ),
            },
        )
        .await
        .wrap_err("Failed to reply with version-mismatch HelloAck")?;
        return Ok(());
    }

    if !tokens_match(&hello.token, state.config().auth_token()) {
        send_ack(
            &mut socket,
            &HelloAck::Err {
                kind: HelloError::AuthFailed,
                message: "invalid token".into(),
            },
        )
        .await
        .wrap_err("Failed to reply with auth-failed HelloAck")?;
        tracing::warn!(client = ?hello.client_name, "rejected control upgrade: bad token");
        return Ok(());
    }

    if state.active().read().await.is_some() {
        send_ack(
            &mut socket,
            &HelloAck::Err {
                kind: HelloError::AlreadyConnected,
                message: "another client is already connected".into(),
            },
        )
        .await
        .wrap_err("Failed to reply with already-connected HelloAck")?;
        return Ok(());
    }

    send_ack(
        &mut socket,
        &HelloAck::Ok {
            server_name: state.config().server_name().clone(),
        },
    )
    .await
    .wrap_err("Failed to send HelloAck::Ok to accepted client")?;

    let transport = AxumWsTransport(socket);
    let mux = Mux::start(transport, Role::Initiator);
    let opener = mux.opener();
    let closed = mux.closed_signal();

    {
        let mut guard = state.active().write().await;
        *guard = Some(ActiveTunnel::new(opener, hello.client_name.clone()));
    }
    tracing::info!(client = ?hello.client_name, "tunnel registered");

    closed.notified().await;

    {
        let mut guard = state.active().write().await;
        *guard = None;
    }
    tracing::info!(client = ?hello.client_name, "tunnel closed");

    drop(mux);
    Ok(())
}

async fn recv_hello(socket: &mut WebSocket) -> Result<Option<Hello>> {
    while let Some(msg) = socket.next().await {
        let msg = msg.wrap_err("WebSocket recv failed before Hello arrived")?;
        match msg {
            Message::Binary(bytes) => {
                let hello = Hello::decode(&bytes)
                    .wrap_err("Failed to decode Hello frame as postcard payload")?;
                return Ok(Some(hello));
            }
            Message::Close(_) => return Ok(None),
            _ => {}
        }
    }
    Ok(None)
}

async fn send_ack(socket: &mut WebSocket, ack: &HelloAck) -> Result<()> {
    socket
        .send(Message::Binary(Bytes::from(ack.encode())))
        .await
        .wrap_err("Failed to send HelloAck over WebSocket")?;
    Ok(())
}

fn tokens_match(presented: &str, expected: &secrecy::SecretString) -> bool {
    let expected = expected.expose_secret();
    bool::from(presented.as_bytes().ct_eq(expected.as_bytes()))
}
