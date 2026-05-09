use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::StreamExt as _;
use houdini_protocol::{Hello, HelloAck, HelloError, Mux, PROTOCOL_VERSION, Role};

use crate::state::{ActiveTunnel, AppState};
use crate::transport::AxumWsTransport;

pub async fn handle_control(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(err) = run_session(socket, state).await {
            tracing::warn!(?err, "control session ended with error");
        }
    })
}

async fn run_session(mut socket: WebSocket, state: AppState) -> anyhow::Result<()> {
    let Some(hello) = recv_hello(&mut socket).await? else {
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
        .await?;
        return Ok(());
    }
    if !constant_time_eq(hello.token.as_bytes(), state.config.auth_token.as_bytes()) {
        send_ack(
            &mut socket,
            &HelloAck::Err {
                kind: HelloError::AuthFailed,
                message: "invalid token".into(),
            },
        )
        .await?;
        tracing::warn!("rejected control upgrade: bad token");
        return Ok(());
    }

    if state.active.read().await.is_some() {
        send_ack(
            &mut socket,
            &HelloAck::Err {
                kind: HelloError::AlreadyConnected,
                message: "another client is already connected".into(),
            },
        )
        .await?;
        return Ok(());
    }

    send_ack(
        &mut socket,
        &HelloAck::Ok {
            server_name: state.config.server_name.clone(),
        },
    )
    .await?;

    let transport = AxumWsTransport(socket);
    let mux = Mux::start(transport, Role::Initiator);
    let opener = mux.opener();
    let closed = mux.closed_signal();

    {
        let mut guard = state.active.write().await;
        *guard = Some(ActiveTunnel {
            opener,
            client_name: hello.client_name.clone(),
        });
    }
    tracing::info!(client = ?hello.client_name, "tunnel registered");

    closed.notified().await;

    {
        let mut guard = state.active.write().await;
        *guard = None;
    }
    tracing::info!(client = ?hello.client_name, "tunnel closed");

    // Hold the mux until here so its IO task isn't dropped early.
    drop(mux);

    Ok(())
}

async fn recv_hello(socket: &mut WebSocket) -> anyhow::Result<Option<Hello>> {
    while let Some(msg) = socket.next().await {
        match msg? {
            Message::Binary(bytes) => {
                let hello = Hello::decode(&bytes)
                    .map_err(|e| anyhow::anyhow!("malformed Hello: {e}"))?;
                return Ok(Some(hello));
            }
            Message::Close(_) => return Ok(None),
            _ => continue,
        }
    }
    Ok(None)
}

async fn send_ack(socket: &mut WebSocket, ack: &HelloAck) -> anyhow::Result<()> {
    let bytes = ack.encode();
    socket.send(Message::Binary(Bytes::from(bytes))).await?;
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
