use std::net::SocketAddr;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::response::{IntoResponse, Response};
use eyre::{Result, WrapErr};
use http::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;

use crate::error::WebError;
use crate::state::AppState;

/// Hop-by-hop headers stripped before forwarding (RFC 7230 §6.1).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[tracing::instrument(
    skip_all,
    fields(method = %req.method(), path = %req.uri().path(), peer = %peer.ip()),
    err,
)]
pub(crate) async fn handle_public(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut req: Request,
) -> Result<Response, WebError> {
    let opener = {
        let guard = state.active().read().await;
        match guard.as_ref() {
            Some(active) if active.opener().is_alive() => active.opener().clone(),
            Some(_) => return Err(WebError::TunnelClosed),
            None => return Err(WebError::NoTunnel),
        }
    };

    sanitize_request_headers(&mut req, peer);

    let stream = opener
        .open()
        .await
        .wrap_err("Failed to open mux stream for inbound public request")?;

    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .wrap_err("Failed to negotiate http/1.1 over tunnel mux stream")?;

    tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::debug!(?err, "tunnel http1 connection ended");
        }
    });

    let resp = sender
        .send_request(req)
        .await
        .wrap_err("Failed to forward public request through tunnel")?;

    let (parts, body) = resp.into_parts();
    Ok(Response::from_parts(parts, Body::new(body)).into_response())
}

fn sanitize_request_headers(req: &mut Request, peer: SocketAddr) {
    let extra: Vec<HeaderName> = req
        .headers()
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',')
                .filter_map(|tok| tok.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();

    let headers = req.headers_mut();
    for name in HOP_BY_HOP {
        headers.remove(*name);
    }
    for name in extra {
        headers.remove(name);
    }

    if let Ok(value) = HeaderValue::from_str(&peer.ip().to_string()) {
        headers.append("x-forwarded-for", value);
    }
}

