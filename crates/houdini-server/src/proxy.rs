use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;

use crate::state::AppState;

/// Hop-by-hop headers that must not survive a forward proxy hop, per RFC 7230.
static HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub async fn handle_public(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut req: Request,
) -> Response {
    let opener = {
        let guard = state.active.read().await;
        match guard.as_ref() {
            Some(t) if t.opener.is_alive() => t.opener.clone(),
            _ => {
                return (
                    StatusCode::BAD_GATEWAY,
                    "no houdini client connected\n",
                )
                    .into_response();
            }
        }
    };

    sanitize_request_headers(&mut req, peer);

    let stream = match opener.open().await {
        Ok(s) => s,
        Err(err) => {
            tracing::warn!(?err, "failed to open mux stream");
            return (StatusCode::BAD_GATEWAY, "tunnel busy\n").into_response();
        }
    };

    let io = TokioIo::new(stream);
    let (mut sender, conn) = match hyper::client::conn::http1::handshake(io).await {
        Ok(pair) => pair,
        Err(err) => {
            tracing::warn!(?err, "http1 handshake on mux stream failed");
            return (StatusCode::BAD_GATEWAY, "tunnel handshake failed\n").into_response();
        }
    };
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::debug!(?err, "tunnel http1 connection ended");
        }
    });

    match sender.send_request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Err(err) => {
            tracing::warn!(?err, "tunneled send_request failed");
            (StatusCode::BAD_GATEWAY, "tunnel request failed\n").into_response()
        }
    }
}

fn sanitize_request_headers(req: &mut Request, peer: SocketAddr) {
    // Headers listed in `Connection: ...` are also hop-by-hop.
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
        // Append rather than replace, as some upstream chains already set XFF.
        headers.append("x-forwarded-for", value);
    }
}
