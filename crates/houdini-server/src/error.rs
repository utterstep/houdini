use axum::response::{IntoResponse, Response};
use http::StatusCode;

#[derive(Debug, displaydoc::Display, thiserror::Error)]
pub(crate) enum WebError {
    /// no houdini client connected
    NoTunnel,
    /// tunnel transport closed
    TunnelClosed,
    /// internal error: {0}
    Internal(#[from] eyre::Report),
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let status = StatusCode::BAD_GATEWAY;
        let public = match &self {
            Self::NoTunnel => "no houdini client connected\n",
            Self::TunnelClosed => "tunnel closed\n",
            Self::Internal(_) => "tunnel error\n",
        };

        match &self {
            Self::Internal(report) => tracing::warn!(error = ?report, %status, "internal request failure"),
            other => tracing::warn!(error = %other, %status, "request rejected"),
        }

        (status, public).into_response()
    }
}
