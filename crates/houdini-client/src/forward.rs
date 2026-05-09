use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, StatusCode, Uri};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use url::Url;

/// Hop-by-hop headers stripped before forwarding (RFC 7230 §6.1).
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

/// Body type returned from the forwarding service.
pub type ServiceBody = BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

/// Forwards inbound tunneled HTTP/1.1 requests to a configured local target.
///
/// Currently HTTP-only (no TLS to the local target). HTTPS local targets are
/// validated at config load time but not yet implemented here — wire up
/// `hyper-rustls` here when needed.
pub struct Forwarder {
    target: Url,
    client: Client<HttpConnector, Incoming>,
}

impl Forwarder {
    pub fn new(target: Url) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { target, client }
    }

    pub async fn handle(self: Arc<Self>, mut req: Request<Incoming>) -> Response<ServiceBody> {
        match self.rewrite(&mut req) {
            Ok(()) => {}
            Err(err) => {
                tracing::warn!(?err, "tunneled request: bad URI");
                return error_response(StatusCode::BAD_GATEWAY, "bad upstream URI");
            }
        }

        match self.client.request(req).await {
            Ok(resp) => {
                let (parts, body) = resp.into_parts();
                let body: ServiceBody = body
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    .boxed();
                Response::from_parts(parts, body)
            }
            Err(err) => {
                tracing::warn!(?err, "local target request failed");
                error_response(StatusCode::BAD_GATEWAY, "local target unreachable")
            }
        }
    }

    fn rewrite(&self, req: &mut Request<Incoming>) -> anyhow::Result<()> {
        // Strip hop-by-hop headers.
        let extra: Vec<HeaderName> = req
            .headers()
            .get("connection")
            .and_then(|v| v.to_str().ok())
            .map(|s| {
                s.split(',')
                    .filter_map(|t| t.trim().parse().ok())
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

        let path_and_query = req
            .uri()
            .path_and_query()
            .map_or("/", http::uri::PathAndQuery::as_str);
        let base = self.target.as_str().trim_end_matches('/');
        let new_uri: Uri = format!("{base}{path_and_query}").parse()?;

        if let Some(authority) = new_uri.authority() {
            if let Ok(value) = HeaderValue::from_str(authority.as_str()) {
                req.headers_mut().insert(http::header::HOST, value);
            }
        }
        *req.uri_mut() = new_uri;

        Ok(())
    }
}

pub fn error_response(status: StatusCode, msg: &str) -> Response<ServiceBody> {
    let body: ServiceBody = Full::new(Bytes::from(format!("{msg}\n")))
        .map_err(|n| match n {})
        .boxed();
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .expect("static error response")
}
