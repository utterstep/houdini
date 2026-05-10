use std::sync::Arc;

use bytes::Bytes;
use eyre::{Result, WrapErr};
use http::{HeaderName, HeaderValue, StatusCode, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use url::Url;

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

pub(crate) type ServiceBody = BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

/// Forwards inbound tunneled HTTP/1.1 requests to a configured local target.
///
/// HTTP-only for v0.1; HTTPS local targets are accepted at config-load time
/// but only `HttpConnector` is wired here. Add `hyper-rustls` to extend.
pub(crate) struct Forwarder {
    target: Url,
    client: Client<HttpConnector, Incoming>,
}

impl Forwarder {
    pub(crate) fn new(target: Url) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { target, client }
    }

    #[tracing::instrument(
        skip_all,
        fields(method = %req.method(), path = %req.uri().path()),
    )]
    pub(crate) async fn handle(self: Arc<Self>, mut req: Request<Incoming>) -> Response<ServiceBody> {
        if let Err(report) = self.rewrite(&mut req) {
            tracing::warn!(?report, "tunneled request: bad URI rewrite");
            return error_response(StatusCode::BAD_GATEWAY, "bad upstream URI");
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
                tracing::warn!(?err, target = %self.target, "local target request failed");
                error_response(StatusCode::BAD_GATEWAY, "local target unreachable")
            }
        }
    }

    fn rewrite(&self, req: &mut Request<Incoming>) -> Result<()> {
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
        let new_uri: Uri = format!("{base}{path_and_query}")
            .parse()
            .wrap_err_with(|| {
                format!(
                    "Failed to build outbound URI for tunneled request (base='{base}', path='{path_and_query}')"
                )
            })?;

        if let Some(authority) = new_uri.authority()
            && let Ok(value) = HeaderValue::from_str(authority.as_str())
        {
            req.headers_mut().insert(http::header::HOST, value);
        }
        *req.uri_mut() = new_uri;

        Ok(())
    }
}

fn error_response(status: StatusCode, msg: &str) -> Response<ServiceBody> {
    let body: ServiceBody = Full::new(Bytes::from(format!("{msg}\n")))
        .map_err(|n| match n {})
        .boxed();
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .expect("static error response cannot fail to build")
}
