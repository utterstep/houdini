use std::pin::Pin;
use std::task::{Context, Poll};

use axum::extract::ws::{Message, WebSocket};
use bytes::Bytes;
use futures::{Sink, Stream};

/// Adapter from axum's [`WebSocket`] to a `Sink<Vec<u8>> + Stream<Item =
/// Result<Vec<u8>, _>>` so it can drive [`houdini_protocol::Mux`].
///
/// Inbound non-binary frames (Text, Ping, Pong) are dropped silently; outbound
/// payloads are always sent as `Message::Binary`.
pub(crate) struct AxumWsTransport(pub WebSocket);

impl Stream for AxumWsTransport {
    type Item = Result<Vec<u8>, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None | Some(Ok(Message::Close(_)))) => return Poll::Ready(None),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(Some(Ok(Message::Binary(bytes)))) => {
                    return Poll::Ready(Some(Ok(bytes.to_vec())));
                }
                Poll::Ready(Some(Ok(_))) => {}
            }
        }
    }
}

impl Sink<Vec<u8>> for AxumWsTransport {
    type Error = axum::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        Pin::new(&mut self.0).start_send(Message::Binary(Bytes::from(item)))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}
