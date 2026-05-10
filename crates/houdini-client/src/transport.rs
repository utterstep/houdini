use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub(crate) type WsClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Adapter from a `tokio-tungstenite` client `WebSocketStream` to a
/// `Sink<Vec<u8>> + Stream<Item = Result<Vec<u8>, WsError>>` so it can drive
/// [`houdini_protocol::Mux`].
pub(crate) struct TungsteniteWsTransport(pub WsClient);

impl Stream for TungsteniteWsTransport {
    type Item = Result<Vec<u8>, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None | Some(Ok(Message::Close(_)))) => return Poll::Ready(None),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(Some(Ok(Message::Binary(bytes)))) => {
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(Some(Ok(_))) => {}
            }
        }
    }
}

impl Sink<Vec<u8>> for TungsteniteWsTransport {
    type Error = WsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        Pin::new(&mut self.0).start_send(Message::Binary(item))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}
