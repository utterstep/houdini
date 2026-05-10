//! Stream multiplexer over a duplex byte transport.
//!
//! The transport is supplied as anything that is `Sink<Vec<u8>> +
//! Stream<Item = Result<Vec<u8>, _>>` — typically a WebSocket adapted to send
//! and receive whole binary frames. Each emitted/received `Vec<u8>` is exactly
//! one [`Frame`].
//!
//! The protocol is intentionally simple: a single u32 stream id, three control
//! frames (`Open`, `Fin`, `Reset`), one data frame (`Data`), plus ping/pong
//! liveness. There is no per-stream window — backpressure flows through the
//! underlying transport's TCP buffer plus a small bounded inbox per stream.
//!
//! Two roles share the same code:
//!
//! - [`Role::Initiator`] — opens new streams; obtains a [`MuxOpener`].
//! - [`Role::Acceptor`] — accepts streams that the initiator opens.
//!
//! In Houdini the **server** is the initiator (it opens a stream per public
//! HTTP request) and the **client** behind NAT is the acceptor.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::PollSender;

use crate::frame::{Frame, StreamId};

/// Outbound channel depth (frames waiting to hit the wire).
const OUTBOUND_BACKLOG: usize = 128;
/// Per-stream inbox depth (data chunks waiting for the local reader).
const PER_STREAM_INBOX: usize = 32;
/// Maximum payload size of a single Data frame. Larger writes are split.
const MAX_DATA_CHUNK: usize = 64 * 1024;

#[derive(Debug, Clone, Copy)]
pub enum Role {
    /// Opens new streams.
    Initiator,
    /// Accepts streams opened by the peer.
    Acceptor,
}

#[derive(Debug)]
enum InboxMsg {
    Data(Bytes),
    Fin,
    Reset,
}

type StreamMap = Arc<Mutex<HashMap<StreamId, mpsc::Sender<InboxMsg>>>>;

/// Result of [`Mux::start`]. Hold both halves alive for the duration of the
/// session — dropping `MuxOpener` on the initiator side, or `Mux` itself, will
/// tear down the underlying I/O task and signal `closed()`.
pub struct Mux {
    closed: Arc<Notify>,
    accept_rx: mpsc::Receiver<MuxStream>,
    /// Kept alive only to drop on shutdown.
    _io_task: JoinHandle<()>,
    /// One opener handle is always retained so the initiator role can clone it.
    opener: MuxOpener,
}

impl Mux {
    /// Start a multiplexer driving the given duplex transport.
    pub fn start<S, E>(transport: S, role: Role) -> Self
    where
        S: Sink<Vec<u8>, Error = E>
            + Stream<Item = Result<Vec<u8>, E>>
            + Send
            + Unpin
            + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let (out_tx, out_rx) = mpsc::channel::<Frame>(OUTBOUND_BACKLOG);
        let (accept_tx, accept_rx) = mpsc::channel::<MuxStream>(32);
        let streams: StreamMap = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(Notify::new());
        let next_id = Arc::new(Mutex::new(match role {
            Role::Initiator => 1u32,
            // Acceptor seed is unused (acceptors never allocate outbound IDs),
            // but pick an even value so `Initiator`/`Acceptor` ID spaces are
            // disjoint should we ever support both directions.
            Role::Acceptor => 2u32,
        }));

        let opener = MuxOpener {
            out_tx: out_tx.clone(),
            streams: Arc::clone(&streams),
            next_id: Arc::clone(&next_id),
        };

        let io_task = tokio::spawn(io_loop(
            transport,
            out_rx,
            out_tx,
            accept_tx,
            streams,
            Arc::clone(&closed),
        ));

        Self {
            closed,
            accept_rx,
            _io_task: io_task,
            opener,
        }
    }

    /// Cloneable handle for opening new streams. Useful for the initiator side
    /// (server) which shares the opener across many request handlers.
    pub fn opener(&self) -> MuxOpener {
        self.opener.clone()
    }

    /// Wait for the next stream opened by the peer. Returns `None` when the
    /// transport has died.
    pub async fn accept(&mut self) -> Option<MuxStream> {
        self.accept_rx.recv().await
    }

    /// Resolves when the underlying transport closes for any reason.
    pub fn closed_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.closed)
    }
}

/// Cloneable handle for opening new streams.
#[derive(Clone)]
pub struct MuxOpener {
    out_tx: mpsc::Sender<Frame>,
    streams: StreamMap,
    next_id: Arc<Mutex<StreamId>>,
}

impl MuxOpener {
    /// Allocate a new stream ID, send `Open` to the peer, and return a
    /// [`MuxStream`] for it.
    pub async fn open(&self) -> io::Result<MuxStream> {
        let id = {
            let mut guard = self.next_id.lock().expect("stream id lock");
            let id = *guard;
            *guard = id
                .checked_add(2)
                .ok_or_else(|| io::Error::other("stream id space exhausted"))?;
            id
        };

        let (inbox_tx, inbox_rx) = mpsc::channel(PER_STREAM_INBOX);
        self.streams
            .lock()
            .expect("streams lock")
            .insert(id, inbox_tx);

        if self.out_tx.send(Frame::Open { stream_id: id }).await.is_err() {
            self.streams.lock().expect("streams lock").remove(&id);
            return Err(io::Error::other("mux transport closed"));
        }

        Ok(MuxStream::new(
            id,
            inbox_rx,
            self.out_tx.clone(),
            Arc::clone(&self.streams),
        ))
    }

    pub fn is_alive(&self) -> bool {
        !self.out_tx.is_closed()
    }
}

/// A single multiplexed byte stream.
///
/// Implements `AsyncRead + AsyncWrite`, so the application layer (hyper, etc.)
/// can use it as if it were a TCP connection.
pub struct MuxStream {
    id: StreamId,
    rx: mpsc::Receiver<InboxMsg>,
    out: PollSender<Frame>,
    /// Direct sender used only on Drop to send a Reset.
    raw_out: mpsc::Sender<Frame>,
    streams: StreamMap,
    read_buf: Bytes,
    read_eof: bool,
    write_closed: bool,
    /// Set when the local side shuts down cleanly so Drop doesn't send a Reset.
    finished_cleanly: bool,
}

impl MuxStream {
    fn new(
        id: StreamId,
        rx: mpsc::Receiver<InboxMsg>,
        out: mpsc::Sender<Frame>,
        streams: StreamMap,
    ) -> Self {
        Self {
            id,
            rx,
            out: PollSender::new(out.clone()),
            raw_out: out,
            streams,
            read_buf: Bytes::new(),
            read_eof: false,
            write_closed: false,
            finished_cleanly: false,
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }
}

impl AsyncRead for MuxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.read_buf.is_empty() {
                let n = std::cmp::min(self.read_buf.len(), buf.remaining());
                let chunk = self.read_buf.split_to(n);
                buf.put_slice(&chunk);
                return Poll::Ready(Ok(()));
            }
            if self.read_eof {
                return Poll::Ready(Ok(()));
            }
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.read_eof = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(InboxMsg::Data(b))) => {
                    self.read_buf = b;
                }
                Poll::Ready(Some(InboxMsg::Fin)) => {
                    self.read_eof = true;
                }
                Poll::Ready(Some(InboxMsg::Reset)) => {
                    self.read_eof = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "mux stream reset",
                    )));
                }
            }
        }
    }
}

impl AsyncWrite for MuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "mux stream write half closed",
            )));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }

        match self.out.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => {
                return Poll::Ready(Err(io::Error::other("mux transport closed")));
            }
            Poll::Ready(Ok(())) => {}
        }

        let n = std::cmp::min(data.len(), MAX_DATA_CHUNK);
        let frame = Frame::Data {
            stream_id: self.id,
            payload: data[..n].to_vec(),
        };
        if self.out.send_item(frame).is_err() {
            return Poll::Ready(Err(io::Error::other("mux transport closed")));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if self.write_closed {
            return Poll::Ready(Ok(()));
        }
        match self.out.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => {
                self.write_closed = true;
                self.finished_cleanly = true;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Ok(())) => {}
        }
        let id = self.id;
        // Best-effort Fin: if the channel was closed between reserving and
        // sending, the peer is already going away and there's nothing to
        // surface to the caller.
        let _ = self.out.send_item(Frame::Fin { stream_id: id });
        self.write_closed = true;
        self.finished_cleanly = true;
        Poll::Ready(Ok(()))
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        self.streams.lock().expect("streams lock").remove(&self.id);
        if !self.finished_cleanly {
            // Best-effort reset to the peer; ignore failures.
            let _ = self.raw_out.try_send(Frame::Reset { stream_id: self.id });
        }
    }
}

async fn io_loop<S, E>(
    mut transport: S,
    mut out_rx: mpsc::Receiver<Frame>,
    out_tx: mpsc::Sender<Frame>,
    accept_tx: mpsc::Sender<MuxStream>,
    streams: StreamMap,
    closed: Arc<Notify>,
) where
    S: Sink<Vec<u8>, Error = E>
        + Stream<Item = Result<Vec<u8>, E>>
        + Send
        + Unpin
        + 'static,
    E: std::fmt::Display + Send + 'static,
{
    loop {
        tokio::select! {
            outbound = out_rx.recv() => {
                let Some(frame) = outbound else { break };
                let bytes = frame.encode();
                if let Err(err) = transport.send(bytes).await {
                    tracing::warn!(%err, "mux: transport send failed");
                    break;
                }
            }
            inbound = transport.next() => {
                let Some(item) = inbound else {
                    tracing::debug!("mux: transport stream ended");
                    break;
                };
                let bytes = match item {
                    Ok(b) => b,
                    Err(err) => {
                        tracing::warn!(%err, "mux: transport recv failed");
                        break;
                    }
                };
                let frame = match Frame::decode(&bytes) {
                    Ok(f) => f,
                    Err(err) => {
                        tracing::warn!(%err, "mux: malformed frame, ignoring");
                        continue;
                    }
                };
                handle_inbound(frame, &streams, &accept_tx, &out_tx).await;
            }
        }
    }

    // Best-effort transport close on shutdown — if it errors there's no
    // reasonable recovery; readers wake via the cleared `streams` map below.
    let _ = transport.close().await;
    streams.lock().expect("streams lock").clear();
    closed.notify_waiters();
}

async fn handle_inbound(
    frame: Frame,
    streams: &StreamMap,
    accept_tx: &mpsc::Sender<MuxStream>,
    out_tx: &mpsc::Sender<Frame>,
) {
    match frame {
        Frame::Open { stream_id } => {
            let (inbox_tx, inbox_rx) = mpsc::channel(PER_STREAM_INBOX);
            streams
                .lock()
                .expect("streams lock")
                .insert(stream_id, inbox_tx);
            let stream = MuxStream::new(
                stream_id,
                inbox_rx,
                out_tx.clone(),
                Arc::clone(streams),
            );
            if accept_tx.send(stream).await.is_err() {
                tracing::warn!(stream_id, "mux: accept queue closed; dropping stream");
            }
        }
        Frame::Data { stream_id, payload } => {
            let sender = streams
                .lock()
                .expect("streams lock")
                .get(&stream_id)
                .cloned();
            if let Some(tx) = sender
                && tx
                    .send(InboxMsg::Data(Bytes::from(payload)))
                    .await
                    .is_err()
            {
                streams.lock().expect("streams lock").remove(&stream_id);
            }
        }
        Frame::Fin { stream_id } => {
            let sender = streams
                .lock()
                .expect("streams lock")
                .get(&stream_id)
                .cloned();
            if let Some(tx) = sender {
                // If the local reader has been dropped, the Fin is moot.
                let _ = tx.send(InboxMsg::Fin).await;
            }
        }
        Frame::Reset { stream_id } => {
            let sender = streams
                .lock()
                .expect("streams lock")
                .remove(&stream_id);
            if let Some(tx) = sender {
                // Same: if the reader is gone, drop carries the same signal.
                let _ = tx.send(InboxMsg::Reset).await;
            }
        }
        Frame::Ping { token } => {
            // Pong is best-effort; if the outbound channel is closed the io
            // loop is already shutting down and will exit on its next tick.
            let _ = out_tx.send(Frame::Pong { token }).await;
        }
        Frame::Pong { .. } => {}
    }
}
