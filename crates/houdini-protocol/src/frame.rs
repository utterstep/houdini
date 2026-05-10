use serde::{Deserialize, Serialize};

/// Identifier for a multiplexed stream. The initiator (server) allocates IDs.
pub type StreamId = u32;

/// Multiplexer frame. Each WebSocket binary message after the handshake is
/// exactly one [`Frame`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    /// Initiator opens a new stream. The acceptor side spawns a stream task.
    Open { stream_id: StreamId },
    /// Bytes for a stream. Either direction.
    Data { stream_id: StreamId, payload: Vec<u8> },
    /// No more bytes will be sent in this direction. Reader sees EOF.
    Fin { stream_id: StreamId },
    /// Hard reset — both directions are torn down.
    Reset { stream_id: StreamId },
    /// Liveness probe. Receivers must reply with [`Frame::Pong`] carrying the
    /// same token.
    Ping { token: u32 },
    Pong { token: u32 },
}

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("frame encoding never fails")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}
