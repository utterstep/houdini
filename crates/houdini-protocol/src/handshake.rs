use serde::{Deserialize, Serialize};

/// Protocol version. Bumped when the wire format changes incompatibly.
pub const PROTOCOL_VERSION: u16 = 1;

/// First message sent by the client immediately after the WebSocket upgrade
/// completes. Encoded with `postcard` and shipped as a single binary frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u16,
    pub token: String,
    /// Optional human-readable client identifier — purely informational, used
    /// in server logs.
    pub client_name: Option<String>,
}

/// Server reply to a [`Hello`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HelloAck {
    Ok {
        server_name: String,
    },
    Err {
        kind: HelloError,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HelloError {
    UnsupportedVersion,
    AuthFailed,
    AlreadyConnected,
}

impl Hello {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("Hello encoding never fails")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

impl HelloAck {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("HelloAck encoding never fails")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}
