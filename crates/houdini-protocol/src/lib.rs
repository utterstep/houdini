//! Wire protocol for the Houdini HTTP-NAT-escape tunnel.
//!
//! Two layers ride on top of a single WebSocket:
//!
//! 1. A **handshake** ([`Hello`] / [`HelloAck`]) sent as the first WebSocket
//!    binary frame from client to server, then server to client.
//! 2. A **stream multiplexer** ([`mux::Mux`]) that turns the WebSocket into a
//!    pool of byte streams. Each public HTTP request handled by the server
//!    opens a new mux stream, and the client treats each accepted stream as if
//!    it were an inbound TCP connection carrying HTTP/1.1.

pub mod frame;
pub mod handshake;
pub mod mux;

pub use frame::{Frame, StreamId};
pub use handshake::{Hello, HelloAck, HelloError, PROTOCOL_VERSION};
pub use mux::{Mux, MuxOpener, MuxStream, Role};
