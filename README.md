# houdini

A NAT-escape tunnel that runs over plain HTTP(S). It exists for one specific
shape of problem: you have a VPS where the only inbound path from the
internet is a shared HAProxy in HTTP/HTTPS mode (no raw TCP, no public
IPv4), and you want to expose a service running on a NAT'd machine
elsewhere. ngrok / rathole / frp all assume you can listen on arbitrary TCP
ports somewhere on the internet — Houdini does not.

```
       public           HAProxy            houdini-server          mux           houdini-client       local
   ┌──────────┐      ┌──────────┐         ┌──────────────┐    over WebSocket   ┌──────────────┐    ┌────────┐
   │  user    ├─TLS──▶  TLS     ├──HTTP/1─▶ axum router  ├────────────────────▶│ hyper http1  ├────▶ app    │
   │  agent   │      │  term.   │  + WS    │ + mux init  │    (one stream     │ + forwarder  │    │ :3000  │
   └──────────┘      └──────────┘ Upgrade  └──────────────┘    per request)    └──────────────┘    └────────┘
```

## Pieces

- **`houdini-server`** runs on the VPS behind HAProxy. It binds plain HTTP
  on a configured port. One path (`/_houdini/v1/control` by default) is the
  WebSocket control endpoint; everything else is reverse-proxied through
  the active tunnel.
- **`houdini-client`** runs on the NAT'd machine. It dials the server's
  control URL, presents a shared bearer token, and forwards every inbound
  public request to a configured local URL.
- **`houdini-protocol`** is the wire protocol library both binaries depend
  on: a `Hello` / `HelloAck` handshake plus a small stream multiplexer.

## How it works

1. The client opens a WebSocket to the server. HAProxy in HTTP mode passes
   the `Upgrade: websocket` request through unchanged.
2. As the first binary frame the client sends a `Hello` carrying the
   protocol version, shared secret, and an optional client name. The
   server replies with `HelloAck::Ok` or rejects.
3. Both ends start the same multiplexer state machine over the now-open
   WebSocket. The server side opens streams (initiator); the client side
   accepts them.
4. For every public HTTP request the server receives, it opens a new mux
   stream, runs `hyper::client::conn::http1::handshake` over it, sends the
   request, and pipes the response body back to the public client.
5. The client side accepts each new mux stream, runs
   `hyper::server::conn::http1` on it, and the service handler forwards
   the request to `local_target` over a real TCP connection.

Only one client may be registered at a time. The next connecting client
gets `HelloAck::Err { kind: AlreadyConnected }`.

Currently HTTP/1.1 only. WebSocket forwarding through the tunnel (i.e.
public users running WebSocket clients against the tunneled service) is
not yet supported because hyper http1 upgrades aren't bridged across the
mux. Plain HTTP/1.1 request-response is fully supported.

## Install

### Pre-built binaries

Each tagged release attaches per-target tarballs and matching sha256 files to
the GitHub release. Currently built:

| Target                          | When to pick it                                                  |
|---------------------------------|------------------------------------------------------------------|
| `x86_64-unknown-linux-musl`     | Static-pie, no glibc dep — deploys to any modern x86_64 Linux. The default for tiny VPS instances; server idles fine under 128 MiB RAM. |
| `x86_64-unknown-linux-gnu`      | Dynamically linked against glibc — slightly smaller, native on most distros. |
| `aarch64-apple-darwin`          | Apple silicon (M-series) Macs.                                   |

```sh
target=x86_64-unknown-linux-musl   # or x86_64-unknown-linux-gnu, aarch64-apple-darwin
ver=v0.1.0

curl -sSLO "https://github.com/utterstep/houdini/releases/download/${ver}/houdini-${ver}-${target}.tar.gz"
curl -sSLO "https://github.com/utterstep/houdini/releases/download/${ver}/houdini-${ver}-${target}.tar.gz.sha256"
sha256sum -c "houdini-${ver}-${target}.tar.gz.sha256"   # `shasum -a 256 -c` on macOS
tar -xzf "houdini-${ver}-${target}.tar.gz"
sudo install -m 0755 "houdini-${ver}-${target}/houdini-server" /usr/local/bin/
sudo install -m 0755 "houdini-${ver}-${target}/houdini-client" /usr/local/bin/
```

On macOS, Gatekeeper will quarantine the unsigned binary the first time you
try to run it. Either right-click → Open in Finder, or strip the attribute
manually: `xattr -d com.apple.quarantine /usr/local/bin/houdini-*`.

The release workflow also accepts a manual `workflow_dispatch` run; the
resulting binaries are uploaded per-target as workflow artifacts named
`houdini-dev-<sha>-<target>` so untagged builds are fetchable from the GHA
run page.

### From source

Requires Rust 1.95+ (pinned via `rust-toolchain.toml`).

```sh
cargo build --release --workspace
```

The release artifacts are at `target/release/houdini-server` and
`target/release/houdini-client`.

To reproduce the musl build locally:

```sh
sudo apt-get install -y musl-tools
rustup target add x86_64-unknown-linux-musl
CC_x86_64_unknown_linux_musl=musl-gcc \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
cargo build --release --workspace --target x86_64-unknown-linux-musl --bins
```

## Run

Server, on the VPS:

```sh
houdini-server --config /etc/houdini/server.toml
```

See `examples/server.toml` for the full config schema. Make sure HAProxy
is configured to forward both regular HTTP/1.1 and WebSocket upgrades to
the `listen` address. The config path can also be supplied via
`HOUDINI_SERVER_CONFIG`.

Client, on the NAT'd machine:

```sh
houdini-client --config ~/.config/houdini/client.toml
```

Env-var fallback: `HOUDINI_CLIENT_CONFIG`. See `examples/client.toml`.

### Logging

Both binaries emit hierarchical spans via `tracing-tree`. Filter with
`RUST_LOG`:

```sh
RUST_LOG=houdini_server=debug,houdini_protocol=info houdini-server …
```

Defaults are `info` for `houdini_*` targets. The bearer token is wrapped
in `secrecy::SecretString` and is never written to logs.

### Shutdown

Both binaries install a `ctrlc` handler that fires on Ctrl+C and SIGTERM.
The server stops accepting new connections and drains in-flight requests
through `axum`'s graceful shutdown; the client breaks out of its
reconnect loop.

## HAProxy snippet

A minimal HAProxy frontend/backend pair that does the right thing:

```haproxy
frontend public_https
    bind :443 ssl crt /etc/haproxy/certs/tunnel.example.com.pem alpn http/1.1
    default_backend houdini

backend houdini
    server houdini 127.0.0.1:8080 check
    # No special websocket directive needed in HTTP mode — HAProxy passes
    # Upgrade through automatically as long as `option http-server-close`
    # is *not* set. `option http-keep-alive` is fine.
    timeout tunnel 1h
```

The long `timeout tunnel` is important — without it HAProxy will sever
the WebSocket after the default idle timeout.

## Status

This is v0.1. Single client, HTTP/1.1 only, shared-token auth, no metrics
endpoint, no rate limiting. The protocol is stable enough to use against
itself (server and client at the same version) but not yet stable for
external implementations.
