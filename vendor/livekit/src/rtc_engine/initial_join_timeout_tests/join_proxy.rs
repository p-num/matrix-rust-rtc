// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! A private relay that pauses a real SFU handshake or withholds ICE negotiation.
//! A WebSocket Ping/Pong proves the real signal tasks exist before a normal constructor timeout.
//! Forwarded frames are unchanged; request URLs and tokens are never logged.

use anyhow::{ensure, Context, Result};
use livekit_protocol as proto;
use prost::Message;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Mutex},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

#[derive(Clone, Copy)]
enum HoldAt {
    Join,
    Resume,
    Ice,
}

struct Hold {
    reached: oneshot::Sender<Instant>,
    release: oneshot::Receiver<()>,
    closed: oneshot::Sender<bool>,
}

pub(super) struct JoinProxy {
    pub(super) url: String,
    reached: Mutex<oneshot::Receiver<Instant>>,
    release: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    closed: Mutex<oneshot::Receiver<bool>>,
    task: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl JoinProxy {
    pub(super) async fn start(backend_url: &str) -> Result<Self> {
        Self::start_for(backend_url, HoldAt::Join).await
    }

    pub(super) async fn start_resume(backend_url: &str) -> Result<Self> {
        Self::start_for(backend_url, HoldAt::Resume).await
    }

    pub(super) async fn start_ice(backend_url: &str) -> Result<Self> {
        Self::start_for(backend_url, HoldAt::Ice).await
    }

    async fn start_for(backend_url: &str, hold_at: HoldAt) -> Result<Self> {
        let authority = backend_url
            .strip_prefix("http://")
            .or_else(|| backend_url.strip_prefix("ws://"))
            .context("test relay requires a plain local SFU")?
            .split('/')
            .next()
            .context("local SFU authority missing")?
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("private relay bind failed")?;
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .context("private relay address failed")?
        );
        let (reached, reached_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let (closed, closed_rx) = oneshot::channel();
        let hold = Arc::new(parking_lot::Mutex::new(Some(Hold {
            reached,
            release: release_rx,
            closed,
        })));
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => match accepted {
                        Ok((client, _)) => {
                            let authority = authority.clone();
                            let hold = hold.clone();
                            connections.spawn(async move {
                                let _ = forward(client, &authority, hold, hold_at).await;
                            });
                        }
                        Err(_) => break,
                    },
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            // Abort and await every exact owned connection before confirming cleanup.
            connections.shutdown().await;
        });
        Ok(Self {
            url,
            reached: Mutex::new(reached_rx),
            release: parking_lot::Mutex::new(Some(release)),
            closed: Mutex::new(closed_rx),
            task,
            shutdown: Some(shutdown),
        })
    }

    pub(super) async fn held(&self) -> Result<Instant> {
        let mut reached = self.reached.lock().await;
        timeout(Duration::from_secs(15), &mut *reached)
            .await
            .context("real signal handshake never reached relay barrier")?
            .context("real signal handshake relay barrier disappeared")
    }

    pub(super) fn release(&self) {
        if let Some(release) = self.release.lock().take() {
            let _ = release.send(());
        }
    }

    pub(super) async fn closed(&self) -> Result<()> {
        let mut closed = self.closed.lock().await;
        let clean = timeout(Duration::from_secs(2), &mut *closed)
            .await
            .context("held signal socket remained open")?
            .context("held signal socket closure was not observed")?;
        ensure!(
            clean,
            "client protocol Close followed by transport termination was not proved"
        );
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> bool {
        self.release();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if matches!(
            timeout(Duration::from_secs(5), &mut self.task).await,
            Ok(Ok(()))
        ) {
            true
        } else {
            self.task.abort();
            let _ = (&mut self.task).await;
            false
        }
    }
}

impl Drop for JoinProxy {
    fn drop(&mut self) {
        self.release();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

async fn header(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        ensure!(bytes.len() < 65_536, "test relay header too large");
        bytes.push(
            stream
                .read_u8()
                .await
                .context("test relay header ended early")?,
        );
    }
    Ok(bytes)
}

async fn forward(
    mut client: TcpStream,
    authority: &str,
    hold: Arc<parking_lot::Mutex<Option<Hold>>>,
    hold_at: HoldAt,
) -> Result<()> {
    let request = header(&mut client).await?;
    let first_line = request
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let resume = first_line
        .windows(b"reconnect=1".len())
        .any(|part| part == b"reconnect=1");
    let mut backend = TcpStream::connect(authority)
        .await
        .context("test relay backend unavailable")?;
    backend
        .write_all(&request)
        .await
        .context("test relay request forwarding failed")?;
    let initial = first_line
        .windows(b"/rtc?".len())
        .any(|part| part == b"/rtc?")
        && !resume;
    let selected = if matches!(hold_at, HoldAt::Resume) {
        resume
    } else {
        initial
    };
    let held = if selected { hold.lock().take() } else { None };
    if let Some(held) = held {
        let upgrade = header(&mut backend).await?;
        ensure!(
            upgrade.starts_with(b"HTTP/1.1 101"),
            "real signal handshake did not upgrade its WebSocket"
        );
        // Forward Upgrade, then hold the real application payload. The control
        // probe below establishes when the client's signal tasks are running.
        client
            .write_all(&upgrade)
            .await
            .context("test relay upgrade forwarding failed")?;
        if matches!(hold_at, HoldAt::Ice) {
            return forward_without_ice(client, backend, held).await;
        }
        let upgraded_at = Instant::now();
        let mut payload = [0_u8; 8192];
        let count = backend
            .read(&mut payload)
            .await
            .context("real signal handshake payload read failed")?;
        ensure!(count > 0, "real signal handshake payload was absent");
        // Merely forwarding HTTP101 would leave a scheduling ambiguity: the test
        // could observe a timeout before SignalStream had spawned its tasks. A valid
        // control Ping, answered by the real client, proves that boundary.
        timeout(
            Duration::from_secs(1),
            ping_round_trip(&mut client, &mut backend),
        )
        .await
        .context("real signal tasks did not answer the ownership probe")??;
        let _ = held.reached.send(upgraded_at);
        let close_seen = Arc::new(AtomicBool::new(false));
        let receipt = Arc::new(parking_lot::Mutex::new(Some(held.closed)));
        // Keep BOTH complete sockets owned until fixture shutdown. Neither a
        // failed payload write nor relay teardown may manufacture our oracle.
        let (mut client_read, mut client_write) = client.split();
        let (mut backend_read, mut backend_write) = backend.split();
        let from_client = async {
            let mut bytes = [0_u8; 8192];
            let mut frames = Vec::new();
            loop {
                match client_read.read(&mut bytes).await {
                    Ok(0) => {
                        let _ = backend_write.shutdown().await;
                        closure_receipt(&receipt, close_seen.load(Ordering::Acquire));
                        break;
                    }
                    Ok(count) => {
                        match observe_close(&mut frames, &bytes[..count], false) {
                            Ok(true) => close_seen.store(true, Ordering::Release),
                            Ok(false) => {}
                            Err(_) => {
                                closure_receipt(&receipt, false);
                                break;
                            }
                        }
                        if backend_write.write_all(&bytes[..count]).await.is_err() {
                            closure_receipt(&receipt, false);
                            break;
                        }
                    }
                    Err(error) => {
                        closure_receipt(
                            &receipt,
                            close_seen.load(Ordering::Acquire)
                                && error.kind() == std::io::ErrorKind::ConnectionReset,
                        );
                        break;
                    }
                }
            }
        };
        let from_backend = async {
            let _ = held.release.await;
            // A protocol Close forbids sending the held application bytes.
            // If release races it, a write failure alone is never a success:
            // only the independent client Close+EOF/reset receipt can pass.
            if !close_seen.load(Ordering::Acquire)
                && client_write.write_all(&payload[..count]).await.is_ok()
            {
                let mut bytes = [0_u8; 8192];
                loop {
                    match backend_read.read(&mut bytes).await {
                        Ok(0) => {
                            if !close_seen.load(Ordering::Acquire) {
                                closure_receipt(&receipt, false);
                            }
                            break;
                        }
                        Ok(count) => {
                            if close_seen.load(Ordering::Acquire)
                                || client_write.write_all(&bytes[..count]).await.is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        };
        tokio::join!(from_client, from_backend);
        // Fixture shutdown aborts and awaits this owned relay task only AFTER
        // the client-originated closure and SFU-absence oracles were captured.
        std::future::pending::<()>().await;
        drop(client);
        drop(backend);
    } else {
        tokio::io::copy_bidirectional(&mut client, &mut backend)
            .await
            .context("test relay forwarding failed")?;
    }
    Ok(())
}

async fn ping_round_trip(client: &mut TcpStream, backend: &mut TcpStream) -> Result<()> {
    const PAYLOAD: &[u8; 4] = b"ownr";
    client
        .write_all(b"\x89\x04ownr")
        .await
        .context("ownership Ping failed")?;
    let mut header = [0_u8; 2];
    client
        .read_exact(&mut header)
        .await
        .context("ownership Pong header missing")?;
    ensure!(
        header[0] == 0x8a && header[1] == 0x84,
        "real client did not produce the expected masked ownership Pong"
    );
    let mut mask = [0_u8; 4];
    let mut payload = [0_u8; 4];
    client
        .read_exact(&mut mask)
        .await
        .context("ownership Pong mask missing")?;
    client
        .read_exact(&mut payload)
        .await
        .context("ownership Pong payload missing")?;
    ensure!(
        payload
            .iter()
            .zip(mask)
            .map(|(byte, mask)| *byte ^ mask)
            .eq(PAYLOAD.iter().copied()),
        "ownership Pong did not match its probe"
    );
    backend
        .write_all(&header)
        .await
        .context("ownership Pong forwarding failed")?;
    backend
        .write_all(&mask)
        .await
        .context("ownership Pong forwarding failed")?;
    backend
        .write_all(&payload)
        .await
        .context("ownership Pong forwarding failed")?;
    Ok(())
}

fn closure_receipt(receipt: &Arc<parking_lot::Mutex<Option<oneshot::Sender<bool>>>>, clean: bool) {
    if let Some(sender) = receipt.lock().take() {
        let _ = sender.send(clean);
    }
}

/// Parse a whole unfragmented test frame without rewriting its wire bytes.
/// Both controls and application frames are bounded before buffering.
fn frame_layout(buffer: &[u8], masked: bool) -> Result<Option<(u8, usize, usize)>> {
    if buffer.len() < 2 {
        return Ok(None);
    }
    let opcode = buffer[0] & 0x0f;
    ensure!(
        buffer[0] & 0xf0 == 0x80
            && (buffer[1] & 0x80 != 0) == masked
            && matches!(opcode, 2 | 8 | 9 | 10),
        "unexpected private relay WebSocket frame"
    );
    let marker = buffer[1] & 0x7f;
    let header = match marker {
        126 => 4,
        127 => 10,
        _ => 2,
    };
    if buffer.len() < header {
        return Ok(None);
    }
    let length = match marker {
        126 => u64::from(u16::from_be_bytes([buffer[2], buffer[3]])),
        127 => u64::from_be_bytes(buffer[2..10].try_into().unwrap()),
        value => u64::from(value),
    };
    ensure!(
        length <= 1_048_576,
        "private relay WebSocket frame too large"
    );
    ensure!(
        opcode < 8 || length <= 125,
        "private relay control frame too large"
    );
    let payload = header + if masked { 4 } else { 0 };
    let total = payload + length as usize;
    Ok((buffer.len() >= total).then_some((opcode, payload, total)))
}

/// Require a complete masked Close frame; an arbitrary I/O error is not proof.
fn observe_close(buffer: &mut Vec<u8>, bytes: &[u8], allow_application: bool) -> Result<bool> {
    buffer.extend_from_slice(bytes);
    let mut close = false;
    while let Some((opcode, payload, total)) = frame_layout(buffer, true)? {
        ensure!(
            allow_application || opcode != 2,
            "unexpected application frame before the selected handshake completed"
        );
        if opcode == 8 {
            ensure!(total - payload != 1, "client Close payload was malformed");
            close = true;
        }
        buffer.drain(..total);
    }
    Ok(close)
}

/// Forward Join and all signal keepalives, but drop only real SFU ICE negotiation.
/// Both complete sockets remain owned until explicit fixture shutdown, even on RED.
async fn forward_without_ice(
    mut client: TcpStream,
    mut backend: TcpStream,
    held: Hold,
) -> Result<()> {
    let close_seen = Arc::new(AtomicBool::new(false));
    let receipt = Arc::new(parking_lot::Mutex::new(Some(held.closed)));
    let mut reached = Some(held.reached);
    // These selected negotiation frames are dropped, not replayed at release.
    // Keep the receiver alive so fixture release cannot tear down either socket.
    let _release = held.release;
    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = backend.split();
    let from_client = async {
        let mut bytes = [0_u8; 8192];
        let mut frames = Vec::new();
        let mut backend_writable = true;
        loop {
            match client_read.read(&mut bytes).await {
                Ok(0) => {
                    let _ = backend_write.shutdown().await;
                    closure_receipt(&receipt, close_seen.load(Ordering::Acquire));
                    break;
                }
                Ok(count) => {
                    match observe_close(&mut frames, &bytes[..count], true) {
                        Ok(true) => close_seen.store(true, Ordering::Release),
                        Ok(false) => {}
                        Err(_) => {
                            closure_receipt(&receipt, false);
                            break;
                        }
                    }
                    if backend_writable && backend_write.write_all(&bytes[..count]).await.is_err() {
                        // A server-side write failure cannot prove disposal.
                        // Keep the exact client socket alive and observe its
                        // independent Close + transport termination instead.
                        backend_writable = false;
                    }
                }
                Err(error) => {
                    closure_receipt(
                        &receipt,
                        close_seen.load(Ordering::Acquire)
                            && error.kind() == std::io::ErrorKind::ConnectionReset,
                    );
                    break;
                }
            }
        }
    };
    let from_backend = async {
        let mut bytes = [0_u8; 8192];
        let mut frames = Vec::new();
        let mut joined = false;
        loop {
            let count = backend_read.read(&mut bytes).await?;
            if count == 0 {
                break;
            }
            frames.extend_from_slice(&bytes[..count]);
            while let Some((opcode, payload, total)) = frame_layout(&frames, false)? {
                let mut negotiation = false;
                if opcode == 2 {
                    let response = proto::SignalResponse::decode(&frames[payload..total])
                        .context("real SFU signal frame was invalid")?;
                    let offer = matches!(
                        &response.message,
                        Some(proto::signal_response::Message::Offer(_))
                    );
                    match response.message {
                        Some(proto::signal_response::Message::Join(response)) => {
                            ensure!(
                                response.subscriber_primary,
                                "probe requires subscriber-primary SFU"
                            );
                            joined = true;
                        }
                        Some(proto::signal_response::Message::Offer(_))
                        | Some(proto::signal_response::Message::Answer(_))
                        | Some(proto::signal_response::Message::Trickle(_)) => {
                            ensure!(joined, "real SFU negotiation arrived before Join");
                            negotiation = true;
                            if offer {
                                if let Some(reached) = reached.take() {
                                    let _ = reached.send(Instant::now());
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if !negotiation && (!close_seen.load(Ordering::Acquire) || opcode == 8) {
                    // The server's Close reply completes a client-initiated
                    // WebSocket handshake; suppressing it would make disposal hang.
                    client_write.write_all(&frames[..total]).await?;
                }
                frames.drain(..total);
            }
        }
        Result::<()>::Ok(())
    };
    let _ = tokio::join!(from_client, from_backend);
    std::future::pending::<()>().await;
    drop(client);
    drop(backend);
    Ok(())
}
