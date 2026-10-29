// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! A private test relay that pauses a real SFU's first resume payload.
//! A WebSocket Ping/Pong proves the real signal tasks exist before cancellation.
//! Application bytes are unchanged; request URLs and tokens are never logged.

use anyhow::{ensure, Context, Result};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Mutex},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

struct Hold {
    reached: oneshot::Sender<Instant>,
    release: oneshot::Receiver<()>,
    closed: oneshot::Sender<bool>,
}

pub(super) struct ResumeProxy {
    pub(super) url: String,
    reached: Mutex<oneshot::Receiver<Instant>>,
    release: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    closed: Mutex<oneshot::Receiver<bool>>,
    task: JoinHandle<()>,
}

impl ResumeProxy {
    pub(super) async fn start(backend_url: &str) -> Result<Self> {
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
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((client, _)) => {
                            let authority = authority.clone();
                            let hold = hold.clone();
                            connections.spawn(async move {
                                let _ = forward(client, &authority, hold).await;
                            });
                        }
                        Err(_) => break,
                    },
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self {
            url,
            reached: Mutex::new(reached_rx),
            release: parking_lot::Mutex::new(Some(release)),
            closed: Mutex::new(closed_rx),
            task,
        })
    }

    pub(super) async fn held(&self) -> Result<Instant> {
        let mut reached = self.reached.lock().await;
        timeout(Duration::from_secs(15), &mut *reached)
            .await
            .context("real resume never reached relay barrier")?
            .context("real resume relay barrier disappeared")
    }

    pub(super) fn release(&self) {
        if let Some(release) = self.release.lock().take() {
            let _ = release.send(());
        }
    }

    pub(super) async fn closed(&self) -> Result<()> {
        let mut closed = self.closed.lock().await;
        let clean = timeout(Duration::from_secs(5), &mut *closed)
            .await
            .context("resumed signal socket remained open")?
            .context("resume socket closure was not observed")?;
        ensure!(
            clean,
            "a relay I/O error cannot prove engine-owned socket cleanup"
        );
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) {
        self.release();
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for ResumeProxy {
    fn drop(&mut self) {
        self.release();
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
    let held = if resume { hold.lock().take() } else { None };
    if let Some(held) = held {
        let upgrade = header(&mut backend).await?;
        ensure!(
            upgrade.starts_with(b"HTTP/1.1 101"),
            "real resume did not upgrade its WebSocket"
        );
        // Forward Upgrade, then hold the real application payload. The control
        // probe below establishes when the client's signal tasks are running.
        client
            .write_all(&upgrade)
            .await
            .context("test relay upgrade forwarding failed")?;
        let upgraded_at = Instant::now();
        let mut payload = [0_u8; 8192];
        let count = backend
            .read(&mut payload)
            .await
            .context("real resume payload read failed")?;
        ensure!(count > 0, "real resume payload was absent");
        // Merely forwarding HTTP101 would leave a scheduling ambiguity: close
        // could cancel before SignalStream had spawned its tasks. A valid
        // control Ping, answered by the real client, proves that boundary.
        timeout(
            Duration::from_secs(1),
            ping_round_trip(&mut client, &mut backend),
        )
        .await
        .context("real signal tasks did not answer the ownership probe")??;
        let _ = held.reached.send(upgraded_at);
        let _ = held.release.await;
        let result = async {
            client.write_all(&payload[..count]).await?;
            tokio::io::copy_bidirectional(&mut client, &mut backend).await
        }
        .await;
        // A closure witness is emitted only after both owned relay sockets
        // have been dropped, and only for the observed resume connection.
        drop(client);
        drop(backend);
        let _ = held.closed.send(result.is_ok());
        result.context("resume relay ended with an I/O failure")?;
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
