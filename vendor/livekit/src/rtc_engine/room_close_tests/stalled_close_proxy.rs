// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Holds only the first old signal WebSocket's remote Close response.
//! The actual client Close and EOF are independently observed before cleanup.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use livekit_protocol as proto;
use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch, Mutex},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

pub(super) struct StalledCloseProxy {
    pub(super) url: String,
    held: Mutex<oneshot::Receiver<()>>,
    closed: Mutex<oneshot::Receiver<()>>,
    release: watch::Sender<bool>,
    hold_open: watch::Sender<bool>,
    trace: Arc<Trace>,
    checkpoint: mpsc::Sender<oneshot::Sender<bool>>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

pub(super) struct Trace {
    origin: Instant,
    events: parking_lot::Mutex<Vec<serde_json::Value>>,
    next_connection: AtomicUsize,
    resumes: AtomicUsize,
    leaves: AtomicUsize,
}

impl Trace {
    fn event(&self, connection: usize, event: &str, resume: Option<bool>) {
        self.events.lock().push(serde_json::json!({
            "at_us": self.origin.elapsed().as_micros(), "connection": connection,
            "event": event, "resume": resume,
            "resume_headers": self.resumes.load(Ordering::Acquire),
            "terminal_leaves": self.leaves.load(Ordering::Acquire),
        }));
    }

    pub(super) fn boundary(&self, event: &str) {
        self.event(0, event, None);
    }

    pub(super) fn print(&self, stage: &str) {
        let events = self.events.lock().clone();
        let admitted: Vec<_> = events
            .iter()
            .filter(|event| event["event"] == "sfu_101_admitted" && event["resume"] == true)
            .filter_map(|event| event["connection"].as_u64())
            .collect();
        let missing_eof: Vec<_> = admitted
            .iter()
            .filter(|connection| {
                !events.iter().any(|event| {
                    event["connection"].as_u64() == Some(**connection)
                        && (event["event"] == "client_eof"
                            || event["event"] == "client_reset"
                            || event["event"] == "client_eof_during_frame")
                })
            })
            .copied()
            .collect();
        eprintln!(
            "STALLED_CLOSE_TRACE {}",
            serde_json::json!({
                "stage": stage, "clock": "microseconds_since_proxy_start",
                "scope": "Proxy accept/header/admission are server observations, not client attempt-start timestamps",
            "admitted_resume_connections": admitted,
            "admitted_resumes_missing_positive_client_eof_or_reset": missing_eof,
            "accepted_candidates_missing_positive_client_eof_or_reset": self.missing_candidate_closures(),
            "events": events,
            })
        );
    }

    fn missing_candidate_closures(&self) -> Vec<u64> {
        let events = self.events.lock();
        events
            .iter()
            .filter(|event| event["event"] == "tcp_accepted")
            .filter_map(|event| event["connection"].as_u64())
            .filter(|connection| *connection != 1)
            .filter(|connection| {
                !events.iter().any(|event| {
                    event["connection"].as_u64() == Some(*connection)
                        && matches!(
                            event["event"].as_str(),
                            Some(
                                "client_eof"
                                    | "client_reset"
                                    | "client_eof_during_frame"
                                    | "client_eof_before_upgrade"
                                    | "client_reset_before_upgrade"
                            )
                        )
                })
            })
            .collect()
    }
}

#[derive(Clone)]
struct ConnectionTrace {
    id: usize,
    trace: Arc<Trace>,
}

impl ConnectionTrace {
    fn event(&self, event: &str, resume: Option<bool>) {
        self.trace.event(self.id, event, resume);
    }
}

impl StalledCloseProxy {
    pub(super) async fn start(backend: &str) -> Result<Self> {
        let authority = backend
            .strip_prefix("http://")
            .or_else(|| backend.strip_prefix("ws://"))
            .context("plain local SFU required")?
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (held, held_rx) = oneshot::channel();
        let (closed, closed_rx) = oneshot::channel();
        let selected = Arc::new(parking_lot::Mutex::new(Some((held, closed))));
        let (release, _) = watch::channel(false);
        let (hold_open, _) = watch::channel(false);
        let released = release.clone();
        let open_hold = hold_open.clone();
        let trace = Arc::new(Trace {
            origin: Instant::now(),
            events: Default::default(),
            next_connection: AtomicUsize::new(1),
            resumes: AtomicUsize::new(0),
            leaves: AtomicUsize::new(0),
        });
        let observations = trace.clone();
        let (checkpoint, mut checkpoints) = mpsc::channel::<oneshot::Sender<bool>>(1);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => match accepted {
                        Ok((client, _)) => {
                            spawn_forward(client, &authority, &selected, &released,
                                &open_hold, &observations, &mut connections);
                        }
                        Err(_) => break,
                    },
                    Some(checkpoint) = checkpoints.recv() => {
                        // After client-owned constructors drain, explicitly
                        // accept the listener's currently ready backlog. An
                        // empty trace set alone could hide an unpolled accept.
                        let healthy = loop {
                            let accepted = std::future::poll_fn(|context| {
                                std::task::Poll::Ready(listener.poll_accept(context))
                            }).await;
                            match accepted {
                                std::task::Poll::Ready(Ok((client, _))) => {
                                    spawn_forward(client, &authority, &selected, &released,
                                        &open_hold, &observations, &mut connections);
                                }
                                std::task::Poll::Ready(Err(_)) => break false,
                                std::task::Poll::Pending => break true,
                            }
                        };
                        observations.boundary("listener_backlog_checkpoint");
                        let _ = checkpoint.send(healthy);
                    },
                    _ = connections.join_next(), if !connections.is_empty() => {},
                }
            }
            connections.shutdown().await;
        });
        Ok(Self {
            url,
            held: Mutex::new(held_rx),
            closed: Mutex::new(closed_rx),
            release,
            hold_open,
            trace,
            checkpoint,
            stop: Some(stop),
            task,
        })
    }

    pub(super) async fn held(&self) -> Result<()> {
        timeout(Duration::from_secs(10), &mut *self.held.lock().await)
            .await
            .context("old client WebSocket Close was never observed")?
            .context("old close barrier disappeared")
    }

    pub(super) async fn client_closed(&self) -> Result<()> {
        timeout(Duration::from_secs(2), &mut *self.closed.lock().await)
            .await
            .context("old client socket did not terminate while remote Close remained held")?
            .context("old socket closure witness disappeared")
    }

    pub(super) fn release(&self) {
        let released = *self.release.borrow();
        if !released {
            self.trace.boundary("first_fixture_release");
            self.release.send_replace(true);
        }
    }

    pub(super) fn hold_open_responses(&self) {
        self.hold_open.send_replace(true);
    }
    pub(super) fn resume_attempts(&self) -> usize {
        self.trace.resumes.load(Ordering::Acquire)
    }

    pub(super) fn terminal_leaves(&self) -> usize {
        self.trace.leaves.load(Ordering::Acquire)
    }

    pub(super) fn trace(&self) -> Arc<Trace> {
        self.trace.clone()
    }

    pub(super) async fn candidate_connections_closed(&self) -> Result<()> {
        timeout(Duration::from_secs(2), async {
            let (checkpoint, completed) = oneshot::channel();
            self.checkpoint
                .send(checkpoint)
                .await
                .context("listener checkpoint disappeared")?;
            ensure!(completed.await?, "listener checkpoint failed");
            loop {
                if self.trace.missing_candidate_closures().is_empty() {
                    self.trace
                        .boundary("all_accepted_candidates_positive_client_eof");
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("an accepted reconnect candidate lacked positive client EOF/reset")?
    }

    pub(super) async fn shutdown(&mut self) -> bool {
        self.release();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
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

impl Drop for StalledCloseProxy {
    fn drop(&mut self) {
        self.release();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.abort();
    }
}

type Selected = Arc<parking_lot::Mutex<Option<(oneshot::Sender<()>, oneshot::Sender<()>)>>>;

fn spawn_forward(
    client: TcpStream,
    authority: &str,
    selected: &Selected,
    released: &watch::Sender<bool>,
    hold_open: &watch::Sender<bool>,
    observations: &Arc<Trace>,
    connections: &mut JoinSet<()>,
) {
    let authority = authority.to_owned();
    let selected = selected.clone();
    let release = released.subscribe();
    let hold_open = hold_open.subscribe();
    let trace = ConnectionTrace {
        id: observations.next_connection.fetch_add(1, Ordering::AcqRel),
        trace: observations.clone(),
    };
    trace.event("tcp_accepted", None);
    connections.spawn(async move {
        let observed = trace.clone();
        if forward(client, &authority, selected, release, hold_open, trace)
            .await
            .is_err()
        {
            observed.event("relay_error", None);
        }
    });
}

async fn client_header(stream: &mut TcpStream, trace: &ConnectionTrace) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        ensure!(bytes.len() < 65_536, "private relay header too large");
        let mut byte = [0];
        match stream.read(&mut byte).await {
            Ok(0) => {
                trace.event("client_eof_before_upgrade", None);
                return Ok(None);
            }
            Ok(_) => bytes.push(byte[0]),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                trace.event("client_reset_before_upgrade", None);
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(bytes))
}

async fn header(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        ensure!(bytes.len() < 65_536, "private relay header too large");
        bytes.push(stream.read_u8().await?);
    }
    Ok(bytes)
}

async fn forward(
    mut client: TcpStream,
    authority: &str,
    selected: Selected,
    mut released: watch::Receiver<bool>,
    hold_open: watch::Receiver<bool>,
    trace: ConnectionTrace,
) -> Result<()> {
    let Some(request) = client_header(&mut client, &trace).await? else {
        return Ok(());
    };
    let first_line = request
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let resume = first_line
        .windows(b"reconnect=1".len())
        .any(|part| part == b"reconnect=1");
    if resume {
        trace.trace.resumes.fetch_add(1, Ordering::AcqRel);
    }
    trace.event("http_header_parsed", Some(resume));
    let old = if resume { None } else { selected.lock().take() };
    let mut backend = TcpStream::connect(authority).await?;
    backend.write_all(&request).await?;
    let upgrade = header(&mut backend).await?;
    ensure!(
        upgrade.starts_with(b"HTTP/1.1 101"),
        "signal relay did not upgrade"
    );
    trace.event("sfu_101_admitted", Some(resume));
    let upgrade_written = client.write_all(&upgrade).await.is_ok();
    if !upgrade_written {
        trace.event("client_upgrade_write_error", Some(resume));
    }
    let Some((held, closed)) = old else {
        return forward_resume(client, backend, trace).await;
    };
    ensure!(upgrade_written, "initial client upgrade write failed");
    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = backend.split();
    let (closing, mut close_seen) = watch::channel(false);
    let held = Arc::new(parking_lot::Mutex::new(Some(held)));
    let client_hold = held.clone();
    let from_client = async {
        let mut closed = Some(closed);
        loop {
            match frame(&mut client_read, true).await? {
                Some((opcode, bytes)) => {
                    if opcode == 2 && is_terminal_leave(&bytes)? {
                        trace.trace.leaves.fetch_add(1, Ordering::AcqRel);
                        trace.event("client_terminal_leave", Some(false));
                    }
                    if opcode == 8 {
                        trace.event("client_close_frame", Some(false));
                        closing.send_replace(true);
                        if let Some(held) = client_hold.lock().take() {
                            let _ = held.send(());
                        }
                    }
                    backend_write.write_all(&bytes).await?;
                }
                None => {
                    trace.event("client_eof", Some(false));
                    if *closing.borrow() {
                        if let Some(closed) = closed.take() {
                            let _ = closed.send(());
                        }
                    }
                    backend_write.shutdown().await?;
                    return Result::<()>::Ok(());
                }
            }
        }
    };
    let from_backend = async {
        loop {
            let next = frame(&mut backend_read, false).await?;
            if (*close_seen.borrow() || *hold_open.borrow()) && !*released.borrow() {
                if let Some(held) = held.lock().take() {
                    let _ = held.send(());
                }
                released.wait_for(|done| *done).await?;
            }
            let Some((_, bytes)) = next else {
                return Result::<()>::Ok(());
            };
            // The held state is checked after receiving each frame as well, so
            // the actual server Close cannot race past the scheduling barrier.
            client_write.write_all(&bytes).await?;
            if *close_seen.borrow_and_update() && *released.borrow() {
                client_write.flush().await?;
            }
        }
    };
    let (first, second) = tokio::join!(from_client, from_backend);
    first?;
    second?;
    Ok(())
}

async fn forward_resume(
    mut client: TcpStream,
    mut backend: TcpStream,
    trace: ConnectionTrace,
) -> Result<()> {
    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = backend.split();
    let from_client = async {
        let mut backend_writable = true;
        loop {
            match frame(&mut client_read, true).await {
                Ok(Some((opcode, bytes))) => {
                    if opcode == 8 {
                        trace.event("client_close_frame", Some(true));
                    }
                    if opcode == 2 && is_terminal_leave(&bytes)? {
                        trace.event("client_terminal_leave", Some(true));
                    }
                    if backend_writable && backend_write.write_all(&bytes).await.is_err() {
                        trace.event("backend_write_error", Some(true));
                        backend_writable = false;
                    }
                }
                Ok(None) => {
                    trace.event("client_eof", Some(true));
                    let _ = backend_write.shutdown().await;
                    break;
                }
                Err(error) => {
                    let kind = error
                        .downcast_ref::<std::io::Error>()
                        .map(std::io::Error::kind);
                    if matches!(
                        kind,
                        Some(
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                        )
                    ) {
                        trace.event(
                            if kind == Some(std::io::ErrorKind::ConnectionReset) {
                                "client_reset"
                            } else {
                                "client_eof_during_frame"
                            },
                            Some(true),
                        );
                        let _ = backend_write.shutdown().await;
                        break;
                    }
                    trace.event("client_read_error_without_eof", Some(true));
                    return Err(error);
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    let from_backend = async {
        loop {
            match frame(&mut backend_read, false).await {
                Ok(Some((_, bytes))) => {
                    if client_write.write_all(&bytes).await.is_err() {
                        trace.event("client_write_error_without_eof", Some(true));
                        break;
                    }
                }
                Ok(None) => {
                    trace.event("backend_eof", Some(true));
                    let _ = client_write.shutdown().await;
                    break;
                }
                Err(_) => {
                    trace.event("backend_read_error", Some(true));
                    break;
                }
            }
        }
    };
    let (client_result, _) = tokio::join!(from_client, from_backend);
    client_result
}

fn is_terminal_leave(bytes: &[u8]) -> Result<bool> {
    let header = match bytes[1] & 0x7f {
        126 => 4,
        127 => 10,
        _ => 2,
    };
    let mask = &bytes[header..header + 4];
    let payload: Vec<u8> = bytes[header + 4..]
        .iter()
        .enumerate()
        .map(|(index, byte)| byte ^ mask[index % 4])
        .collect();
    let request = proto::SignalRequest::decode(payload.as_slice())
        .context("private relay could not decode actual client signal")?;
    Ok(
        matches!(request.message, Some(proto::signal_request::Message::Leave(leave))
        if leave.action() == proto::leave_request::Action::Disconnect),
    )
}

async fn frame(
    reader: &mut (impl AsyncRead + Unpin),
    masked: bool,
) -> Result<Option<(u8, Vec<u8>)>> {
    let Some(first) = ({
        let mut byte = [0];
        match reader.read(&mut byte).await? {
            0 => None,
            _ => Some(byte[0]),
        }
    }) else {
        return Ok(None);
    };
    let second = reader.read_u8().await?;
    ensure!(
        first & 0xf0 == 0x80 && (second & 0x80 != 0) == masked,
        "unexpected private relay WebSocket frame"
    );
    let mut bytes = vec![first, second];
    let length = match second & 0x7f {
        126 => {
            let value = reader.read_u16().await?;
            bytes.extend(value.to_be_bytes());
            u64::from(value)
        }
        127 => {
            let value = reader.read_u64().await?;
            bytes.extend(value.to_be_bytes());
            value
        }
        value => u64::from(value),
    };
    ensure!(length <= 1_048_576, "private relay frame too large");
    let extra = length as usize + if masked { 4 } else { 0 };
    let start = bytes.len();
    bytes.resize(start + extra, 0);
    reader.read_exact(&mut bytes[start..]).await?;
    Ok(Some((first & 0x0f, bytes)))
}
