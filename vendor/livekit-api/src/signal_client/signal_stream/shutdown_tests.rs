// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Exercises terminal priority and actual socket-write backpressure locally.
//! Releasing the server is cleanup and cannot satisfy a shutdown assertion.

use std::sync::Arc;

use super::*;

#[test]
fn terminal_disconnect_cannot_be_downgraded_by_transport_close() {
    let (shutdown, _) = watch::channel(None);
    request_shutdown(
        &shutdown,
        Shutdown::Transport {
            notify_close: false,
        },
    );
    let leave = proto::LeaveRequest {
        reason: proto::DisconnectReason::ClientInitiated as i32,
        ..Default::default()
    };
    request_shutdown(&shutdown, Shutdown::Disconnect(leave.clone()));
    request_shutdown(&shutdown, Shutdown::Transport { notify_close: true });
    request_shutdown(
        &shutdown,
        Shutdown::Disconnect(proto::LeaveRequest::default()),
    );
    assert!(
        matches!(shutdown.borrow().as_ref(), Some(Shutdown::Disconnect(recorded))
        if recorded == &leave)
    );
}

#[tokio::test]
async fn terminal_close_joins_both_io_tasks_while_socket_write_is_blocked() {
    blocked_writer(Trigger::TerminalLeave).await;
}

#[tokio::test]
async fn receiver_loss_stops_blocked_io_before_owned_close_joins_it() {
    blocked_writer(Trigger::ReceiverLoss).await;
}

#[tokio::test]
async fn receiver_loss_interrupts_ping_forwarding_into_a_full_writer_queue() {
    blocked_writer(Trigger::ReceiverLossQueuedPing).await;
}

#[tokio::test]
async fn terminal_leave_joins_installed_stream_when_close_is_queued_behind_a_sender() {
    use super::super::SignalClient;
    use std::{future::Future, pin::Pin, task::Poll};
    use tokio::{net::TcpListener, time::timeout};

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        std::future::poll_fn(move |context| Poll::Ready(future.as_mut().poll(context))).await
    }

    const LIMIT: Duration = Duration::from_secs(2);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reached, reached_rx) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.map_err(|_| "accept failed")?;
        let mut socket = tokio_tungstenite::accept_async(socket)
            .await
            .map_err(|_| "WebSocket upgrade failed")?;
        // Use the public client constructor: the real middleware must retain
        // this stream's events receiver throughout the terminal-close race.
        let join = proto::SignalResponse {
            message: Some(proto::signal_response::Message::Join(proto::JoinResponse {
                participant: Some(proto::ParticipantInfo {
                    sid: "PA_queued_terminal_probe".into(),
                    ..Default::default()
                }),
                ping_interval: 60,
                ping_timeout: 60,
                ..Default::default()
            })),
        };
        socket
            .send(Message::Binary(join.encode_to_vec().into()))
            .await
            .map_err(|_| "Join response write failed")?;
        let mut bytes = [0_u8; 8192];
        let mut received = 0;
        while received < 512 * 1024 {
            let count = socket
                .get_mut()
                .read(&mut bytes)
                .await
                .map_err(|_| "frame prefix read failed")?;
            if count == 0 {
                return Err("client closed before the backpressure witness");
            }
            received += count;
        }
        let _ = reached.send(received);
        // No Close, EOF, or further inbound messages can help client teardown
        // until every shutdown assertion below has finished.
        let _ = released.await;
        drop(socket);
        Ok::<(), &'static str>(())
    });
    let connected = SignalClient::connect(
        &format!("ws://{address}"),
        "private-loopback-probe",
        super::super::SignalOptions::default(),
        None,
    )
    .await;
    let (client, _join, _events) = match connected {
        Ok(connected) => connected,
        Err(error) => {
            let _ = release.send(());
            server.abort();
            let _ = server.await;
            panic!("public client fixture failed to connect: {error}");
        }
    };
    let mut offer = Box::pin(client.send(proto::signal_request::Message::Offer(
        proto::SessionDescription {
            r#type: "offer".into(),
            sdp: "x".repeat(16 * 1024 * 1024),
            ..Default::default()
        },
    )));
    let mut close = Box::pin(client.close());
    let mut offer_done = false;
    let mut close_done = false;
    let result: Result<(), &'static str> = async {
        offer_done = poll_once(offer.as_mut()).await.is_ready();
        if offer_done {
            return Err("large Offer completed before the socket-write witness");
        }
        timeout(Duration::from_secs(10), reached_rx)
            .await
            .map_err(|_| "writer never reached actual socket I/O")?
            .map_err(|_| "server byte witness disappeared")?;
        // Only background I/O runs while the ordinary send stays unpolled and
        // keeps its real read guard. Confirm that it is still backpressured.
        tokio::time::sleep(Duration::from_millis(100)).await;
        offer_done = poll_once(offer.as_mut()).await.is_ready();
        if offer_done || client.inner.stream.try_write().is_ok() {
            return Err("large Offer did not retain a real stream read guard");
        }
        close_done = poll_once(close.as_mut()).await.is_ready();
        if close_done || client.inner.stream.try_read().is_ok() {
            return Err("public close did not queue a write lock behind the sender");
        }
        let mut leave = Box::pin(client.send(proto::signal_request::Message::Leave(
            proto::LeaveRequest {
                action: proto::leave_request::Action::Disconnect.into(),
                ..Default::default()
            },
        )));
        if poll_once(leave.as_mut()).await.is_pending() {
            return Err("terminal intent waited behind the queued write lock");
        }
        // Poll close BEFORE ordinary send can release its read guard. A close
        // that cancels its queued writer on terminal intent now waits for the
        // middleware while leaving the installed stream completely untouched.
        close_done = poll_once(close.as_mut()).await.is_ready();
        if close_done {
            return Err("public close returned while the ordinary sender still owned the stream");
        }
        offer_done = poll_once(offer.as_mut()).await.is_ready();
        if !offer_done {
            return Err("ordinary send did not release its read guard on terminal intent");
        }
        timeout(LIMIT, close.as_mut())
            .await
            .map_err(|_| "public close failed to join the installed stream and middleware")?;
        close_done = true;
        if client.inner.stream.read().await.is_some() || client.handle.lock().is_some() {
            return Err("completed public close retained stream or middleware ownership");
        }
        // Successful public close has joined middleware and both socket I/O
        // tasks through SignalStream.close, with the peer still held open.
        Ok(())
    }
    .await;

    // Retain the pinned close even after its assertion times out. Releasing
    // the peer is cleanup only; joining this same future cannot turn RED green.
    let _ = release.send(());
    let mut clean = matches!(timeout(LIMIT, &mut server).await, Ok(Ok(Ok(()))));
    if !server.is_finished() {
        server.abort();
        let _ = server.await;
        clean = false;
    }
    if !offer_done {
        clean &= timeout(LIMIT, offer.as_mut()).await.is_ok();
    }
    if !close_done {
        clean &= timeout(LIMIT, close.as_mut()).await.is_ok();
    }
    clean &= timeout(LIMIT, client.close()).await.is_ok();
    assert!(
        result.is_ok(),
        "queued terminal-close probe: {result:?}; cleanup confirmed={clean}"
    );
    assert!(clean, "queued terminal-close fixture cleanup failed");
}

#[tokio::test]
async fn terminal_leave_cancels_a_real_restart_before_its_upgrade_response() {
    use super::super::{SignalClient, SignalOptions};
    use tokio::{
        net::TcpListener,
        time::{timeout, timeout_at, Instant},
    };

    const LIMIT: Duration = Duration::from_secs(2);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reached, reached_rx) = oneshot::channel();
    let (closed, closed_rx) = oneshot::channel();
    let (check, checked_rx) = oneshot::channel();
    let (checked, check_rx) = oneshot::channel();
    let (release, mut released) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener
            .accept()
            .await
            .map_err(|_| "initial accept failed")?;
        let mut socket = tokio_tungstenite::accept_async(socket)
            .await
            .map_err(|_| "initial upgrade failed")?;
        let join = proto::SignalResponse {
            message: Some(proto::signal_response::Message::Join(proto::JoinResponse {
                participant: Some(proto::ParticipantInfo {
                    sid: "PA_pre_upgrade_probe".into(),
                    ..Default::default()
                }),
                ping_interval: 60,
                ping_timeout: 60,
                ..Default::default()
            })),
        };
        socket
            .send(Message::Binary(join.encode_to_vec().into()))
            .await
            .map_err(|_| "Join response write failed")?;
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(_)) => return Err("initial stream did not close normally"),
            }
        }
        drop(socket);
        let (mut candidate, _) = listener
            .accept()
            .await
            .map_err(|_| "restart accept failed")?;
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() >= 65_536 {
                return Err("restart header was too large");
            }
            header.push(
                candidate
                    .read_u8()
                    .await
                    .map_err(|_| "restart header read failed")?,
            );
        }
        let request_line = header
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default();
        if !request_line
            .windows(b"reconnect=1".len())
            .any(|part| part == b"reconnect=1")
        {
            return Err("second actual request was not a resume upgrade");
        }
        let _ = reached.send(());
        // Deliberately send NO 101 response. Cancellation must dispose the
        // constructor's real TCP socket before a SignalStream can be installed.
        let mut byte = [0];
        let eof = tokio::select! {
            result = candidate.read(&mut byte) => match result {
                Ok(0) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => Ok(()),
                _ => Err("restart candidate did not produce positive EOF/reset"),
            },
            _ = &mut released => Err("fixture released before candidate EOF"),
        };
        let _ = closed.send(eof);
        eof?;
        tokio::select! {
            request = checked_rx => {
                request.map_err(|_| "listener checkpoint disappeared")?;
                let third = std::future::poll_fn(|context| {
                    std::task::Poll::Ready(listener.poll_accept(context))
                }).await;
                let no_third = matches!(third, std::task::Poll::Pending);
                let _ = checked.send(no_third);
                if !no_third {
                    return Err("listener observed a third connection after terminal restart rejection");
                }
                let _ = released.await;
            },
            _ = &mut released => return Err("fixture released before listener checkpoint"),
        }
        drop(candidate);
        Ok::<(), &'static str>(())
    });
    let connected = SignalClient::connect(
        &format!("ws://{address}"),
        "private-loopback-probe",
        SignalOptions::default(),
        None,
    )
    .await;
    let (client, _join, _events) = match connected {
        Ok(connected) => connected,
        Err(error) => {
            let _ = release.send(());
            server.abort();
            let _ = server.await;
            panic!("public pre-upgrade fixture failed to connect: {error}");
        }
    };
    let mut restart = Box::pin(client.restart());
    let mut close = Box::pin(client.close());
    let mut restart_done = false;
    let mut close_done = false;
    let result: Result<(), &'static str> = async {
        tokio::select! {
            result = restart.as_mut() => {
                restart_done = true;
                let _ = result;
                return Err("restart completed before its held upgrade response");
            },
            reached = timeout(Duration::from_secs(10), reached_rx) => {
                reached.map_err(|_| "actual second upgrade request was not observed")?
                    .map_err(|_| "second request witness disappeared")?;
            },
        }
        let deadline = Instant::now() + LIMIT;
        timeout_at(
            deadline,
            client.send(proto::signal_request::Message::Leave(proto::LeaveRequest {
                action: proto::leave_request::Action::Disconnect.into(),
                ..Default::default()
            })),
        )
        .await
        .map_err(|_| "terminal intent waited for the held upgrade")?;
        timeout_at(deadline, async {
            let restart_drained = async {
                let result = restart.as_mut().await;
                restart_done = true;
                if !matches!(result, Err(SignalError::SendError)) {
                    return Err("held constructor did not reject terminal restart");
                }
                Ok(())
            };
            let close_drained = async {
                close.as_mut().await;
                close_done = true;
                Ok(())
            };
            tokio::try_join!(restart_drained, close_drained).map(|_| ())
        })
        .await
        .map_err(|_| "restart/close owners did not drain before upgrade release")??;
        timeout_at(deadline, closed_rx)
            .await
            .map_err(|_| "candidate EOF was not observed")?
            .map_err(|_| "candidate EOF witness disappeared")??;
        if client.inner.stream.read().await.is_some() || client.handle.lock().is_some() {
            return Err("pre-upgrade cancellation retained stream or middleware ownership");
        }
        let fresh = timeout_at(deadline, client.restart())
            .await
            .map_err(|_| "fresh terminal restart failed to reject promptly")?;
        if !matches!(fresh, Err(SignalError::SendError)) {
            return Err("fresh restart was accepted after terminal close");
        }
        check
            .send(())
            .map_err(|_| "listener checkpoint receiver disappeared")?;
        if !timeout_at(deadline, check_rx)
            .await
            .map_err(|_| "listener checkpoint timed out")?
            .map_err(|_| "listener checkpoint result disappeared")?
        {
            return Err("fresh terminal restart produced a third observed connection");
        }
        Ok(())
    }
    .await;

    let _ = release.send(());
    let mut clean = matches!(timeout(LIMIT, &mut server).await, Ok(Ok(Ok(()))));
    if !server.is_finished() {
        server.abort();
        let _ = server.await;
        clean = false;
    }
    if !restart_done {
        clean &= timeout(LIMIT, restart.as_mut()).await.is_ok();
    }
    if !close_done {
        clean &= timeout(LIMIT, close.as_mut()).await.is_ok();
    }
    clean &= timeout(LIMIT, client.close()).await.is_ok();
    assert!(
        result.is_ok(),
        "pre-upgrade terminal probe: {result:?}; cleanup confirmed={clean}"
    );
    assert!(clean, "pre-upgrade terminal fixture cleanup failed");
}

#[derive(Clone, Copy, Debug)]
enum Trigger {
    TerminalLeave,
    ReceiverLoss,
    ReceiverLossQueuedPing,
}

async fn blocked_writer(trigger: Trigger) {
    use tokio::{
        net::TcpListener,
        time::{timeout, timeout_at, Instant},
    };

    const LIMIT: Duration = Duration::from_secs(2);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reached, reached_rx) = oneshot::channel();
    let (release, mut released) = oneshot::channel();
    let (request_pings, requested_pings) = oneshot::channel::<usize>();
    let (pings_sent, sent_pings) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.map_err(|_| "accept failed")?;
        let mut socket = tokio_tungstenite::accept_async(socket)
            .await
            .map_err(|_| "WebSocket upgrade failed")?;
        // Observe real frame bytes before holding the peer. A pending sender
        // alone would not distinguish socket backpressure from slow scheduling.
        let mut bytes = [0_u8; 8192];
        let mut received = 0;
        while received < 512 * 1024 {
            let count = socket
                .get_mut()
                .read(&mut bytes)
                .await
                .map_err(|_| "frame prefix read failed")?;
            if count == 0 {
                return Err("client closed before the backpressure witness");
            }
            received += count;
        }
        let _ = reached.send(received);
        tokio::select! {
            _ = &mut released => {},
            count = requested_pings => {
                if let Ok(count) = count {
                    for _ in 0..count {
                        socket.send(Message::Ping(Bytes::from_static(b"blocked-writer-probe")))
                            .await.map_err(|_| "real server Ping write failed")?;
                    }
                    let _ = pings_sent.send(());
                }
                let _ = released.await;
            }
        }
        drop(socket);
        Ok::<(), &'static str>(())
    });
    let connected = SignalStream::connect(
        url::Url::parse(&format!("ws://{address}/rtc")).unwrap(),
        "private-loopback-probe",
        LIMIT,
    )
    .await;
    let mut stream = connected
        .ok()
        .map(|(stream, events)| (Arc::new(stream), Some(events)));
    let mut sender = None;
    let mut closer = None;
    let result: Result<(), &'static str> = async {
        let (socket, events) = stream.as_mut().ok_or("client upgrade failed")?;
        let owner = socket.clone();
        sender = Some(tokio::spawn(async move {
            owner
                .send(proto::signal_request::Message::Offer(
                    proto::SessionDescription {
                        r#type: "offer".into(),
                        sdp: "x".repeat(16 * 1024 * 1024),
                        ..Default::default()
                    },
                ))
                .await
        }));
        timeout(Duration::from_secs(10), reached_rx)
            .await
            .map_err(|_| "writer never reached actual socket I/O")?
            .map_err(|_| "server byte witness disappeared")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        if sender.as_ref().unwrap().is_finished() {
            return Err("large Offer did not establish socket backpressure");
        }
        let shutdown_deadline = match trigger {
            Trigger::TerminalLeave => {
                let deadline = Instant::now() + LIMIT;
                timeout_at(
                    deadline,
                    socket.send(proto::signal_request::Message::Leave(proto::LeaveRequest {
                        action: proto::leave_request::Action::Disconnect.into(),
                        ..Default::default()
                    })),
                )
                .await
                .map_err(|_| "terminal Leave waited for the unread peer")?
                .map_err(|_| "terminal Leave failed before shutdown")?;
                deadline
            }
            Trigger::ReceiverLoss => {
                // Drop the actual constructor/middleware receiver, without
                // requesting close or terminal Leave on the retained stream.
                let deadline = Instant::now() + LIMIT;
                drop(events.take());
                deadline
            }
            Trigger::ReceiverLossQueuedPing => {
                let count = socket.internal_tx.max_capacity() + 1;
                request_pings
                    .send(count)
                    .map_err(|_| "Ping server disappeared")?;
                timeout(LIMIT, sent_pings)
                    .await
                    .map_err(|_| "server Pings did not reach the socket")?
                    .map_err(|_| "server Ping witness disappeared")?;
                let mut observed = socket.observed_pings.clone();
                timeout(LIMIT, observed.wait_for(|received| *received >= count))
                    .await
                    .map_err(|_| "reader did not reach the final real Ping")?
                    .map_err(|_| "reader Ping witness disappeared")?;
                if socket.internal_tx.capacity() != 0 {
                    return Err("real Pongs did not fill the actual writer queue");
                }
                // The reader has parsed the ninth actual Ping and cannot put
                // its Pong into the full queue. Dropping this real receiver
                // must interrupt that INNER await, without explicit close.
                let deadline = Instant::now() + LIMIT;
                drop(events.take());
                deadline
            }
        };
        let sent = timeout_at(shutdown_deadline, sender.as_mut().unwrap())
            .await
            .map_err(|_| "ordinary writer did not observe terminal shutdown")?;
        sender = None;
        if !matches!(sent, Ok(Err(SignalError::SendError))) {
            return Err("blocked ordinary send was not explicitly cancelled");
        }
        if matches!(
            trigger,
            Trigger::ReceiverLoss | Trigger::ReceiverLossQueuedPing
        ) && !matches!(
            socket.shutdown.borrow().as_ref(),
            Some(Shutdown::Transport { .. })
        ) {
            return Err("receiver loss did not independently request I/O shutdown");
        }
        let (socket, _events) = stream.take().unwrap();
        let socket =
            Arc::try_unwrap(socket).map_err(|_| "ordinary writer retained stream ownership")?;
        closer = Some(tokio::spawn(async move {
            socket.close(true).await;
        }));
        let closed = timeout_at(shutdown_deadline, closer.as_mut().unwrap())
            .await
            .map_err(|_| "Stream.close did not join both I/O tasks under backpressure")?;
        closer = None;
        closed.map_err(|_| "owned close task panicked")?;
        // The server is still held here. No release/EOF from the peer produced
        // any of the completed client send or I/O-join assertions above.
        Ok(())
    }
    .await;

    let _ = release.send(());
    let mut clean = matches!(timeout(LIMIT, &mut server).await, Ok(Ok(Ok(()))));
    if !server.is_finished() {
        server.abort();
        let _ = server.await;
        clean = false;
    }
    if let Some(task) = &mut sender {
        let joined = timeout(LIMIT, &mut *task).await;
        clean &= matches!(joined, Ok(Ok(_)));
        if !task.is_finished() {
            task.abort();
            let _ = task.await;
            clean = false;
        }
    }
    if let Some(task) = &mut closer {
        clean &= matches!(timeout(LIMIT, &mut *task).await, Ok(Ok(())));
        if !task.is_finished() {
            task.abort();
            let _ = task.await;
            clean = false;
        }
    }
    if let Some((socket, _events)) = stream {
        match Arc::try_unwrap(socket) {
            Ok(socket) => clean &= timeout(LIMIT, socket.close(true)).await.is_ok(),
            Err(_) => clean = false,
        }
    }
    assert!(
        result.is_ok(),
        "blocked-writer probe {trigger:?}: {result:?}; cleanup confirmed={clean}"
    );
    assert!(clean, "blocked-writer fixture cleanup failed");
}
