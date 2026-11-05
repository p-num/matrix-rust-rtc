// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use livekit_protocol as proto;
use prost::Message as ProtoMessage;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

use tokio::sync::{mpsc, oneshot, watch};

use super::{SignalError, SignalResult};

/// Upper bound for the writer to flush its Close frame during `close`; a peer
/// that never answers must not hold the stream lock hostage.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct InternalMessage {
    signal: proto::signal_request::Message,
    response: oneshot::Sender<SignalResult<()>>,
}

#[derive(Clone)]
enum Shutdown {
    Transport,
    Disconnect(proto::LeaveRequest),
}

/// SignalStream holds the WebSocket connection (via `WsConnection`).
///
/// It is replaced by [SignalClient] at each reconnection.
pub(super) struct SignalStream {
    internal_tx: mpsc::Sender<InternalMessage>,
    shutdown: watch::Sender<Option<Shutdown>>,
    read_handle: JoinHandle<()>,
    write_handle: JoinHandle<()>,
}

/// Terse: this appears inside `SignalState` in logs and tests.
impl std::fmt::Debug for SignalStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SignalStream")
    }
}

impl SignalStream {
    /// Connect to livekit websocket.
    /// Returns SignalError if the connection failed.
    ///
    /// SignalStream will never try to reconnect if the connection has been closed.
    pub async fn connect(
        url: url::Url,
        token: &str,
        connect_timeout: Duration,
    ) -> SignalResult<(
        Self,
        mpsc::UnboundedReceiver<Box<proto::signal_response::Message>>,
    )> {
        log::info!("connecting to {}", livekit_net::redact_url(&url));

        // Reject a malformed token before touching the transport, so it surfaces
        // as a non-retryable TokenFormat error rather than a generic connection
        // failure that the caller would pointlessly retry.
        super::check_token_format(token)?;

        let transport = super::require_ws_client()?;

        let headers = super::bearer_headers(token);

        // Delegate the connect deadline to the transport via `timeout_ms`, but keep
        // an outer Rust-side timeout as a backstop: a foreign/host transport that
        // ignores or mishandles `timeout_ms` must not be able to hang connect (and
        // thus the engine's reconnect loop) forever.
        let conn = tokio::time::timeout(
            connect_timeout,
            transport.connect(url.to_string(), headers, connect_timeout.as_millis() as u64),
        )
        .await
        .map_err(|_| SignalError::Timeout("signal connection timed out".into()))??
        .connection;

        Ok(Self::spawn(conn))
    }

    /// Run the read and write tasks over an open connection.
    pub(super) fn spawn(
        conn: Arc<dyn livekit_net::WsConnection>,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<Box<proto::signal_response::Message>>,
    ) {
        let (emitter, events) = mpsc::unbounded_channel();
        let (internal_tx, internal_rx) = mpsc::channel::<InternalMessage>(8);
        let (shutdown, _) = watch::channel(None);
        let write_handle = tokio::spawn(Self::write_task(
            internal_rx,
            conn.clone(),
            shutdown.clone(),
        ));
        let read_handle = tokio::spawn(Self::read_task(conn, emitter, shutdown.clone()));

        (
            Self {
                internal_tx,
                shutdown,
                read_handle,
                write_handle,
            },
            events,
        )
    }

    /// Stop both tasks without waiting behind an ordinary signaling write.
    pub async fn close(self, leave: Option<proto::LeaveRequest>) {
        let Self {
            internal_tx,
            shutdown,
            read_handle,
            mut write_handle,
        } = self;
        request_shutdown(
            &shutdown,
            leave.map_or(Shutdown::Transport, Shutdown::Disconnect),
        );
        drop(internal_tx);
        let _ = read_handle.await;
        if tokio::time::timeout(CLOSE_TIMEOUT, &mut write_handle)
            .await
            .is_err()
        {
            write_handle.abort();
            let _ = write_handle.await;
        }
    }

    /// Send a SignalRequest and wait for the writer's acknowledgement.
    pub async fn send(&self, signal: proto::signal_request::Message) -> SignalResult<()> {
        let (response, ack) = oneshot::channel();
        self.internal_tx
            .send(InternalMessage { signal, response })
            .await
            .map_err(|_| SignalError::SendError)?;
        ack.await.map_err(|_| SignalError::SendError)?
    }

    async fn write_task(
        mut internal_rx: mpsc::Receiver<InternalMessage>,
        conn: Arc<dyn livekit_net::WsConnection>,
        shutdown: watch::Sender<Option<Shutdown>>,
    ) {
        let mut request = shutdown.subscribe();
        tokio::select! {
            biased;
            _ = request.wait_for(Option::is_some) => {}
            _ = async {
                while let Some(InternalMessage { signal, response }) = internal_rx.recv().await {
                    let data = proto::SignalRequest { message: Some(signal) }.encode_to_vec();
                    let result = conn.send(data).await.map_err(SignalError::from);
                    let failed = result.is_err();
                    let _ = response.send(result);
                    if failed {
                        break;
                    }
                }
            } => request_shutdown(&shutdown, Shutdown::Transport),
        }
        // Release queued senders before best-effort protocol shutdown.
        drop(internal_rx);
        let request = shutdown.borrow().clone();
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, async {
            if let Some(Shutdown::Disconnect(leave)) = request {
                let data = proto::SignalRequest {
                    message: Some(proto::signal_request::Message::Leave(leave)),
                }
                .encode_to_vec();
                let _ = conn.send(data).await;
            }
            conn.close().await;
        })
        .await;
    }

    async fn read_task(
        conn: Arc<dyn livekit_net::WsConnection>,
        emitter: mpsc::UnboundedSender<Box<proto::signal_response::Message>>,
        shutdown: watch::Sender<Option<Shutdown>>,
    ) {
        let mut request = shutdown.subscribe();
        tokio::select! {
            biased;
            _ = request.wait_for(Option::is_some) => {}
            _ = emitter.closed() => {}
            _ = async {
                loop {
                    match conn.recv().await {
                        Ok(Some(bytes)) => match proto::SignalResponse::decode(bytes.as_slice()) {
                            Ok(res) => {
                                if let Some(message) = res.message {
                                    if emitter.send(Box::new(message)).is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(err) => log::error!("failed to decode SignalResponse: {err:?}"),
                        },
                        Ok(None) => break,
                        Err(err) => {
                            log::error!("websocket recv error: {err:?}");
                            break;
                        }
                    }
                }
            } => {}
        }
        request_shutdown(&shutdown, Shutdown::Transport);
    }
}

fn request_shutdown(shutdown: &watch::Sender<Option<Shutdown>>, request: Shutdown) {
    shutdown.send_if_modified(|current| {
        if current.is_none() {
            *current = Some(request);
            true
        } else {
            false
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit_net::{TransportError, WsConnection};

    /// A half-open socket: writes succeed, the peer never sends another byte
    /// and never closes.
    struct HalfOpenConn;

    #[async_trait::async_trait]
    impl WsConnection for HalfOpenConn {
        async fn send(&self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }
        async fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
            std::future::pending().await
        }
        async fn close(&self) {}
    }

    /// A peer that never finishes closing: the writer stalls in `close()`.
    struct StalledCloseConn;

    #[async_trait::async_trait]
    impl WsConnection for StalledCloseConn {
        async fn send(&self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }
        async fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
            std::future::pending().await
        }
        async fn close(&self) {
            std::future::pending::<()>().await
        }
    }

    #[derive(Default)]
    struct HeldWriter {
        offer_started: tokio::sync::Notify,
        leaves: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl WsConnection for HeldWriter {
        async fn send(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
            use proto::signal_request::Message;
            match proto::SignalRequest::decode(bytes.as_slice())
                .unwrap()
                .message
            {
                Some(Message::Offer(_)) => {
                    self.offer_started.notify_one();
                    std::future::pending().await
                }
                Some(Message::Leave(_)) => {
                    self.leaves
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
                message => panic!("unexpected signal: {message:?}"),
            }
        }

        async fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
            std::future::pending().await
        }

        async fn close(&self) {}
    }

    #[tokio::test]
    async fn terminal_leave_overtakes_a_stalled_write_and_joins_the_socket_tasks() {
        use proto::signal_request::Message;
        let socket = Arc::new(HeldWriter::default());
        let (stream, _events) = SignalStream::spawn(socket.clone());
        let (tx, ack) = oneshot::channel();
        stream
            .internal_tx
            .send(InternalMessage {
                signal: Message::Offer(proto::SessionDescription::default()),
                response: tx,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), socket.offer_started.notified())
            .await
            .expect("the ordinary writer did not start");

        tokio::time::timeout(
            Duration::from_secs(2),
            stream.close(Some(proto::LeaveRequest::default())),
        )
        .await
        .expect("terminal close was blocked by the Offer");
        assert!(ack.await.is_err(), "the abandoned Offer was acknowledged");
        assert_eq!(socket.leaves.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&socket), 1);
    }

    #[tokio::test]
    async fn abandoning_the_response_receiver_releases_the_stream_connection() {
        let socket = Arc::new(HalfOpenConn);
        let (stream, response) = SignalStream::spawn(socket.clone());
        drop(response);
        // Keep the stream handle itself live. Receiver loss must wake both I/O tasks.
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&socket) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abandoning the response receiver left the socket owned");
        stream.close(None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_releases_the_connection_before_returning() {
        let conn = Arc::new(StalledCloseConn);
        let (stream, _events) = SignalStream::spawn(conn.clone());
        tokio::time::timeout(CLOSE_TIMEOUT + Duration::from_secs(2), stream.close(None))
            .await
            .expect("close hung on a writer stalled in close()");
        // Both tasks are gone, so nothing still holds the old socket when the
        // caller dials a replacement.
        assert_eq!(
            Arc::strong_count(&conn),
            1,
            "a stream task still holds the connection"
        );
    }

    #[tokio::test]
    async fn close_returns_on_a_half_open_socket() {
        let (stream, _events) = SignalStream::spawn(Arc::new(HalfOpenConn));
        tokio::time::timeout(Duration::from_secs(2), stream.close(None))
            .await
            .expect("close hung on a half-open socket");
    }
}
