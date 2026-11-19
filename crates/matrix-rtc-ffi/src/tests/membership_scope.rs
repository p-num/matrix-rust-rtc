// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Exercises participation endpoint capture through the real FFI and shared
//! call stack, including producers that outlive their original room handle.

use std::collections::HashMap;
use std::sync::Mutex;

use super::*;
use crate::backend::test_support::{Send, SendBarrier, SendTrace};
use matrix_rtc_core::compat::LEGACY_JOIN_ID_FIELD;

#[derive(Default)]
struct Factory {
    endpoints: Mutex<HashMap<String, Arc<MockHost>>>,
    lookups: Mutex<Vec<(String, Option<String>)>>,
}

impl Factory {
    fn register(&self, scope: &str, endpoint: Arc<MockHost>) {
        self.endpoints
            .lock()
            .unwrap()
            .insert(scope.to_owned(), endpoint);
    }

    fn retire(&self, scope: &str) {
        self.endpoints.lock().unwrap().remove(scope);
    }

    fn lookups(&self) -> Vec<(String, Option<String>)> {
        self.lookups.lock().unwrap().clone()
    }
}

impl MembershipBackendFactory for Factory {
    fn for_membership(&self, room_id: String, scope: Option<String>) -> Arc<dyn MatrixBackend> {
        self.lookups.lock().unwrap().push((room_id, scope.clone()));
        if let Some(endpoint) =
            scope.and_then(|scope| self.endpoints.lock().unwrap().get(&scope).cloned())
        {
            return endpoint;
        }
        let denied = MockHost::new();
        *denied.refuse_sticky.lock().unwrap() = Some(refusal());
        *denied.fail_state_once.lock().unwrap() = Some(refusal());
        denied
    }
}

fn refusal() -> FfiBackendError {
    FfiBackendError::Failed {
        errcode: Some("M_FORBIDDEN".to_owned()),
        status: Some(403),
        retry_after_ms: None,
        permanent: true,
        reason: "scope is not registered".to_owned(),
    }
}

fn transient() -> FfiBackendError {
    FfiBackendError::Failed {
        errcode: None,
        status: Some(500),
        retry_after_ms: Some(1),
        permanent: false,
        reason: "retry this publication".to_owned(),
    }
}

fn endpoint(name: &str, trace: &SendTrace) -> Arc<MockHost> {
    let host = MockHost::new();
    *host.endpoint_trace.lock().unwrap() = Some((name.to_owned(), trace.clone()));
    host
}

fn scoped(scope: &str) -> FfiJoinSessionParams {
    FfiJoinSessionParams {
        request_scope_id: Some(scope.to_owned()),
        ..join_params()
    }
}

fn joining(send: &Send) -> bool {
    send.content
        .pointer("/member/membership")
        .and_then(serde_json::Value::as_str)
        == Some("join")
        || send.content.get(LEGACY_JOIN_ID_FIELD).is_some()
}

fn identity(send: &Send) -> Option<String> {
    send.content
        .pointer("/member/id")
        .or_else(|| send.content.get(LEGACY_JOIN_ID_FIELD))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn joins(host: &MockHost) -> Vec<Send> {
    host.sends().into_iter().filter(joining).collect()
}

async fn open_at(
    client: &Arc<RtcClient>,
    root: &MockHost,
    room_id: &str,
    format: FfiMembershipFormat,
) -> Arc<RtcRoom> {
    root.forget_room_sink(room_id);
    let opening = client.clone().room(
        room_id.to_owned(),
        FfiRoomOptions {
            format: Some(format),
        },
    );
    let seed = async {
        while root.subjects(room_id).is_none() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        seed(root, room_id);
    };
    let (room, ()) = tokio::join!(opening, seed);
    room.expect("room opens")
}

fn seed(root: &MockHost, room_id: &str) {
    let subjects = root.subjects(room_id).unwrap();
    let sink = root.room_sink(room_id);
    sink.on_encryption(false);
    sink.on_joined_members(vec![
        "@alice:example.org".to_owned(),
        "@bob:example.org".to_owned(),
    ]);
    for event_type in subjects.state_event_types {
        let events = if event_type == matrix_rtc_core::SLOT_EVENT_TYPE {
            vec![open_slot(None)]
        } else {
            Vec::new()
        };
        sink.on_state_events(event_type, events);
    }
    sink.on_sticky_events(Vec::new());
}

// The FFI's existing join implementation runs on the test's paused executor;
// only the exported runtime handoff is bypassed. The adapter and call/core
// client, outbox, dialects and producers are the real implementations.
fn paused_client(backend: FfiBackend) -> Arc<RtcClient> {
    let origin = tokio::time::Instant::now();
    let outbox = matrix_rtc_core::Outbox::with_clock(Arc::new(move || {
        10_000 + origin.elapsed().as_millis() as u64
    }));
    Arc::new(RtcClient {
        client: call::RtcClient::with_outbox(Arc::new(backend), outbox),
    })
}

async fn open_on_current_runtime(
    client: &Arc<RtcClient>,
    root: &MockHost,
    room_id: &str,
    format: FfiMembershipFormat,
) -> Arc<RtcRoom> {
    let compat = compat::resolve(Some(format));
    let room = client
        .client
        .room(room_id, matrix_rtc_core::RoomOptions { format: compat })
        .await
        .unwrap();
    seed(root, room_id);
    room.seeded().await;
    Arc::new(RtcRoom {
        room_id: room_id.to_owned(),
        room: TokioRwLock::new(Some(room)),
    })
}

async fn wait_for_sends(host: &MockHost, condition: impl Fn(&[Send]) -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let changed = host.sends_changed.notified();
            if condition(&host.sends()) {
                return;
            }
            changed.await;
        }
    })
    .await
    .expect("the acknowledged publication is observed");
}

#[tokio::test]
async fn legacy_constructor_keeps_the_original_backend_with_or_without_a_scope() {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        for scope in [None, Some("opaque-local-request")] {
            let root = MockHost::new();
            let client = RtcClient::new(root.clone());
            let room = open_at(&client, &root, ROOM, format).await;
            let call = room
                .clone()
                .join_call(FfiJoinSessionParams {
                    request_scope_id: scope.map(str::to_owned),
                    ..join_params()
                })
                .await
                .unwrap();
            assert_eq!(joins(&root).len(), 1);
            assert!(
                !root
                    .sends()
                    .iter()
                    .any(|send| send.content.to_string().contains("opaque-local-request"))
            );
            call.leave(no_reason()).await.unwrap();
            room.shutdown().await;
        }
    }
}

#[tokio::test]
async fn a_missing_unknown_or_retired_scope_never_uses_the_root_or_current_endpoint() {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        for scope in [None, Some("unknown"), Some("retired")] {
            let root = MockHost::new();
            let current = MockHost::new();
            let factory = Arc::new(Factory::default());
            factory.register("current", current.clone());
            factory.register("retired", current.clone());
            factory.retire("retired");
            let client = RtcClient::new_with_membership_factory(root.clone(), factory.clone());
            let room = open_at(&client, &root, ROOM, format).await;
            let result = room
                .clone()
                .join_call(FfiJoinSessionParams {
                    request_scope_id: scope.map(str::to_owned),
                    ..join_params()
                })
                .await;
            assert!(
                matches!(result, Err(MatrixRtcFfiError::JoinRefused { .. })),
                "{format:?}/{scope:?}"
            );
            assert!(root.sends().is_empty());
            assert!(current.sends().is_empty());
            assert_eq!(
                factory.lookups(),
                vec![(ROOM.to_owned(), scope.map(str::to_owned))]
            );
            room.shutdown().await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn one_capture_routes_initial_retry_refresh_republish_and_leave() {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        let trace = SendTrace::default();
        let root = endpoint("root", &trace);
        let r1 = endpoint("R1", &trace);
        let r2 = endpoint("R2", &trace);
        match format {
            FfiMembershipFormat::Msc => *r1.fail_sticky_once.lock().unwrap() = Some(transient()),
            FfiMembershipFormat::RoomState => {
                *r1.fail_state_once.lock().unwrap() = Some(transient())
            }
        }
        let factory = Arc::new(Factory::default());
        factory.register("private-scope-R1", r1.clone());
        let client = paused_client(FfiBackend::with_membership_factory(
            root.clone(),
            factory.clone(),
        ));
        let room = open_on_current_runtime(&client, &root, ROOM, format).await;
        let params = FfiJoinSessionParams {
            sticky_duration_ms: Some(3_000),
            ..scoped("private-scope-R1")
        };
        assert!(!params.summary().contains("private-scope-R1"));
        let call = room
            .join_on_runtime(params, Some(Duration::from_millis(10)))
            .await
            .unwrap();
        assert_eq!(joins(&r1).len(), 2, "initial send and real transient retry");
        factory.retire("private-scope-R1");
        factory.register("private-scope-R2", r2.clone());
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        wait_for_sends(&r1, |sends| {
            sends.iter().filter(|send| joining(send)).count() >= 3
        })
        .await;
        let transport = if format == FfiMembershipFormat::RoomState {
            matrix_rtc_core::LiveKitTransport::service("https://moved.example.org")
        } else {
            matrix_rtc_core::LiveKitTransport::sfu("wss://moved.example.org")
        };
        call.call
            .media_status_reporter()
            .moved(matrix_rtc_core::RtcTransport::LiveKit(transport));
        wait_for_sends(&r1, |sends| {
            sends
                .iter()
                .any(|send| joining(send) && send.content.to_string().contains("moved.example.org"))
        })
        .await;
        let original = identity(&joins(&r1)[0]).unwrap();
        assert!(
            joins(&r1)
                .iter()
                .all(|send| identity(send).as_ref() == Some(&original))
        );
        call.leave(no_reason()).await.unwrap();
        drop(call);
        room.shutdown().await;
        wait_for_sends(&r1, |sends| {
            sends.iter().any(|send| {
                !joining(send)
                    && matches!(
                        send.carrier,
                        Carrier::State | Carrier::Sticky | Carrier::Room
                    )
                    && (send.content == serde_json::json!({})
                        || send
                            .content
                            .pointer("/member/membership")
                            .and_then(serde_json::Value::as_str)
                            == Some("leave"))
            })
        })
        .await;
        assert_eq!(
            factory.lookups(),
            vec![(ROOM.to_owned(), Some("private-scope-R1".to_owned()))]
        );
        assert!(root.sends().is_empty());
        assert!(r2.sends().is_empty());
        assert!(
            trace
                .lock()
                .unwrap()
                .iter()
                .all(|event| event.endpoint == "R1"
                    && !event.send.content.to_string().contains("private-scope-"))
        );
    }
}

async fn late_publication_keeps_original_endpoint(republish: bool) {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        for same_room in [false, true] {
            let trace = SendTrace::default();
            let root = endpoint("root", &trace);
            let r1 = endpoint("R1", &trace);
            let r2 = endpoint("R2", &trace);
            let factory = Arc::new(Factory::default());
            factory.register("R1", r1.clone());
            let barrier = Arc::new(SendBarrier::default());
            let client = paused_client(
                FfiBackend::with_membership_factory(root.clone(), factory.clone())
                    .with_send_barrier(barrier.clone()),
            );
            let room = open_on_current_runtime(&client, &root, ROOM, format).await;
            let call = room
                .join_on_runtime(
                    FfiJoinSessionParams {
                        sticky_duration_ms: Some(3_000),
                        ..scoped("R1")
                    },
                    Some(Duration::from_millis(10)),
                )
                .await
                .unwrap();
            let original = identity(&joins(&r1)[0]).unwrap();
            let hold = barrier.arm(ROOM);
            if republish {
                let transport = if format == FfiMembershipFormat::RoomState {
                    matrix_rtc_core::LiveKitTransport::service("https://moved.example.org")
                } else {
                    matrix_rtc_core::LiveKitTransport::sfu("wss://moved.example.org")
                };
                call.call
                    .media_status_reporter()
                    .moved(matrix_rtc_core::RtcTransport::LiveKit(transport));
            }
            tokio::time::timeout(Duration::from_secs(5), hold.held())
                .await
                .expect("old producer reaches pre-host barrier");
            assert_eq!(joins(&r1).len(), 1, "held before host callback entry");
            tokio::time::timeout(Duration::from_secs(2), call.leave(no_reason()))
                .await
                .expect("leave returns while producer held")
                .unwrap();
            assert!(!call.is_live());
            drop(call);
            tokio::time::timeout(Duration::from_secs(2), room.shutdown())
                .await
                .expect("shutdown returns while producer held");
            assert!(!hold.is_released());
            factory.retire("R1");
            factory.register("R2", r2.clone());
            let replacement_room = if same_room {
                ROOM
            } else {
                "!replacement:example.org"
            };
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let mut replacement = Some(tokio::spawn({
                let client = client.clone();
                let root = root.clone();
                async move {
                    started_tx.send(()).unwrap();
                    let room =
                        open_on_current_runtime(&client, &root, replacement_room, format).await;
                    let call = room.join_on_runtime(scoped("R2"), None).await.unwrap();
                    (room, call)
                }
            }));
            started_rx.await.unwrap();
            let completed = if same_room {
                None
            } else {
                Some(
                    tokio::time::timeout(Duration::from_secs(2), replacement.take().unwrap())
                        .await
                        .expect("distinct-room R2 joins before old producer release")
                        .unwrap(),
                )
            };
            hold.release();
            let (new_room, new_call) = if let Some(completed) = completed {
                completed
            } else {
                tokio::time::timeout(Duration::from_secs(5), replacement.take().unwrap())
                    .await
                    .expect("same-room replacement follows original legacy hold")
                    .unwrap()
            };
            wait_for_sends(&r1, |sends| {
                sends.iter().filter(|send| joining(send)).count() == 2
            })
            .await;
            assert!(
                joins(&r1)
                    .iter()
                    .all(|send| identity(send).as_ref() == Some(&original))
            );
            assert!(
                joins(&r2)
                    .iter()
                    .all(|send| identity(send).as_ref() != Some(&original))
            );
            assert!(root.sends().is_empty());
            assert_eq!(
                factory.lookups(),
                vec![
                    (ROOM.to_owned(), Some("R1".to_owned())),
                    (replacement_room.to_owned(), Some("R2".to_owned()))
                ]
            );
            if same_room && format == FfiMembershipFormat::RoomState {
                let tape = trace.lock().unwrap();
                let old_join = tape
                    .iter()
                    .rposition(|event| event.endpoint == "R1" && joining(&event.send))
                    .unwrap();
                let old_leave = tape
                    .iter()
                    .position(|event| {
                        event.endpoint == "R1"
                            && event.send.carrier == Carrier::State
                            && event.send.content == serde_json::json!({})
                    })
                    .unwrap();
                let new_join = tape
                    .iter()
                    .position(|event| event.endpoint == "R2" && joining(&event.send))
                    .unwrap();
                assert!(old_join < old_leave && old_leave < new_join);
                assert_eq!(
                    tape[old_join].send.state_key,
                    tape[old_leave].send.state_key
                );
                assert_eq!(
                    tape[old_leave].send.state_key,
                    tape[new_join].send.state_key
                );
            }
            eprintln!(
                "MEMBERSHIP_SCOPE_TRACE format={format:?} republish={republish} same_room={same_room} endpoints={:?}",
                trace.lock().unwrap()
            );
            new_call.leave(no_reason()).await.unwrap();
            new_room.shutdown().await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_refresh_held_before_host_entry_never_adopts_the_replacement_scope() {
    late_publication_keeps_original_endpoint(false).await;
}

#[tokio::test(start_paused = true)]
async fn a_republish_held_before_host_entry_never_adopts_the_replacement_scope() {
    late_publication_keeps_original_endpoint(true).await;
}

#[tokio::test]
async fn an_initial_publication_is_bound_before_its_first_joining_host_callback() {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        let trace = SendTrace::default();
        let root = endpoint("root", &trace);
        let r1 = endpoint("R1", &trace);
        let r2 = endpoint("R2", &trace);
        let factory = Arc::new(Factory::default());
        factory.register("R1", r1.clone());
        let barrier = Arc::new(SendBarrier::default());
        let client = Arc::new(RtcClient {
            client: call::RtcClient::new(Arc::new(
                FfiBackend::with_membership_factory(root.clone(), factory.clone())
                    .with_send_barrier(barrier.clone()),
            )),
        });
        let room = open_at(&client, &root, ROOM, format).await;
        let hold = barrier.arm(ROOM);
        let joining = tokio::spawn(room.clone().join_call(scoped("R1")));
        tokio::time::timeout(Duration::from_secs(5), hold.held())
            .await
            .unwrap();
        assert!(
            joins(&r1).is_empty(),
            "the first joining payload has not reached its foreign host"
        );
        factory.retire("R1");
        factory.register("R2", r2.clone());
        let new_room = open_at(&client, &root, "!replacement:example.org", format).await;
        let new_call = new_room.clone().join_call(scoped("R2")).await.unwrap();
        hold.release();
        let old_call = joining.await.unwrap().unwrap();
        assert_eq!(joins(&r1).len(), 1);
        assert_eq!(joins(&r2).len(), 1);
        assert_ne!(identity(&joins(&r1)[0]), identity(&joins(&r2)[0]));
        assert!(root.sends().is_empty());
        assert_eq!(factory.lookups().len(), 2);
        old_call.leave(no_reason()).await.unwrap();
        room.shutdown().await;
        new_call.leave(no_reason()).await.unwrap();
        new_room.shutdown().await;
    }
}

#[tokio::test]
async fn cancelling_an_initial_join_does_not_give_its_endpoint_to_a_new_same_room_join() {
    for format in [FfiMembershipFormat::Msc, FfiMembershipFormat::RoomState] {
        let trace = SendTrace::default();
        let root = endpoint("root", &trace);
        let r1 = endpoint("R1", &trace);
        let r2 = endpoint("R2", &trace);
        let factory = Arc::new(Factory::default());
        factory.register("R1", r1.clone());
        let barrier = Arc::new(SendBarrier::default());
        let client = Arc::new(RtcClient {
            client: call::RtcClient::new(Arc::new(
                FfiBackend::with_membership_factory(root.clone(), factory.clone())
                    .with_send_barrier(barrier.clone()),
            )),
        });
        let room = open_at(&client, &root, ROOM, format).await;
        let hold = barrier.arm(ROOM);
        let joining = tokio::spawn(room.clone().join_call(scoped("R1")));
        tokio::time::timeout(Duration::from_secs(5), hold.held())
            .await
            .unwrap();
        joining.abort();
        match joining.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("initial join completed while held"),
        }
        tokio::time::timeout(Duration::from_secs(2), room.shutdown())
            .await
            .unwrap();
        assert!(!hold.is_released());
        factory.retire("R1");
        factory.register("R2", r2.clone());
        let new_room = open_at(&client, &root, ROOM, format).await;
        let new_call = new_room.clone().join_call(scoped("R2")).await.unwrap();
        hold.release();
        assert!(
            joins(&r1).is_empty(),
            "cancelled initial joining publication never entered the foreign host"
        );
        assert_eq!(joins(&r2).len(), 1);
        assert!(root.sends().is_empty());
        assert_eq!(
            factory.lookups(),
            vec![
                (ROOM.to_owned(), Some("R1".to_owned())),
                (ROOM.to_owned(), Some("R2".to_owned()))
            ]
        );
        new_call.leave(no_reason()).await.unwrap();
        new_room.shutdown().await;
    }
}
