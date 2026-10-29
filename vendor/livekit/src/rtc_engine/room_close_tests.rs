// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Closes a real, publishing Room while the old signal close is held remotely.
//! Server observations precede fixture cleanup and cannot be manufactured by it.
//! Fresh recovery rejection is checked at the closed engine's actual gate.
//! Proxy accept/header times describe server observations, not client attempt birth.

use std::{sync::Arc, time::Duration};

use anyhow::{ensure, Context, Result};
use libwebrtc::{
    audio_source::native::NativeAudioSource,
    prelude::{AudioFrame, AudioSourceOptions, RtcAudioSource},
};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::room::{CreateRoomOptions, RoomClient},
};
use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};

use crate::{
    options::TrackPublishOptions,
    prelude::{LocalAudioTrack, LocalTrack, TrackSource},
    ConnectionState, Room, RoomOptions, SimulateScenario,
};

mod stalled_close_proxy;
use stalled_close_proxy::StalledCloseProxy;

const LIMIT: Duration = Duration::from_secs(15);
const CLOSE_LIMIT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum Scenario {
    OldClose,
    OpenUnresponsive,
}

#[tokio::test]
async fn publishing_room_closes_before_old_signal_close_is_released() -> Result<()> {
    run(Scenario::OldClose).await
}

#[tokio::test]
async fn publishing_room_delivers_leave_while_old_signal_responses_are_held() -> Result<()> {
    run(Scenario::OpenUnresponsive).await
}

async fn run(scenario: Scenario) -> Result<()> {
    let backend = std::env::var("LIVEKIT_URL").context("LIVEKIT_URL is required")?;
    let port = backend
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| backend.strip_prefix("ws://127.0.0.1:"))
        .or_else(|| backend.strip_prefix("http://localhost:"))
        .or_else(|| backend.strip_prefix("ws://localhost:"))
        .context("an explicitly selected plain local SFU is required")?;
    ensure!(port.parse::<u16>().is_ok(), "local SFU port is invalid");
    let key = std::env::var("LIVEKIT_API_KEY").context("LIVEKIT_API_KEY is required")?;
    let secret = std::env::var("LIVEKIT_API_SECRET").context("LIVEKIT_API_SECRET is required")?;
    let admin = RoomClient::with_api_key(&backend.replacen("ws://", "http://", 1), &key, &secret);
    let room_name = format!("room_close_{}", libwebrtc::native::create_random_uuid());
    let identity = "publishing-room-close";
    let token = AccessToken::with_api_key(&key, &secret)
        .with_identity(identity)
        .with_ttl(Duration::from_secs(120))
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            ..Default::default()
        })
        .to_jwt()
        .map_err(|_| anyhow::anyhow!("private room token creation failed"))?;
    let mut proxy = StalledCloseProxy::start(&backend).await?;
    let created = timeout(
        LIMIT,
        admin.create_room(
            &room_name,
            CreateRoomOptions {
                empty_timeout: 120,
                departure_timeout: 120,
                ..Default::default()
            },
        ),
    )
    .await;
    if !matches!(created, Ok(Ok(_))) {
        let relay = proxy.shutdown().await;
        let room = cleanup_room(&admin, &room_name).await;
        anyhow::bail!(
            "private room creation failed; cleanup confirmed={}",
            relay && room
        );
    }

    let mut room = None;
    let mut trigger = None;
    let mut closer = None;
    let mut feeder = None;
    let mut stop_feeding = None;
    let result: Result<()> = async {
        let (connected, mut events) = timeout(
            LIMIT,
            Room::connect(&proxy.url, &token, RoomOptions::default()),
        )
        .await
        .context("publishing Room connection timed out")??;
        let connected = Arc::new(connected);
        room = Some(connected.clone());
        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1000);
        let track =
            LocalAudioTrack::create_audio_track("close-probe", RtcAudioSource::Native(source.clone()));
        let publication = timeout(
            LIMIT,
            connected.local_participant().publish_track(
                LocalTrack::Audio(track),
                TrackPublishOptions {
                    source: TrackSource::Microphone,
                    ..Default::default()
                },
            ),
        )
        .await
        .context("actual audio publication timed out")??;
        let (stop, mut stopped) = oneshot::channel();
        stop_feeding = Some(stop);
        feeder = Some(tokio::spawn(async move {
            // Nonzero synthetic PCM establishes an actual RTP publication;
            // this is not a microphone-device or hardware-audibility claim.
            let samples: Vec<i16> = (0..480).map(|index|
                if index % 48 < 24 { 2000 } else { -2000 }).collect();
            let frame = AudioFrame {
                data: samples.into(), sample_rate: 48_000,
                num_channels: 1, samples_per_channel: 480,
            };
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    captured = source.capture_frame(&frame) => {
                        ensure!(captured.is_ok(), "private PCM capture failed");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
        let expected_sid = connected.local_participant().sid().to_string();
        let expected_track = publication.sid().to_string();
        let old_sid = timeout(LIMIT, async {
            loop {
                let people = timeout(Duration::from_secs(1), admin.list_participants(&room_name))
                    .await.context("publication observation request timed out")??;
                ensure!(people.len() <= 1 && people.iter().all(|person|
                    person.identity == identity && person.sid == expected_sid),
                    "unexpected participant in the private publishing room");
                if people.first().is_some_and(|person| person.tracks.iter().any(|track|
                    track.sid == expected_track
                        && track.source == livekit_protocol::TrackSource::Microphone as i32
                        && !track.muted)) {
                    return Result::<String>::Ok(expected_sid.clone());
                }
                sleep(Duration::from_millis(25)).await;
            }
        }).await.context("actual unmuted microphone publication was not observed before the fault")??;

        match scenario {
            Scenario::OldClose => {
                let owner = connected.clone();
                trigger = Some(tokio::spawn(async move {
                    owner
                        .simulate_scenario(SimulateScenario::SignalReconnect)
                        .await
                        .map_err(anyhow::Error::from)
                }));
            }
            Scenario::OpenUnresponsive => {
                proxy.hold_open_responses();
                let participant = connected.local_participant();
                trigger = Some(tokio::spawn(async move {
                    participant.set_metadata("held-response-probe".into()).await
                        .map_err(anyhow::Error::from)
                }));
            }
        }
        proxy.held().await?;
        // Either a real old Close or the first response on an open old socket
        // remains held. Remote bytes cannot be needed to stop local media.
        let attempts_before_close = proxy.resume_attempts();
        proxy.trace().boundary("before_public_close_spawn");
        let owner = connected.clone();
        let close_trace = proxy.trace();
        closer = Some(tokio::spawn(async move {
            close_trace.boundary("public_room_close_entered");
            let result = owner.close().await;
            close_trace.boundary("public_room_close_returned");
            result
        }));
        let closed = timeout(CLOSE_LIMIT, closer.as_mut().unwrap())
            .await
            .context("Room.close waited for the held OLD signal close")?;
        closer = None;
        closed.context("Room.close task panicked")??;
        let attempts_after_close = proxy.resume_attempts();
        proxy.trace().boundary("after_close_task_join");
        // A reusable transport close can start recovery before Room.close is
        // polled. Its buffered request may reach the server after local close
        // completes. Header totals cannot classify when that attempt began.
        let mut disconnected_events = 0;
        let mut disconnected_states = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                crate::RoomEvent::Disconnected { .. } => disconnected_events += 1,
                crate::RoomEvent::ConnectionStateChanged(ConnectionState::Disconnected) =>
                    disconnected_states += 1,
                _ => {},
            }
        }
        ensure!(disconnected_events == 1 && disconnected_states == 1,
            "completed Room.close did not emit exactly one terminal event/state transition");
        ensure!(
            connected.connection_state() == ConnectionState::Disconnected,
            "Room did not finish local disconnection while old close remained held"
        );
        ensure!(
            connected
                .local_participant()
                .track_publications()
                .is_empty(),
            "local audio publication remained after Room.close"
        );
        ensure!(publication.track().is_none(), "closed microphone publication retained its track");
        // Subscribe AFTER dispatcher.clear, then invoke the real late recovery
        // handlers. Cleared old subscribers cannot hide a resurrection event.
        connected.probe_closed_recovery_for_test()?;
        proxy.client_closed().await?;
        connected.probe_closed_engine_for_test().await?;
        // This accounts for accepted-but-not-yet-parsed candidates as well as
        // admitted WebSockets. Actual client EOF/reset must be observed while
        // the original remote Close is STILL held, before any fixture release.
        proxy.candidate_connections_closed().await?;
        if matches!(scenario, Scenario::OpenUnresponsive) {
            ensure!(attempts_before_close == 0 && attempts_after_close == 0,
                "an open publishing Room unexpectedly started a resume");
        }

        let absence_limit = match scenario {
            Scenario::OldClose => {
                proxy.release();
                // The frozen RED retains the original strict five-second
                // absence assertion. A connected LiveKit1.13.4 participant
                // whose signal already closed is resumable until the server
                // detects PC loss and its disconnect cleanup timer expires.
                // Never reopen a normal session merely to manufacture Leave.
                Duration::from_secs(45)
            }
            // Here the old socket is still writable. Leave must remove the
            // exact participant BEFORE any response or fixture is released.
            Scenario::OpenUnresponsive => Duration::from_secs(5),
        };
        timeout(absence_limit, async {
            loop {
                let people = timeout(Duration::from_secs(1), admin.list_participants(&room_name))
                    .await
                    .context("post-close SFU observation timed out")??;
                ensure!(
                    people.iter().all(|person| person.sid == old_sid),
                    "terminal close admitted a new participant SID"
                );
                if people.is_empty() {
                    return Result::<()>::Ok(());
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("old participant remained on SFU after terminal Room close")??;
        if matches!(scenario, Scenario::OpenUnresponsive) {
            ensure!(
                proxy.terminal_leaves() > 0,
                "SFU absence did not have an actual protocol Leave witness on the open old stream"
            );
        }
        proxy.release();
        // Absence is sampled, rather than accepting a transient empty snapshot.
        for _ in 0..10 {
            sleep(Duration::from_millis(50)).await;
            ensure!(
                timeout(Duration::from_secs(1), admin.list_participants(&room_name))
                    .await??
                    .is_empty(),
                "a signal owner resurrected the closed participant"
            );
        }
        connected.probe_closed_engine_for_test().await?;
        proxy.candidate_connections_closed().await?;
        proxy.trace().boundary("after_final_owner_and_socket_assertions");
        proxy.trace().print("after_final_owner_and_socket_assertions");
        if matches!(scenario, Scenario::OldClose) {
            eprintln!("old-signal control: observed resume headers before close={attempts_before_close}, at completed close={attempts_after_close}, final={}, old-stream terminal Leaves={}", proxy.resume_attempts(), proxy.terminal_leaves());
        }
        while let Ok(event) = events.try_recv() {
            ensure!(
                !matches!(event, crate::RoomEvent::Reconnected),
                "Room reported recovered after completed Room.close"
            );
        }
        Ok(())
    }
    .await;

    // Release and await every exact task even on RED. Administrative removal
    // belongs only to this cleanup, after all resource oracles above.
    proxy.trace().print("before_fixture_cleanup");
    let mut clean = true;
    if let Some(stop) = stop_feeding {
        let _ = stop.send(());
    }
    if let Some(task) = &mut feeder {
        let finished = timeout(LIMIT, &mut *task).await;
        clean &= matches!(&finished, Ok(Ok(Ok(()))));
        if finished.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
    proxy.release();
    if let Some(task) = &mut closer {
        let finished = timeout(LIMIT, &mut *task).await;
        clean &= matches!(&finished, Ok(Ok(Ok(()))));
        if finished.is_err() {
            task.abort();
            let _ = task.await;
        }
    } else if let Some(room) = room {
        clean &= timeout(LIMIT, room.close()).await.is_ok();
    }
    if let Some(task) = &mut trigger {
        let finished = timeout(LIMIT, &mut *task).await;
        clean &= matches!(&finished, Ok(Ok(_)));
        if finished.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
    clean &= proxy.shutdown().await;
    clean &= cleanup_room(&admin, &room_name).await;
    match result {
        Err(error) => Err(error.context(format!(
            "publishing Room probe failed; cleanup confirmed={clean}"
        ))),
        Ok(()) => {
            ensure!(clean, "publishing Room fixture cleanup failed");
            Ok(())
        }
    }
}

async fn cleanup_room(admin: &RoomClient, room: &str) -> bool {
    matches!(timeout(LIMIT, admin.delete_room(room)).await, Ok(Ok(())))
        || matches!(timeout(LIMIT, admin.list_rooms(vec![room.to_owned()])).await,
            Ok(Ok(rooms)) if rooms.is_empty())
}
