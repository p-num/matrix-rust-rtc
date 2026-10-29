// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Observes a real SignalClient's ordinary Join and Resume response timeouts.
//! No caller cancellation or fixture teardown may manufacture the resource oracle.

use anyhow::{ensure, Context, Result};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::room::{CreateRoomOptions, RoomClient},
    signal_client::{SignalClient, SignalError, SignalOptions, JOIN_RESPONSE_TIMEOUT},
};
use std::time::Duration;
use tokio::time::{sleep, timeout};

pub(super) mod ice_timeout_tests;
mod join_proxy;
mod pc_error_tests;
use join_proxy::JoinProxy;

const LIMIT: Duration = Duration::from_secs(15);
const ABSENCE_LIMIT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn initial_join_response_timeout_closes_its_unowned_signal_stream() -> Result<()> {
    let backend =
        std::env::var("LIVEKIT_URL").map_err(|_| anyhow::anyhow!("LIVEKIT_URL is required"))?;
    // Only a plain loopback authority is accepted; no path, userinfo or query.
    let authority = backend
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| backend.strip_prefix("ws://127.0.0.1:"))
        .or_else(|| backend.strip_prefix("http://localhost:"))
        .or_else(|| backend.strip_prefix("ws://localhost:"))
        .context("an explicitly selected plain local SFU is required")?;
    ensure!(
        authority.parse::<u16>().is_ok(),
        "local SFU port is invalid"
    );
    let key = std::env::var("LIVEKIT_API_KEY")
        .map_err(|_| anyhow::anyhow!("LIVEKIT_API_KEY is required"))?;
    let secret = std::env::var("LIVEKIT_API_SECRET")
        .map_err(|_| anyhow::anyhow!("LIVEKIT_API_SECRET is required"))?;
    let admin = RoomClient::with_api_key(&backend.replacen("ws://", "http://", 1), &key, &secret);
    let nonce = libwebrtc::native::create_random_uuid();
    let room = format!("initial_join_timeout_{nonce}");
    let identity = format!("initial-join-timeout-{nonce}");
    let token = AccessToken::with_api_key(&key, &secret)
        .with_identity(&identity)
        .with_ttl(Duration::from_secs(120))
        .with_grants(VideoGrants {
            room_join: true,
            room: room.clone(),
            ..Default::default()
        })
        .to_jwt()
        .map_err(|_| anyhow::anyhow!("private probe token creation failed"))?;
    let mut proxy = JoinProxy::start(&backend).await?;
    let created = timeout(
        LIMIT,
        admin.create_room(
            &room,
            CreateRoomOptions {
                empty_timeout: 120,
                departure_timeout: 120,
                ..Default::default()
            },
        ),
    )
    .await;
    if !matches!(created, Ok(Ok(_))) {
        let relay_clean = proxy.shutdown().await;
        let cleanup = room_cleanup(&admin, &room).await;
        anyhow::bail!(if relay_clean && cleanup.is_ok() {
            "private probe room creation failed; cleanup confirmed"
        } else {
            "private probe room creation failed; cleanup also failed"
        });
    }
    let url = proxy.url.clone();
    let mut attempt = tokio::spawn(async move {
        SignalClient::connect(&url, &token, SignalOptions::default(), None).await
    });
    let mut attempt_completed = false;
    let mut unexpected_client = None;
    let result = async {
        let held_at = proxy.held().await?;
        // A matching masked Pong has already proved the actual SignalStream
        // tasks exist while all real Join-response application bytes remain held.
        let people = timeout(LIMIT, admin.list_participants(&room))
            .await
            .context("private admission observation timed out")?
            .map_err(|_| anyhow::anyhow!("private admission observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].identity == identity,
            "the exact initial Join probe was not admitted to its unique room"
        );
        // Borrow the task handle and allow the SDK's own five-second timeout.
        // This outer bound is deliberately longer; it must not cause cancellation.
        let completed = timeout(JOIN_RESPONSE_TIMEOUT + LIMIT, &mut attempt)
            .await
            .context("the real SignalClient constructor did not complete")?;
        attempt_completed = true;
        let outcome = completed.context("the real constructor task panicked")?;
        match outcome {
            Err(SignalError::Timeout(message)) => ensure!(
                message.starts_with("failed to receive ") && message.ends_with("JoinResponse"),
                "constructor error was not the real initial Join-response timeout"
            ),
            Err(_) => anyhow::bail!("constructor failed at another stage"),
            Ok((client, _, _)) => {
                unexpected_client = Some(client);
                anyhow::bail!("the held Join constructor unexpectedly succeeded");
            }
        }
        ensure!(
            held_at.elapsed() >= JOIN_RESPONSE_TIMEOUT - Duration::from_millis(100),
            "the constructor returned before its own Join-response timeout"
        );
        // The actual Timeout Err has returned before any payload or fixture
        // cleanup is released. Releasing bytes must not resurrect the lost stream.
        proxy.release();
        let (socket, participant) = tokio::join!(proxy.closed(), async {
            timeout(ABSENCE_LIMIT, async {
                loop {
                    let people = timeout(Duration::from_secs(1), admin.list_participants(&room))
                        .await
                        .map_err(|_| anyhow::anyhow!("probe absence observation timed out"))?
                        .map_err(|_| anyhow::anyhow!("probe absence observation failed"))?;
                    if people.is_empty() {
                        return Result::<()>::Ok(());
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .context("initial Join timeout left its SFU participant behind")?
        });
        socket?;
        participant?;
        Ok(())
    }
    .await;

    // Every oracle above precedes fixture teardown. A failing RED must still
    // release the hold, close any returned client, drop both relay sockets and
    // remove only this unique room. Preserve the primary failure if cleanup fails.
    proxy.release();
    let mut clean = true;
    if let Some(client) = unexpected_client.take() {
        clean &= timeout(LIMIT, client.close()).await.is_ok();
    }
    clean &= proxy.shutdown().await;
    if !attempt_completed {
        match timeout(LIMIT, &mut attempt).await {
            Ok(Ok(Ok((client, _, _)))) => clean &= timeout(LIMIT, client.close()).await.is_ok(),
            Ok(_) => {}
            Err(_) => {
                attempt.abort();
                let _ = attempt.await;
                clean = false;
            }
        }
    }
    clean &= room_cleanup(&admin, &room).await.is_ok();
    match result {
        Err(error) => Err(error.context(if clean {
            "probe failed; exact relay and unique room cleanup confirmed"
        } else {
            "probe failed; fixture cleanup also failed"
        })),
        Ok(()) => {
            ensure!(clean, "probe fixture cleanup was not confirmed");
            Ok(())
        }
    }
}

async fn room_cleanup(admin: &RoomClient, room: &str) -> Result<()> {
    let deleted = matches!(timeout(LIMIT, admin.delete_room(room)).await, Ok(Ok(())));
    let absent = deleted
        || matches!(
            timeout(LIMIT, admin.list_rooms(vec![room.to_owned()])).await,
            Ok(Ok(rooms)) if rooms.is_empty()
        );
    ensure!(absent, "the unique initial-timeout room was not removed");
    Ok(())
}

#[tokio::test]
async fn resume_response_timeout_closes_its_unowned_signal_stream_and_allows_retry() -> Result<()> {
    use super::{rtc_session::RtcSession, EngineOptions};
    use crate::room::DisconnectReason;
    use std::sync::Arc;

    let backend =
        std::env::var("LIVEKIT_URL").map_err(|_| anyhow::anyhow!("LIVEKIT_URL is required"))?;
    let authority = backend
        .strip_prefix("http://127.0.0.1:")
        .or_else(|| backend.strip_prefix("ws://127.0.0.1:"))
        .or_else(|| backend.strip_prefix("http://localhost:"))
        .or_else(|| backend.strip_prefix("ws://localhost:"))
        .context("an explicitly selected plain local SFU is required")?;
    ensure!(
        authority.parse::<u16>().is_ok(),
        "local SFU port is invalid"
    );
    let key = std::env::var("LIVEKIT_API_KEY")
        .map_err(|_| anyhow::anyhow!("LIVEKIT_API_KEY is required"))?;
    let secret = std::env::var("LIVEKIT_API_SECRET")
        .map_err(|_| anyhow::anyhow!("LIVEKIT_API_SECRET is required"))?;
    let admin = RoomClient::with_api_key(&backend.replacen("ws://", "http://", 1), &key, &secret);
    let nonce = libwebrtc::native::create_random_uuid();
    let room = format!("resume_response_timeout_{nonce}");
    let identity = format!("resume-response-timeout-{nonce}");
    let token = AccessToken::with_api_key(&key, &secret)
        .with_identity(&identity)
        .with_ttl(Duration::from_secs(120))
        .with_grants(VideoGrants {
            room_join: true,
            room: room.clone(),
            ..Default::default()
        })
        .to_jwt()
        .map_err(|_| anyhow::anyhow!("private probe token creation failed"))?;
    let mut proxy = JoinProxy::start_resume(&backend).await?;
    let mut session: Option<Arc<RtcSession>> = None;
    let mut restart = None;
    let mut restart_completed = false;
    let result = async {
        timeout(
            LIMIT,
            admin.create_room(
                &room,
                CreateRoomOptions {
                    empty_timeout: 120,
                    departure_timeout: 120,
                    ..Default::default()
                },
            ),
        )
        .await
        .context("private resume room creation timed out")?
        .map_err(|_| anyhow::anyhow!("private resume room creation failed"))?;
        // Use real connected PCs, but no Engine: nothing may automatically close
        // or reconnect the session while we observe the ordinary signal error.
        let (connected, response, _session_events) = timeout(
            LIMIT,
            RtcSession::connect(&proxy.url, &token, EngineOptions::default(), None),
        )
        .await
        .context("initial real session construction timed out")?
        .map_err(|_| anyhow::anyhow!("initial real session construction failed"))?;
        let connected = Arc::new(connected);
        session = Some(connected.clone());
        timeout(LIMIT, connected.wait_pc_connection())
            .await
            .context("initial peer connection timed out")?
            .map_err(|_| anyhow::anyhow!("initial peer connection failed"))?;
        let sid = response
            .participant
            .context("initial Join participant missing")?
            .sid;
        let client = connected.signal_client().clone();
        let owner = client.clone();
        restart = Some(tokio::spawn(async move { owner.restart().await }));
        let held_at = proxy.held().await?;
        let people = timeout(LIMIT, admin.list_participants(&room))
            .await
            .context("resume admission observation timed out")?
            .map_err(|_| anyhow::anyhow!("resume admission observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].identity == identity && people[0].sid == sid,
            "the held resume did not belong to the exact connected probe"
        );
        let completed = timeout(JOIN_RESPONSE_TIMEOUT + LIMIT, restart.as_mut().unwrap())
            .await
            .context("the real signal restart did not complete")?;
        restart_completed = true;
        match completed.context("real signal restart task panicked")? {
            Err(SignalError::Timeout(message)) => ensure!(
                message.starts_with("failed to receive ") && message.ends_with("ReconnectResponse"),
                "restart error was not the real Resume-response timeout"
            ),
            Err(_) => anyhow::bail!("signal restart failed at another stage"),
            Ok(_) => anyhow::bail!("the held Resume unexpectedly succeeded"),
        }
        ensure!(
            held_at.elapsed() >= JOIN_RESPONSE_TIMEOUT - Duration::from_millis(100),
            "restart returned before its own Resume-response timeout"
        );
        // No session/engine close, task abort, room removal or relay teardown has
        // occurred. A resumed participant can retain its PC during signal recovery;
        // prove disposal of this exact new signal socket rather than SFU absence.
        proxy.release();
        proxy.closed().await?;
        ensure!(
            !client.is_connected().await,
            "failed resume published a signal stream"
        );
        // The one-shot barrier is consumed. An unheld retry must succeed, retain
        // its private receiver in middleware, and keep the same participant SID.
        timeout(LIMIT, client.restart())
            .await
            .context("normal Resume retry timed out")?
            .map_err(|_| anyhow::anyhow!("normal Resume retry failed"))?;
        client.set_reconnected().await;
        ensure!(
            client.is_connected().await,
            "successful Resume did not retain its stream"
        );
        let people = timeout(LIMIT, admin.list_participants(&room))
            .await
            .context("retry admission observation timed out")?
            .map_err(|_| anyhow::anyhow!("retry admission observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].identity == identity && people[0].sid == sid,
            "normal Resume retry replaced or lost the exact participant"
        );
        Ok(())
    }
    .await;

    // Oracles above precede every teardown operation. On an early failure,
    // preserve and drain any pending real restart before exact fixture removal.
    proxy.release();
    let mut clean = true;
    if !restart_completed {
        if let Some(attempt) = restart.as_mut() {
            if timeout(LIMIT, &mut *attempt).await.is_err() {
                attempt.abort();
                let _ = attempt.await;
                clean = false;
            }
        }
    }
    if let Some(connected) = session.take() {
        clean &= timeout(LIMIT, connected.close(DisconnectReason::ClientInitiated))
            .await
            .is_ok();
    }
    clean &= proxy.shutdown().await;
    clean &= room_cleanup(&admin, &room).await.is_ok();
    match result {
        Err(error) => Err(error.context(if clean {
            "resume probe failed; exact relay and unique room cleanup confirmed"
        } else {
            "resume probe failed; fixture cleanup also failed"
        })),
        Ok(()) => {
            ensure!(clean, "resume probe fixture cleanup was not confirmed");
            Ok(())
        }
    }
}
