// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Real initial post-Join ICE failure/cancellation must dispose its unowned session.
//! Negotiation is withheld while Join and signal keepalives pass through unchanged.

use super::{join_proxy::JoinProxy, room_cleanup, ABSENCE_LIMIT, LIMIT};
use crate::{
    room::DisconnectReason,
    rtc_engine::{rtc_session::ICE_CONNECT_TIMEOUT, EngineError, EngineOptions, RtcEngine},
};
use anyhow::{ensure, Context, Result};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::room::{CreateRoomOptions, RoomClient},
};
use std::{
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

// The relay alone cannot prove the caller consumed Join before cancellation.
// A URL-keyed one-shot observes the real completed RtcSession, without pausing
// production logic or replacing any constructor/error/transport operation.
static CONSTRUCTED: LazyLock<parking_lot::Mutex<HashMap<String, oneshot::Sender<Instant>>>> =
    LazyLock::new(Default::default);

pub(in crate::rtc_engine) fn session_constructed(url: &str) {
    if let Some(constructed) = CONSTRUCTED.lock().remove(url) {
        let _ = constructed.send(Instant::now());
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Timeout,
    Cancelled,
}

#[tokio::test]
async fn initial_ice_timeout_closes_its_unowned_session_and_allows_retry() -> Result<()> {
    run(Outcome::Timeout).await
}

#[tokio::test]
async fn cancelled_initial_ice_wait_closes_its_unowned_session_and_allows_retry() -> Result<()> {
    run(Outcome::Cancelled).await
}

async fn run(outcome: Outcome) -> Result<()> {
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
    let room = format!("initial_ice_{nonce}");
    let identity = format!("initial-ice-{nonce}");
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
    let mut proxy = JoinProxy::start_ice(&backend).await?;
    let (constructed, mut constructed_rx) = oneshot::channel();
    CONSTRUCTED.lock().insert(proxy.url.clone(), constructed);
    let mut attempt = None;
    let mut attempt_completed = false;
    let mut returned_engine = None;
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
        .context("private initial ICE room creation timed out")?
        .map_err(|_| anyhow::anyhow!("private initial ICE room creation failed"))?;
        let url = proxy.url.clone();
        let attempt_token = token.clone();
        attempt = Some(tokio::spawn(async move {
            // Isolate one real failed acquisition; do not turn a 15s timeout
            // into four attempts or replace the constructor's actual error.
            let options = EngineOptions {
                join_retries: 0,
                ..Default::default()
            };
            RtcEngine::connect(&url, &attempt_token, options, None).await
        }));
        // The relay has forwarded the real Join and withheld an actual SFU
        // Offer. Signal pings/pongs continue; no transport is torn down here.
        proxy.held().await?;
        let constructed_at = timeout(LIMIT, &mut constructed_rx)
            .await
            .context("initial RtcSession construction did not finish")?
            .context("initial RtcSession construction marker disappeared")?;
        let people = timeout(LIMIT, admin.list_participants(&room))
            .await
            .context("initial ICE admission observation timed out")?
            .map_err(|_| anyhow::anyhow!("initial ICE admission observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].identity == identity,
            "the exact post-Join probe was not admitted to its unique room"
        );
        let pending = attempt
            .as_mut()
            .context("initial ICE attempt was not started")?;
        match outcome {
            Outcome::Timeout => {
                let completed = timeout(ICE_CONNECT_TIMEOUT + LIMIT, &mut *pending)
                    .await
                    .context("the actual initial ICE constructor did not finish")?;
                attempt_completed = true;
                match completed.context("initial ICE constructor task panicked")? {
                    Err(EngineError::Connection(message)) => ensure!(
                        message.as_ref() == "wait_pc_connection timed out",
                        "constructor error was not the real initial ICE timeout"
                    ),
                    Err(_) => anyhow::bail!("initial ICE constructor failed at another stage"),
                    Ok((engine, _, _)) => {
                        returned_engine = Some(engine);
                        anyhow::bail!("the negotiation-blocked initial constructor succeeded");
                    }
                }
                ensure!(
                    constructed_at.elapsed() >= ICE_CONNECT_TIMEOUT - Duration::from_millis(100),
                    "constructor returned before its own 15-second ICE timeout"
                );
            }
            Outcome::Cancelled => {
                pending.abort();
                let cancelled = pending.await;
                attempt_completed = true;
                match cancelled {
                    Err(error) if error.is_cancelled() => {}
                    Ok(Ok((engine, _, _))) => {
                        returned_engine = Some(engine);
                        anyhow::bail!("the post-Join caller completed before cancellation");
                    }
                    _ => anyhow::bail!("the post-Join caller was not cancelled"),
                }
            }
        }
        // Neither negotiation release, caller-provided close, relay teardown
        // nor room removal may manufacture this closure/absence oracle.
        let (socket, participant) = tokio::join!(proxy.closed(), async {
            timeout(ABSENCE_LIMIT, async {
                loop {
                    let people = timeout(Duration::from_secs(1), admin.list_participants(&room))
                        .await
                        .map_err(|_| anyhow::anyhow!("ICE probe absence observation timed out"))?
                        .map_err(|_| anyhow::anyhow!("ICE probe absence observation failed"))?;
                    if people.is_empty() {
                        return Result::<()>::Ok(());
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .context("initial ICE failure left its SFU participant behind")?
        });
        socket?;
        participant?;
        // The same relay's one-shot hold has been consumed. This normal retry
        // also proves successful initial adoption does not dispose its session.
        let options = EngineOptions {
            join_retries: 0,
            ..Default::default()
        };
        let (engine, joined, _events) = timeout(
            ICE_CONNECT_TIMEOUT + LIMIT,
            RtcEngine::connect(&proxy.url, &token, options, None),
        )
        .await
        .context("normal initial ICE retry timed out")?
        .map_err(|_| anyhow::anyhow!("normal initial ICE retry failed"))?;
        returned_engine = Some(engine);
        let retry_sid = joined
            .participant
            .context("retry Join participant missing")?
            .sid;
        sleep(Duration::from_millis(100)).await;
        let people = timeout(LIMIT, admin.list_participants(&room))
            .await
            .context("retry admission observation timed out")?
            .map_err(|_| anyhow::anyhow!("retry admission observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].identity == identity && people[0].sid == retry_sid,
            "successful initial retry lost or replaced its adopted session"
        );
        Ok(())
    }
    .await;

    // Preserve the oracle result before exact owned fixture cleanup, including RED.
    CONSTRUCTED.lock().remove(&proxy.url);
    proxy.release();
    let mut clean = true;
    if let Some(engine) = returned_engine.take() {
        clean &= timeout(LIMIT, engine.close(DisconnectReason::ClientInitiated))
            .await
            .is_ok();
    }
    clean &= proxy.shutdown().await;
    if !attempt_completed {
        if let Some(pending) = attempt.as_mut() {
            match timeout(ICE_CONNECT_TIMEOUT + LIMIT, &mut *pending).await {
                Ok(Ok(Ok((engine, _, _)))) => {
                    clean &= timeout(LIMIT, engine.close(DisconnectReason::ClientInitiated))
                        .await
                        .is_ok();
                }
                Ok(Ok(Err(_))) => {}
                Ok(Err(_)) => clean = false,
                Err(_) => {
                    pending.abort();
                    let _ = pending.await;
                    clean = false;
                }
            }
        }
    }
    clean &= room_cleanup(&admin, &room).await.is_ok();
    match result {
        Err(error) => Err(error.context(if clean {
            "initial ICE probe failed; exact relay and unique room cleanup confirmed"
        } else {
            "initial ICE probe failed; fixture cleanup also failed"
        })),
        Ok(()) => {
            ensure!(clean, "initial ICE probe fixture cleanup was not confirmed");
            Ok(())
        }
    }
}
