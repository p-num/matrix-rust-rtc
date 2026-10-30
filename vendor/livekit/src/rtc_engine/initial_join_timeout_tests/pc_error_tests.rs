// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! A real post-Join peer-connection constructor error must release its signal owner.
//! The real CXX error, rather than admission polling, proves the selected stage.

use super::{room_cleanup, ABSENCE_LIMIT, LIMIT};
use crate::{
    room::DisconnectReason,
    rtc_engine::{rtc_session::RtcSession, EngineError, EngineOptions},
};
use anyhow::{ensure, Context, Result};
use libwebrtc::{
    peer_connection_factory::{IceServer, RtcConfiguration},
    RtcErrorType,
};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::room::{CreateRoomOptions, RoomClient},
};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn post_join_pc_constructor_error_closes_its_unowned_signal_client() -> Result<()> {
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
    let room = format!("initial_pc_error_{nonce}");
    let identity = format!("initial-pc-error-{nonce}");
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
    let mut attempt = None;
    let mut attempt_completed = false;
    let mut returned_session = None;
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
        .context("private PC-error room creation timed out")?
        .map_err(|_| anyhow::anyhow!("private PC-error room creation failed"))?;
        let url = backend.clone();
        let attempt_token = token.clone();
        attempt = Some(tokio::spawn(async move {
            let mut options = EngineOptions::default();
            // Dual-PC creation is strictly after successful SignalClient Join.
            // A nonempty list survives make_rtc_config_join's server merge.
            options.signal_options.single_peer_connection = false;
            options.rtc_config = RtcConfiguration {
                ice_servers: vec![IceServer {
                    urls: vec!["invalid-ice-scheme:post-join-probe".to_owned()],
                    username: String::new(),
                    password: String::new(),
                }],
                ..Default::default()
            };
            RtcSession::connect(&url, &attempt_token, options, None).await
        }));
        let pending = attempt.as_mut().context("PC-error probe was not started")?;
        let completed = timeout(LIMIT, pending)
            .await
            .context("real post-Join constructor did not finish")?;
        attempt_completed = true;
        match completed.context("real post-Join constructor task panicked")? {
            Err(EngineError::Rtc(error)) => ensure!(
                error.error_type == RtcErrorType::Internal
                    && error.message == "failed to initialize pc",
                "constructor error was not the real CXX peer-connection failure"
            ),
            Err(_) => anyhow::bail!("constructor failed before the selected real RTC stage"),
            Ok((session, _, _)) => {
                returned_session = Some(session);
                anyhow::bail!("invalid real ICE configuration was unexpectedly accepted");
            }
        }
        // The actual CXX error above can only occur after Join was consumed on
        // this explicit dual-PC path. Do not require catching brief admission
        // on GREEN, where cleanup may already have completed before this poll.
        timeout(ABSENCE_LIMIT, async {
            loop {
                let people = timeout(Duration::from_secs(1), admin.list_participants(&room))
                    .await
                    .map_err(|_| anyhow::anyhow!("PC-error absence observation timed out"))?
                    .map_err(|_| anyhow::anyhow!("PC-error absence observation failed"))?;
                if people.is_empty() {
                    return Result::<()>::Ok(());
                }
                ensure!(
                    people.len() == 1 && people[0].identity == identity,
                    "the unique PC-error room contained another identity"
                );
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("post-Join PC constructor error left its SFU participant behind")??;
        Ok(())
    }
    .await;

    // Preserve the measured error/absence result before any fixture deletion.
    // A pending constructor is drained; any unexpectedly returned session closes.
    let mut clean = true;
    if let Some(session) = returned_session.take() {
        clean &= timeout(LIMIT, session.close(DisconnectReason::ClientInitiated))
            .await
            .is_ok();
    }
    if !attempt_completed {
        if let Some(pending) = attempt.as_mut() {
            match timeout(LIMIT, &mut *pending).await {
                Ok(Ok(Ok((session, _, _)))) => {
                    clean &= timeout(LIMIT, session.close(DisconnectReason::ClientInitiated))
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
            "PC-error probe failed; unique room cleanup confirmed"
        } else {
            "PC-error probe failed; fixture cleanup also failed"
        })),
        Ok(()) => {
            ensure!(clean, "PC-error probe fixture cleanup was not confirmed");
            Ok(())
        }
    }
}
