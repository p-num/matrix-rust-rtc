// Copyright 2026 Element Creations Ltd.
// Licensed under the Apache License, Version 2.0.
// See LICENSE-APACHE in this vendored crate for the license text.

//! Exercises actual reconnect ownership against an explicitly selected local SFU.
//! Barriers control scheduling; every resource oracle observes the real server.

use super::*;
use anyhow::{ensure, Context, Result};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::room::{CreateRoomOptions, RoomClient},
};
use tokio::{
    sync::watch,
    time::{sleep, timeout},
};

mod resume_proxy;
use resume_proxy::ResumeProxy;

const LIMIT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
enum Scenario {
    Connected,
    Acquisition,
    Promotion,
    Adopted,
    Resume,
}

#[tokio::test]
async fn close_during_full_restart_does_not_leave_sfu_participant() -> Result<()> {
    run(Scenario::Connected).await
}

#[tokio::test]
async fn cancelled_acquisition_receiver_closes_its_late_session() -> Result<()> {
    run(Scenario::Acquisition).await
}

#[tokio::test]
async fn close_rejects_a_connected_candidate_before_promotion() -> Result<()> {
    run(Scenario::Promotion).await
}

#[tokio::test]
async fn close_after_successful_reconnect_closes_the_adopted_session() -> Result<()> {
    run(Scenario::Adopted).await
}

#[tokio::test]
async fn close_during_resume_response_drains_the_owned_signal_stream() -> Result<()> {
    run(Scenario::Resume).await
}

async fn run(scenario: Scenario) -> Result<()> {
    let fixture = Fixture::connect(matches!(scenario, Scenario::Resume)).await?;
    let result = match scenario {
        Scenario::Connected => close_connected(&fixture).await,
        Scenario::Acquisition => cancel_acquisition_receiver(&fixture).await,
        Scenario::Promotion => reject_promotion(&fixture).await,
        Scenario::Adopted => close_adopted(&fixture).await,
        Scenario::Resume => close_resuming(&fixture).await,
    };
    let cleanup = fixture.cleanup().await;
    match result {
        Err(primary) => Err(primary.context(if cleanup.is_ok() {
            "exercise failed; unique test room cleanup confirmed"
        } else {
            "exercise failed; unique test room cleanup also failed"
        })),
        Ok(()) => cleanup,
    }
}

struct Gate {
    reached: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl Gate {
    async fn wait(&mut self) -> Result<()> {
        timeout(LIMIT, &mut self.reached)
            .await
            .context("scheduling barrier timed out")?
            .context("scheduling barrier disappeared")
    }

    fn release(self) {
        let _ = self.release.send(());
    }
}

struct Fixture {
    engine: RtcEngine,
    old_sid: String,
    url: String,
    token: String,
    admin: RoomClient,
    room: String,
    event_task: tokio::task::JoinHandle<()>,
    disposals: parking_lot::Mutex<Vec<watch::Receiver<bool>>>,
    proxy: Option<ResumeProxy>,
}

impl Fixture {
    async fn connect(hold_resume: bool) -> Result<Self> {
        let backend_url =
            std::env::var("LIVEKIT_URL").map_err(|_| anyhow::anyhow!("LIVEKIT_URL is required"))?;
        ensure!(
            [
                "http://127.0.0.1:",
                "ws://127.0.0.1:",
                "http://localhost:",
                "ws://localhost:"
            ]
            .iter()
            .any(|prefix| backend_url.starts_with(prefix)),
            "a private local SFU is required"
        );
        let key = std::env::var("LIVEKIT_API_KEY")
            .map_err(|_| anyhow::anyhow!("LIVEKIT_API_KEY is required"))?;
        let secret = std::env::var("LIVEKIT_API_SECRET")
            .map_err(|_| anyhow::anyhow!("LIVEKIT_API_SECRET is required"))?;
        let admin =
            RoomClient::with_api_key(&backend_url.replacen("ws://", "http://", 1), &key, &secret);
        let proxy = if hold_resume {
            Some(ResumeProxy::start(&backend_url).await?)
        } else {
            None
        };
        let url = proxy
            .as_ref()
            .map_or(backend_url, |proxy| proxy.url.clone());
        let room = format!(
            "reconnect_close_{}",
            libwebrtc::native::create_random_uuid()
        );
        let token = AccessToken::with_api_key(&key, &secret)
            .with_identity("reconnect-close")
            .with_ttl(Duration::from_secs(120))
            .with_grants(VideoGrants {
                room_join: true,
                room: room.clone(),
                ..Default::default()
            })
            .to_jwt()
            .map_err(|_| anyhow::anyhow!("private test token creation failed"))?;
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
            let absent = delete_private_room(&admin, &room).await;
            anyhow::bail!(if absent {
                "private room creation failed; cleanup confirmed"
            } else {
                "private room creation failed; cleanup also failed"
            });
        }
        let initial = timeout(
            LIMIT,
            RtcEngine::connect(&url, &token, EngineOptions::default(), None),
        )
        .await;
        let (engine, joined, mut events) = match initial {
            Ok(Ok(value)) => value,
            _ => {
                let cleanup = delete_private_room(&admin, &room).await;
                anyhow::bail!(
                    "initial private engine connection failed; unique room cleanup {}",
                    if cleanup { "confirmed" } else { "failed" }
                );
            }
        };
        let Some(participant) = joined.participant else {
            let _ = timeout(LIMIT, engine.close(DisconnectReason::ClientInitiated)).await;
            let cleanup = delete_private_room(&admin, &room).await;
            anyhow::bail!(
                "initial participant missing; unique room cleanup {}",
                if cleanup { "confirmed" } else { "failed" }
            );
        };
        let old_sid = participant.sid;
        // Mirror Room acknowledgements and discard its receiver on close.
        let event_task = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    EngineEvent::Restarting(tx)
                    | EngineEvent::Restarted(tx)
                    | EngineEvent::Resuming(tx)
                    | EngineEvent::Resumed(tx)
                    | EngineEvent::SignalRestarted { tx, .. }
                    | EngineEvent::SignalResumed { tx, .. } => {
                        let _ = tx.send(());
                    }
                    EngineEvent::Disconnected { .. } => break,
                    _ => {}
                }
            }
        });
        Ok(Self {
            engine,
            old_sid,
            url,
            token,
            admin,
            room,
            event_task,
            disposals: parking_lot::Mutex::new(Vec::new()),
            proxy,
        })
    }

    async fn gate(&self, point: RestartBarrierPoint) -> Result<Gate> {
        let (reached, receiver) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut slot = self.engine.inner.restart_candidate_barrier.lock().await;
        ensure!(
            slot.is_none(),
            "an earlier scheduling barrier is still armed"
        );
        *slot = Some(RestartCandidateBarrier {
            point,
            reached,
            release: released,
        });
        Ok(Gate {
            reached: receiver,
            release,
        })
    }

    fn disposal(&self) -> Result<watch::Receiver<bool>> {
        let (sender, receiver) = watch::channel(false);
        let mut slot = self.engine.inner.candidate_disposal.lock();
        ensure!(slot.is_none(), "an earlier disposal witness is still armed");
        *slot = Some(sender);
        self.disposals.lock().push(receiver.clone());
        Ok(receiver)
    }

    async fn fresh_candidate(&self) -> Result<()> {
        let people = timeout(LIMIT, self.admin.list_participants(&self.room))
            .await
            .context("candidate observation timed out")?
            .map_err(|_| anyhow::anyhow!("candidate observation failed"))?;
        ensure!(
            people.len() == 1 && people[0].sid != self.old_sid,
            "no fresh admitted reconnect candidate was measured"
        );
        Ok(())
    }

    fn close(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.engine.inner.clone();
        tokio::spawn(async move {
            engine.close(DisconnectReason::ClientInitiated).await;
        })
    }

    async fn marked_closed(&self) -> Result<()> {
        timeout(LIMIT, async {
            while !self.engine.inner.running_handle.read().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("actual engine did not become terminal")
    }

    async fn outer_owner_done(&self) -> Result<()> {
        drop(
            timeout(LIMIT, self.engine.inner.reconnecting_lock.read())
                .await
                .context("outer reconnect owner did not finish")?,
        );
        Ok(())
    }

    async fn owners_done(&self) -> Result<()> {
        self.outer_owner_done().await?;
        let mut owners = self.engine.inner.owned_attempts.subscribe();
        timeout(LIMIT, owners.wait_for(|count| *count == 0))
            .await
            .context("owned acquisition did not finish")?
            .map(|_| ())
            .context("owned acquisition witness disappeared")?;
        Ok(())
    }

    async fn absent(&self) -> Result<()> {
        timeout(Duration::from_secs(5), async {
            loop {
                let people = timeout(
                    Duration::from_secs(2),
                    self.admin.list_participants(&self.room),
                )
                .await
                .map_err(|_| anyhow::anyhow!("SFU absence observation timed out"))?
                .map_err(|_| anyhow::anyhow!("SFU absence observation failed"))?;
                if people.is_empty() {
                    return Result::<()>::Ok(());
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("a reconnect candidate remained on SFU after engine close")?
    }

    fn no_installed_task(&self) -> Result<()> {
        ensure!(
            self.engine
                .inner
                .running_handle
                .read()
                .engine_task
                .is_none(),
            "a reconnect installed a fresh task after close"
        );
        Ok(())
    }

    async fn cleanup(mut self) -> Result<()> {
        // Capture every result: one failed cleanup step must not skip the rest.
        if let Some(proxy) = &self.proxy {
            proxy.release();
        }
        let first_close = timeout(LIMIT, self.engine.close(DisconnectReason::ClientInitiated))
            .await
            .is_ok();
        let owners = self.owners_done().await.is_ok();
        let second_close = timeout(LIMIT, self.engine.close(DisconnectReason::ClientInitiated))
            .await
            .is_ok();
        self.engine.inner.candidate_disposal.lock().take();
        let disposals = self.disposals.into_inner();
        let mut disposed = true;
        for mut witness in disposals {
            let finished = timeout(LIMIT, witness.wait_for(|done| *done)).await;
            // Closed without true means no candidate was created or it was
            // adopted; the explicit second close owns the latter's disposal.
            disposed &= !matches!(finished, Err(_));
        }
        self.event_task.abort();
        let absent = delete_private_room(&self.admin, &self.room).await;
        if let Some(proxy) = &mut self.proxy {
            proxy.shutdown().await;
        }
        ensure!(
            first_close && owners && second_close && disposed && absent,
            "private regression engine/room cleanup was not confirmed"
        );
        Ok(())
    }
}

async fn delete_private_room(admin: &RoomClient, room: &str) -> bool {
    matches!(timeout(LIMIT, admin.delete_room(room)).await, Ok(Ok(())))
        || matches!(timeout(LIMIT, admin.list_rooms(vec![room.to_owned()])).await,
            Ok(Ok(rooms)) if rooms.is_empty())
}

async fn disposed(mut witness: watch::Receiver<bool>) -> Result<()> {
    timeout(LIMIT, witness.wait_for(|done| *done))
        .await
        .context("candidate disposal did not finish")?
        .map(|_| ())
        .context("candidate was not disposed by its acquisition owner")?;
    Ok(())
}

async fn close_connected(fixture: &Fixture) -> Result<()> {
    let witness = fixture.disposal()?;
    let mut gate = fixture.gate(RestartBarrierPoint::Connected).await?;
    fixture.engine.inner.reconnection_needed(true, true);
    gate.wait().await?;
    fixture.fresh_candidate().await?;
    let close = fixture.close();
    fixture.marked_closed().await?;
    gate.release();
    timeout(LIMIT, close)
        .await
        .context("engine close timed out")?
        .context("engine close panicked")?;
    fixture.owners_done().await?;
    disposed(witness).await?;
    fixture.absent().await?;
    fixture.no_installed_task()
}

async fn cancel_acquisition_receiver(fixture: &Fixture) -> Result<()> {
    let witness = fixture.disposal()?;
    let mut acquiring = fixture.gate(RestartBarrierPoint::Acquiring).await?;
    fixture.engine.inner.reconnection_needed(true, true);
    acquiring.wait().await?;
    let close = fixture.close();
    fixture.marked_closed().await?;
    // The acquisition child is still held, so this proves its receiver was
    // cancelled while the child retains ownership of the pending constructor.
    fixture.outer_owner_done().await?;
    timeout(LIMIT, close)
        .await
        .context("engine close timed out")?
        .context("engine close panicked")?;
    let mut acquired = fixture.gate(RestartBarrierPoint::Acquired).await?;
    acquiring.release();
    acquired.wait().await?;
    fixture.fresh_candidate().await?;
    acquired.release();
    fixture.owners_done().await?;
    disposed(witness).await?;
    fixture.absent().await?;
    fixture.no_installed_task()
}

async fn reject_promotion(fixture: &Fixture) -> Result<()> {
    let witness = fixture.disposal()?;
    let mut gate = fixture.gate(RestartBarrierPoint::Promotion).await?;
    let owner = fixture.engine.inner.clone();
    let url = fixture.url.clone();
    let token = fixture.token.clone();
    // Invoke the production restart function directly: close's outer select
    // cannot mask the final closed-state promotion check in this test.
    let restart = tokio::spawn(async move {
        owner
            .try_restart_connection(&url, &token, EngineOptions::default(), None)
            .await
    });
    gate.wait().await?;
    fixture.fresh_candidate().await?;
    timeout(
        LIMIT,
        fixture.engine.close(DisconnectReason::ClientInitiated),
    )
    .await
    .context("engine close timed out")?;
    gate.release();
    let outcome = timeout(LIMIT, restart)
        .await
        .context("restart did not finish")?
        .context("restart panicked")?;
    ensure!(
        outcome.is_err(),
        "a terminal engine accepted a reconnect candidate"
    );
    disposed(witness).await?;
    fixture.absent().await?;
    fixture.no_installed_task()
}

async fn close_adopted(fixture: &Fixture) -> Result<()> {
    let mut gate = fixture.gate(RestartBarrierPoint::Promotion).await?;
    fixture.engine.inner.reconnection_needed(true, true);
    gate.wait().await?;
    fixture.fresh_candidate().await?;
    gate.release();
    fixture.owners_done().await?;
    {
        let state = fixture.engine.inner.running_handle.read();
        ensure!(
            !state.closed && state.engine_task.is_some(),
            "the fresh session was never adopted"
        );
    }
    timeout(
        LIMIT,
        fixture.engine.close(DisconnectReason::ClientInitiated),
    )
    .await
    .context("adopted session close timed out")?;
    fixture.absent().await?;
    fixture.no_installed_task()
}

async fn close_resuming(fixture: &Fixture) -> Result<()> {
    let proxy = fixture.proxy.as_ref().context("resume relay missing")?;
    timeout(
        LIMIT,
        fixture
            .engine
            .simulate_scenario(SimulateScenario::SignalReconnect),
    )
    .await
    .context("signal reconnect trigger timed out")?
    .map_err(|_| anyhow::anyhow!("signal reconnect trigger failed"))?;
    let held_at = proxy.held().await?;
    let close = fixture.close();
    fixture.marked_closed().await?;
    fixture.outer_owner_done().await?;
    // Join-response timeout is five seconds. Reject a slow scheduling branch
    // instead of accidentally testing timeout cleanup rather than cancellation.
    ensure!(
        held_at.elapsed() < Duration::from_secs(3),
        "resume response hold exceeded the cancellation window"
    );
    proxy.release();
    timeout(LIMIT, close)
        .await
        .context("engine close during resume timed out")?
        .context("engine close during resume panicked")?;
    fixture.owners_done().await?;
    fixture.absent().await?;
    proxy.closed().await?;
    fixture.no_installed_task()
}
