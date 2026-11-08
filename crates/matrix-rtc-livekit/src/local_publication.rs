// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Keep local mute intent coherent while a transport publishes or replaces tracks.
//! Weak registrations cover live native tracks without retaining retired publications.

use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

pub(crate) struct LocalPublicationMute<T> {
    state: Mutex<MuteState<T>>,
    apply: fn(&T, bool),
}

struct MuteState<T> {
    muted: bool,
    tracks: Vec<Weak<T>>,
}

impl<T> LocalPublicationMute<T> {
    pub(crate) fn new(muted: bool, apply: fn(&T, bool)) -> Self {
        Self {
            state: Mutex::new(MuteState {
                muted,
                tracks: Vec::new(),
            }),
            apply,
        }
    }

    pub(crate) async fn publish<F, Fut, C, P, E>(
        &self,
        track: T,
        publish: F,
        complete: C,
    ) -> Result<Arc<T>, E>
    where
        F: FnOnce(Arc<T>) -> Fut,
        Fut: Future<Output = Result<P, E>>,
        C: FnOnce(P, bool),
    {
        let track = Arc::new(track);
        {
            let mut state = self.state.lock().expect("local mute mutex poisoned");
            (self.apply)(&track, state.muted);
            // Register before the asynchronous handoff: mute changes must reach
            // pending tracks too, including a privacy mute before publish returns.
            state.tracks.retain(|track| track.strong_count() > 0);
            state.tracks.push(Arc::downgrade(&track));
        }
        let publication = publish(Arc::clone(&track)).await?;
        {
            let state = self.state.lock().expect("local mute mutex poisoned");
            // Publication can enable the native track and attach mute listeners
            // late. Restore capture state and confirm the current intent on wire.
            (self.apply)(&track, state.muted);
            complete(publication, state.muted);
        }
        Ok(track)
    }

    pub(crate) fn set_muted(&self, muted: bool) {
        let mut state = self.state.lock().expect("local mute mutex poisoned");
        state.muted = muted;
        state.tracks.retain(|track| {
            if let Some(track) = track.upgrade() {
                (self.apply)(&track, muted);
                true
            } else {
                false
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::oneshot;

    use super::*;

    struct Track(Arc<AtomicBool>);

    fn apply(track: &Track, muted: bool) {
        track.0.store(muted, Ordering::SeqCst);
    }

    async fn pending_changes(initial: bool, changes: &[bool]) {
        let mute = Arc::new(LocalPublicationMute::new(initial, apply));
        let actual = Arc::new(AtomicBool::new(!initial));
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let publishing = {
            let mute = Arc::clone(&mute);
            let actual = Arc::clone(&actual);
            tokio::spawn(async move {
                mute.publish(
                    Track(actual),
                    |_| async move {
                        started_tx.send(()).unwrap();
                        finish_rx.await.unwrap();
                        Ok::<(), ()>(())
                    },
                    |(), _| {},
                )
                .await
            })
        };
        started_rx.await.unwrap();
        assert_eq!(actual.load(Ordering::SeqCst), initial);
        for &muted in changes {
            mute.set_muted(muted);
            assert_eq!(
                actual.load(Ordering::SeqCst),
                muted,
                "mute intent must reach a track before its publication completes"
            );
        }
        finish_tx.send(()).unwrap();
        let track = publishing.await.unwrap().unwrap();
        assert_eq!(track.0.load(Ordering::SeqCst), *changes.last().unwrap());
    }

    #[tokio::test]
    async fn camera_unmute_during_publication_is_not_lost() {
        pending_changes(true, &[false]).await;
    }

    #[tokio::test]
    async fn microphone_mute_applies_before_publication_completes() {
        pending_changes(false, &[true]).await;
    }

    #[tokio::test]
    async fn pending_publication_keeps_the_latest_of_several_changes() {
        pending_changes(true, &[false, true, false]).await;
    }

    #[tokio::test]
    async fn new_publications_start_with_the_current_intent() {
        let mute = LocalPublicationMute::new(false, apply);
        mute.set_muted(true);
        let actual = Arc::new(AtomicBool::new(false));
        let track = mute
            .publish(
                Track(Arc::clone(&actual)),
                |_| async { Ok::<(), ()>(()) },
                |(), _| {},
            )
            .await
            .unwrap();
        assert!(actual.load(Ordering::SeqCst));
        mute.set_muted(false);
        assert!(!track.0.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn current_and_replacement_tracks_follow_the_same_intent() {
        let mute = LocalPublicationMute::new(false, apply);
        let first = mute
            .publish(
                Track(Arc::new(AtomicBool::new(true))),
                |_| async { Ok::<(), ()>(()) },
                |(), _| {},
            )
            .await
            .unwrap();
        let second = mute
            .publish(
                Track(Arc::new(AtomicBool::new(true))),
                |_| async { Ok::<(), ()>(()) },
                |(), _| {},
            )
            .await
            .unwrap();
        mute.set_muted(true);
        assert!(first.0.load(Ordering::SeqCst));
        assert!(second.0.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn retired_tracks_are_not_kept_alive() {
        let mute = LocalPublicationMute::new(false, apply);
        let track = mute
            .publish(
                Track(Arc::new(AtomicBool::new(false))),
                |_| async { Ok::<(), ()>(()) },
                |(), _| {},
            )
            .await
            .unwrap();
        let retired = Arc::downgrade(&track);
        drop(track);
        mute.set_muted(true);
        assert!(retired.upgrade().is_none());
    }

    #[tokio::test]
    async fn failed_publication_drops_its_track() {
        let mute = LocalPublicationMute::new(false, apply);
        let (track_tx, track_rx) = oneshot::channel();
        let result = mute
            .publish(
                Track(Arc::new(AtomicBool::new(false))),
                |track| async move {
                    track_tx.send(Arc::downgrade(&track)).ok();
                    Err::<(), ()>(())
                },
                |(), _| {},
            )
            .await;
        assert!(result.is_err());
        assert!(track_rx.await.unwrap().upgrade().is_none());
        mute.set_muted(true);
    }

    #[tokio::test]
    async fn completion_reasserts_the_wire_mute_after_listeners_attach() {
        let mute = Arc::new(LocalPublicationMute::new(true, apply));
        let actual = Arc::new(AtomicBool::new(false));
        let wire = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let publishing = {
            let mute = Arc::clone(&mute);
            let actual = Arc::clone(&actual);
            let wire = Arc::clone(&wire);
            tokio::spawn(async move {
                mute.publish(
                    Track(actual),
                    |track| async move {
                        // LiveKit builds AddTrackRequest before awaiting the SFU;
                        // mute listeners are attached only after the response.
                        wire.store(track.0.load(Ordering::SeqCst), Ordering::SeqCst);
                        started_tx.send(()).unwrap();
                        finish_rx.await.unwrap();
                        Ok::<_, ()>(wire)
                    },
                    |wire, muted| wire.store(muted, Ordering::SeqCst),
                )
                .await
            })
        };
        started_rx.await.unwrap();
        mute.set_muted(false);
        assert!(!actual.load(Ordering::SeqCst));
        assert!(wire.load(Ordering::SeqCst));
        finish_tx.send(()).unwrap();
        let _track = publishing.await.unwrap().unwrap();
        assert!(!wire.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn completion_restores_native_mute_after_the_sdk_enables_the_track() {
        let mute = LocalPublicationMute::new(true, apply);
        let actual = Arc::new(AtomicBool::new(false));
        let track = mute
            .publish(
                Track(Arc::clone(&actual)),
                |track| async move {
                    assert!(track.0.load(Ordering::SeqCst));
                    // publish_track enables the native track just before returning.
                    track.0.store(false, Ordering::SeqCst);
                    Ok::<(), ()>(())
                },
                |(), _| {},
            )
            .await
            .unwrap();
        assert!(track.0.load(Ordering::SeqCst));
    }
}
