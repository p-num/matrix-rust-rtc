# LiveKit 0.7.48 local lifecycle patch

Source: the pristine crates.io `livekit-0.7.48.crate`, SHA-256
`3db88772d10f8ef1ab43a0aa0634a1d4807d0567b65665274af3e084589dbec6`.
Upstream source revision: `livekit/rust-sdks@af369aae0fe074762ef0a2051529e7ec63045042`,
crate directory `livekit`. Original Apache-2.0 notices are retained;
`LICENSE-APACHE` contains the standard Apache-2.0 text omitted from the archive.

The root Cargo patch selects this copy for mobile artifact builds. The crate is
excluded from the MatrixRTC workspace so upstream optional features and test
suites do not become workspace-wide feature requirements.

## Production ownership and terminal close

Local production deviations from the archive are in
`src/rtc_engine/mod.rs`, `src/rtc_engine/rtc_session.rs` and `src/room/mod.rs`.
The sibling [API patch note](../livekit-api/PATCHES.md) describes its two changed
signaling files. The current shutdown correction adds no public API, generated
binding or Matrix wire-format change.

The engine close barrier prevents reconnect promotion after terminal close and
retains ownership of full-reconnect/resume acquisitions until their acquired
candidates are disposed. A candidate not adopted by a live engine schedules its
existing close operation; this is eventual cleanup, not a synchronous drain of
an acquisition still awaiting the SFU. Initial construction also retains the
session candidate across the first PC wait, and a signal-client candidate owns a
successful API Join until `SessionInner` exists. That covers fallible CXX PC,
configuration/data-channel construction and the single-PC fallback await.

`RtcSession.close` now notifies its workers, marks the session terminal, closes
both PCs and requests terminal signaling shutdown before joining workers that
may themselves await reconnect signaling. It then joins those workers. The API
terminal watch interrupts the old signal path instead of requiring an
unresponsive old server response to release the local teardown.

`RoomSession` has a private closing seal serialized with state changes under
`info.write()`. Close seals before unpublishing and finalizes Disconnected after
worker joins, so a queued engine Disconnected losing the room-task stop select
cannot leave stale Connected/Reconnecting state. Server-terminal Disconnected
also seals the Room before its asynchronous cleanup begins. All four late
recovery handlers retain their acknowledgements; state changes and associated
Reconnecting/Reconnected events share the seal. Late restart work skips a
closing room and publications whose track has already been retired; final
recovery dispatch uses the same sealed helper. This does not promise a
synchronous drain of every previously spawned publication job.

The API receiver watcher covers abandoned response waits before a successful
signal client exists. An engine-owned resume explicitly closes/joins its exact
candidate stream. Arbitrary external cancellation of
`SignalClient.restart` only requests cooperative I/O shutdown through receiver
loss; it does not synchronously join dropped handles or acknowledge remote
Leave. Temporary transport recovery stays distinct from terminal Disconnect.

The later queued-writer correction in API `SignalInner.close` retains its write
acquisition until it can take/join an installed stream. Terminal intent may
interrupt ordinary read-owned sends and constructors, but must not cancel close
itself while a blocked Offer still owns the stream. The public-client regression
fails the original select in 2.14 seconds and passes the same oracle after this
minimal change; no Room or session production change accompanies that follow-up.

## Retained vendor coverage and current API gate

The current feature-enabled library suite passes **80 tests**: 68 existing cases
and **12 local-SFU cases**, zero ignored/filtered, in 68.07 seconds
(76.14-second command). The sibling signaling API separately passes all **35
tests** in 4.01 seconds (5.13-second command). Both current-source runs and the
new focused pre-101 API control are independently accepted in
`terminal-causal-oracles-api35-vendor80-independent-proof-20261029.json`, with
all 211 input hashes matched and actual executable lineage verified. Their
latest oracle refinement is test-only; the four production correction hashes
remain unchanged, including API `mod.rs` `ed82f625…`.

| Group | Cases | Boundary |
|---|---:|---|
| `reconnect_close_tests` | 5 | Acquisition/cancellation, promotion rejection, adoption and close during a held real Resume response |
| `initial_join_timeout_tests` | 5 | Ordinary Join/Resume response timeouts, initial ICE cancellation/15-second timeout with retry, and real post-Join CXX PC-constructor error |
| `room_close_tests` | 2 | Publishing Room close with an already closed old signal socket or a writable old socket whose application response is held |

The API queued-close RED/GREEN uses real blocked-Offer bytes and a queued public
close writer: original `9bf…` fails in 2.14 seconds with cleanup confirmed;
`ed82…` passes the same focused oracle in 0.13 seconds. The following API34 pass
remains valid preceding evidence. The added API35 pre-101 control observes the
actual second Resume HTTP request while **no upgrade 101 is sent**. The public
terminal-Leave request establishes intent and restart/close joins complete within
two seconds, positive EOF/reset
precedes first release, installed stream/middleware handles are absent, a fresh
restart returns SendError and no third connection is observed at the listener
checkpoint. Host results do not accept a rebuilt mobile SDK or consumer.

One-shot barriers observe ownership boundaries. Response relays use actual
WebSocket Ping/Pong to prove I/O tasks exist; application bytes stay unchanged.
The ICE relay forwards Join/keepalives while withholding Offer/Answer/Trickle.
The 15-second initial ICE timeout also passes on the baseline and is preservation
coverage, not a RED-to-GREEN claim. Initial ICE cancellation and the real CXX
constructor error have failing pre-guard controls.

Set `LIVEKIT_URL`, `LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET` privately, then run
the complete suite without a name filter:

```sh
cargo test --manifest-path vendor/livekit/Cargo.toml --locked --jobs 2 \
  --features __lk-e2e-test --lib -- --nocapture --test-threads=1
```

There is no skip when credentials or a running SFU are missing. Resource limits
and serialization are enforced by the workspace runner; credentials remain in
its child environment. Cleanup touches only each probe's owned connection and
unique room, after its resource oracles.

## Strict fixture oracles and limits

The held-Resume relay retains a persistent client-frame parser while the real
application response is held. Production close, all owned attempts, actual SFU
absence and positive client closure are checked **before** response release.
A masked Close plus actual client TCP EOF, or a post-Close connection reset,
provides the positive closure witness. Arbitrary read/write errors do not count
as clean closure. The relay does not drop endpoints merely on a Close frame to
supply an EOF that could manufacture production cleanup.

Both public publishing-Room controls require close within two seconds, local
publication/track retirement, final Disconnected state and exactly one terminal
event/state transition while old server bytes remain held. They subscribe again
after the dispatcher is cleared and exercise all four real late recovery
handlers, requiring acknowledgements and rejecting new events/state or a
republish job after completed close. The engine probe drains owned attempts and
the outer reconnect lock, confirms closed/cannot-reconnect/no engine task/stable
session, then rejects both real fresh recovery-mode triggers.

Both controls account for listener backlog and require positive EOF/reset for
**every accepted candidate**, including one whose HTTP header is still pending,
before the **first fixture release** and at the final recheck. This is an
ownership/recovery-gate oracle, not a literal OS connection-start counter.

- **Already closed old socket:** an in-flight request may reach header parsing
  or SFU admission after local close; every accepted candidate must still close
  positively before first release. The old participant may expire under explicit
  **45-second natural LiveKit server grace**. No old-stream protocol Leave or
  universal five-second SFU-removal guarantee is claimed.
- **Open, unresponsive old socket:** the actual terminal Leave must be parsed
  from the client stream and the exact SFU participant must disappear within
  **five seconds before first response release**. Later administrative cleanup
  cannot satisfy this oracle.

Both controls require ten stable empty SFU samples and no Reconnected event.
The current old-close trace accepts its candidate before Room.close enters;
that candidate's header and 101 admission arrive after close returns, but
positive EOF and the all-candidate checkpoint precede first release. A later
proxy header is not a client attempt-start timestamp, and buffered network
bytes cannot be revoked. The traced 79-pass/1-fail counter result exposed this
observation error; earlier **untraced** 79/1 remains causally unclassified.
Preserve those raw failures, the earlier 77/3 result and the earlier `9bf…`
80-test pass (68.10 seconds); do not assign an older run a cause from a later
trace. The current test-only correction replaces header-count equality with
explicit owner/gate rejection and stronger positive all-candidate closure.

The genuine `51d` publishing-Room baseline RED reaches the original two-second
close deadline and fails in 2.30 seconds. An owned nonzero PCM feeder and exact
publication polling fix an earlier invalid precondition attempt; they do not
change that baseline's close/no-resume/five-second-absence assertions. The
corrected already-closed-old control's 45-second grace is an explicitly
different server-removal boundary. It does not accept the separate iOS
five-second-post-restore zero requirement without a rebuilt live rerun.

Local deviations also include the focused regression modules/relays and their
test-only ownership/timestamp probes, this note and license text, plus the exact
sibling API path pin in `Cargo.toml` and matching path-source lock entry.
`Cargo.toml.orig` remains pristine. Original Apache-2.0 notices and archive-origin
information above remain in place.

Current-source production check passes in 5.15 seconds, Clippy in 7.16 seconds,
all **527 workspace tests** in an 18.32-second command, and configured workspace
formatting in 1.08 seconds (`terminal-core-accepted-*`). Each run reports source
unchanged, its owned process group drained and no resource stop. The previous
168.76-second check and 52.68-second Clippy passes used `9bf…`; the earlier
workspace compilation stopped at the 15 GB reserve before tests. Preserve the
old full-vendor format failure with its legacy differences; the accepted
`cargo fmt --all -- --check` covers the configured workspace, not a reformat of
all excluded upstream vendor code.

New rebuilt SDK/native/app consumers and actual iOS shutdown/recovery/default-
engine acceptance are **NOT RUN**. No physical camera/audio, CallKit/PushKit,
older-client or production-release acceptance follows from these host gates.
