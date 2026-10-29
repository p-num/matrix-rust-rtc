# LiveKit 0.7.48 local lifecycle patch

Source: the pristine crates.io `livekit-0.7.48.crate`, SHA-256
`3db88772d10f8ef1ab43a0aa0634a1d4807d0567b65665274af3e084589dbec6`.
Upstream source revision: `livekit/rust-sdks@af369aae0fe074762ef0a2051529e7ec63045042`,
crate directory `livekit`. Original Apache-2.0 notices are retained;
`LICENSE-APACHE` contains the standard Apache-2.0 text omitted from the archive.

The root Cargo patch selects this copy for mobile artifact builds. The crate is
excluded from the MatrixRTC workspace so upstream optional features and test
suites do not become workspace-wide feature requirements.

The ten local-SFU cases use actual engine, session or signal constructors and
unique rooms. Five reconnect cases cover acquisition, candidate cancellation,
promotion rejection, adoption and cancellation during a real Resume response.
Two cases exercise the ordinary Join and Resume response timeouts. Two initial
ICE cases cover cancellation after session construction and the real 15-second
PC timeout, each followed by a normal retry. The final case uses a real invalid
ICE configuration to reach the CXX peer-constructor error after Join.

One-shot barriers observe ownership boundaries. Response relays add a WebSocket
control Ping/Pong to prove signal tasks exist before cancellation or timeout;
application bytes are unchanged. The ICE relay forwards Join and keepalives but
withholds actual Offer/Answer/Trickle frames. The initial ICE timeout also passed
on the baseline and is retained as preservation coverage; its pass is not a
RED-to-GREEN claim. Initial ICE cancellation and the post-Join CXX error both
demonstrated leaks before their ownership guards were applied.
Set `LIVEKIT_URL`, `LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET` privately, then run:

```sh
cargo test --manifest-path vendor/livekit/Cargo.toml --locked --jobs 2 \
  --features __lk-e2e-test --lib \
  rtc_engine::reconnect_close_tests:: -- --nocapture --test-threads=1
cargo test --manifest-path vendor/livekit/Cargo.toml --locked --jobs 2 \
  --features __lk-e2e-test --lib \
  rtc_engine::initial_join_timeout_tests:: -- --nocapture --test-threads=1
```

The complete feature-enabled library suite is expected to contain 78 tests:
68 existing cases and these ten local-SFU cases. Run it with the same options
and no name filter. Resource limits and serialization are enforced by the
workspace runner; credentials remain in its child environment.

There is no skip when credentials or a running local SFU are missing. Oracles
are captured before fixture cleanup: SFU absence where the participant must
leave, and client-originated Close plus transport termination for held signal
responses. The ordinary Resume timeout preserves its existing participant and
proves a subsequent unheld resume keeps the same SID. Cleanup closes only each
probe's owned connection and unique room.

Local deviations from the archive are limited to this note and license text,
the lifecycle ownership fixes in `src/rtc_engine/mod.rs` and
`src/rtc_engine/rtc_session.rs`,
their focused regression modules, and the exact sibling API path pin in
`Cargo.toml` (with the matching path-source lock entry). `Cargo.toml.orig`
remains pristine. The sibling
`../livekit-api/PATCHES.md` documents the signal receiver ownership fix used by
both production builds and these standalone tests.

Closing prevents reconnect promotion and schedules disposal
of an unadopted session; this is eventual cleanup, not a synchronous drain of
an acquisition still awaiting the SFU. Full reconnect and resume acquisitions
keep an owner until they complete, so receiver cancellation cannot abandon a
partially constructed transport.

Initial construction now uses the same session candidate before the first PC
wait and adopts it only into the engine handle. Earlier in construction, a
separate signal-client candidate owns a successful API Join until `SessionInner`
exists, covering peer/configuration/data-channel errors and the single-PC
fallback await. Both unadopted guards schedule existing close operations; they
do not promise synchronous disposal. The API receiver watcher covers abandoned
response waits before a successful signal client exists, completing that
ownership boundary without changing the public API.
