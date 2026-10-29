# LiveKit API 0.5.3 local signal ownership patch

Source: the pristine crates.io `livekit-api-0.5.3.crate`, SHA-256
`c4d063f66ee1faf1c654a2bd59eba2e3f3eefc63772d957f2702c0724903db8a`.
Upstream source revision: `livekit/rust-sdks@af369aae0fe074762ef0a2051529e7ec63045042`,
crate directory `livekit-api`. All 23 archive files are retained. Original
Apache-2.0 notices remain; `LICENSE-APACHE` supplies the standard license text
omitted from the archive.

Production changes are confined to `src/signal_client/mod.rs` and
`src/signal_client/signal_stream.rs`. Protocol DTOs remain inside this API
boundary; there is no new public API or wire-format change.

## Terminal intent and stream ownership

`SignalInner` keeps a shared terminal-Leave watch distinct from a temporary
transport close. A terminal Disconnect cannot be downgraded by a transport
shutdown. It prevents subsequent restart/adoption, interrupts waits behind a
reconnect's stream lock and pending ordinary sends, and disposes an already
created candidate stream. Temporary close still permits normal recovery. The
candidate installation and terminal store share the watch's synchronous read
boundary; no watch guard is retained across an await.

Terminal intent interrupts read-owned sends and constructor waits, but explicit
`SignalInner.close` retains its queued `stream.write()` acquisition. It takes
and joins any stream still installed after those readers release it. Cancelling
that queued writer merely because terminal intent is set can return from close
without disposing the installed stream; terminal Leave itself remains able to
establish intent without waiting behind the writer.

`SignalStream` uses a separate shutdown watch to wake both I/O tasks even when
an ordinary writer or the bounded Pong queue is backpressured. The reader
observes loss of its private event receiver both in its main read select and
while forwarding a received Ping into that queue. An abandoned Join/Resume
response receiver therefore requests transport shutdown before the stream is
installed. Successful constructors transfer the same receiver to the middleware
without triggering this path.

The writer cancels pending ordinary writes and releases their acknowledgements.
For terminal shutdown it attempts the final protocol Leave on a writable stream;
reader completion lets an unwritable socket stop local teardown without waiting
for a remote Close handshake. This is best-effort protocol delivery, not a server
acknowledgement. Explicit `SignalStream.close` requests shutdown and joins both
I/O tasks. Neither this mechanism nor the sibling Room controls promise delivery
through a partition or removal of every old SFU participant within five seconds.

The engine-owned full-reconnect/resume acquisition retains its whole attempt
until terminal candidate disposal closes and joins the exact acquired stream.
Arbitrary external cancellation of the public `SignalClient.restart` future is
a narrower contract: dropping its event receiver requests cooperative I/O
shutdown, but cancellation itself does not synchronously join dropped task
handles or prove remote Leave delivery. No asynchronous Drop/reaper or new
recovery session is introduced to manufacture Leave on an already closed stream.

Both the root Cargo patch and the sibling LiveKit crate's exact path dependency
select this source. The root and standalone LiveKit lockfiles preserve every
API dependency version while replacing only this package's registry provenance
with the path source. The API crate's own upstream manifest and lockfile remain
unchanged.

## Verification and boundary

The current standalone API suite passes all **35 tests** in 4.01 seconds with
`signal-client-tokio`, zero ignored or filtered (5.13-second command). Independent
source/executable-lineage acceptance is recorded in
`terminal-causal-oracles-api35-vendor80-independent-proof-20261029.json`.

The public-client queued-close regression uses real middleware/a held WebSocket,
actual bytes from a blocked 16 MiB Offer, its read guard and an observed queued
close writer. Original `9bf…` fails the unchanged two-second join oracle in
2.14 seconds, with cleanup confirmed separately; retaining the write acquisition
passes in 0.13 seconds. Both sources are explicitly compiled. The immediately
following API34 pass (4.00 seconds) and its identical-source executable chain
remain recorded in
`queued-terminal-close-red-green-api34-independent-proof-20261029.json`.

The added pre-101 control reaches an actual second Resume HTTP request but holds
its upgrade response before sending any 101. The public terminal-Leave request
establishes intent and restart/close joins complete within two seconds, with positive client EOF/reset **before the
first fixture release**, no installed stream/middleware handle, and a rejected
fresh restart. A listener checkpoint observes no third connection. This is a
separate construction boundary from a held post-upgrade response.

The earlier API30/Pong-receiver-loss mutation proof remains valid: removing only
the inner Pong-forward `emitter.closed()` branch fails while the peer stays
held. Clean-compiled API33 and earlier production check/Clippy passes used `9bf…`
and remain historical evidence. The cached-mutant API33 attempt has invalid
executable provenance, not an accepted product regression. Source hashes alone
do not identify the executed binary.

```sh
cargo test --manifest-path vendor/livekit-api/Cargo.toml --locked --jobs 2 \
  --features signal-client-tokio --lib -- --nocapture --test-threads=1
```

The sibling [LiveKit patch note](../livekit/PATCHES.md) records current-source
**80/80 tests**, including all **12 local-SFU cases**, in 68.07 seconds
(76.14-second command). Those cases require explicit private loopback SFU
credentials and a running server, with no prerequisite skip. The test-only
oracle correction preserves the four production-file hashes: recovery-owner
and gate rejection distinguish client ownership from later observation of
buffered HTTP bytes. All accepted candidates, including header-pending ones,
require positive EOF/reset before the **first** fixture release and at the final
recheck. This does not promise that buffered network bytes can be revoked or
measure literal OS connection starts. Already-closed old signaling permits
45-second natural SFU grace; the still-writable old path requires actual Leave
and SFU absence within five seconds before release.

Current-source production check (5.15-second command), Clippy (7.16 seconds),
all **527 workspace tests** (18.32 seconds) and configured workspace formatting
(1.08 seconds) pass in the `terminal-core-accepted-*` runs. Each reports source
unchanged, its owned process group drained and no resource stop. The prior
workspace attempt stopped at the 15 GB reserve during compilation before tests;
it remains neither a functional failure nor a test pass. The earlier full-vendor
format attempt's legacy differences remain retained; workspace formatting does
not claim that excluded upstream vendor files are uniformly reformatted.

This accepts the current local host source, not a rebuilt mobile SDK or consumer.
New SDK, native/app tests and actual iOS shutdown/fault acceptance are **NOT RUN**;
physical media, system delivery and release acceptance remain separate.
