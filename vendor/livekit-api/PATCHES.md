# LiveKit API 0.5.3 local signal ownership patch

Source: the pristine crates.io `livekit-api-0.5.3.crate`, SHA-256
`c4d063f66ee1faf1c654a2bd59eba2e3f3eefc63772d957f2702c0724903db8a`.
Upstream source revision: `livekit/rust-sdks@af369aae0fe074762ef0a2051529e7ec63045042`,
crate directory `livekit-api`. All 23 archive files are retained. Original
Apache-2.0 notices remain; `LICENSE-APACHE` supplies the standard license text
omitted from the archive.

The only production change is in `src/signal_client/signal_stream.rs`: its
reader observes closure of the private signal-event receiver and takes the
existing internal Close/writer-shutdown path. A Join or Resume constructor can
drop that receiver on timeout, another error, or cancellation before the stream
is installed. Its detached reader and writer otherwise keep the socket alive.
Successful constructors move the same receiver into the signal middleware, so
that ownership transfer does not trigger closure. This adds no public API or
wire-format change, and does not promise synchronous disposal of pending I/O.

Both the root Cargo patch and the sibling LiveKit crate's exact path dependency
select this source. The root and standalone LiveKit lockfiles preserve every
API dependency version while replacing only this package's registry provenance
with the path source. The API crate's own upstream manifest and lockfile remain
unchanged.

The real-SFU initial-Join timeout regression lives in the sibling LiveKit crate:

```sh
cargo test --manifest-path vendor/livekit/Cargo.toml --locked --jobs 2 \
  --features __lk-e2e-test --lib \
  rtc_engine::initial_join_timeout_tests:: -- --nocapture --test-threads=1
```

It requires explicit private loopback SFU credentials and a running server.
It proves actual signal tasks and unique admission before the real five-second
Join timeout, then observes a client-originated protocol Close and transport
termination plus SFU absence before closing any fixture resource. It has no
skip when its prerequisites are missing. Reconnect ownership remains covered by
the sibling crate's five `rtc_engine::reconnect_close_tests` cases.
