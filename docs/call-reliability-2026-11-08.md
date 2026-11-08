# Native call reliability regressions

This release repairs two independently reproduced transport lifecycle failures.
The public call protocol and FFI interfaces are unchanged.

The package rehearsal also exposed an Android assembly defect: a broad cache
search selected Java classes from WebRTC `51ef663` while the native library
linked `a545de4`. The native binary references `livekit.org.jni_zero.JniZero`,
which that older JAR does not contain. The Android packager now follows the
active `webrtc-sys` package's Cargo linker records and requires matching Java
payloads for every requested ABI. Eleven local selection regressions cover stale
caches, missing or ambiguous inputs, wrong targets and incompatible ABI payloads.
Two further integration tests perform fresh and cached Android shared-library
links and an intentional compiler failure. They verify that Cargo records survive
the build boundary; `cargo-ndk` normally consumes them, so the packager captures
its inner Cargo stream. The complete NDK environment stays intact: its environment
export alone omits private linker flags, which a cached build or `cargo check`
cannot detect.

| Failure | Evidence | Repair |
| --- | --- | --- |
| Cancelled call joins after delayed authorization resumes | Core `.1` consumer APK `f2cadf0c2ad86c199b08270db982b75a261e140ad9e645184cfaed2bbb66d86d` clears its UI and membership, then creates an orphan SFU session. The cancellation repair keeps zero SFU participants for 90 seconds after authorization resumes; a separate replacement replay keeps only the new session, unchanged for 90 seconds. | Close abandoned connector results, drain completed connections during shutdown and make a closed prepared handle terminal. |
| Camera appears on but its server publication stays muted | The earlier, unpublished `.2` source `8670d526582a03cd4d77e8cd57c2b4fa8e7befb4`, in Android APK `9a3e4f415dceb76d84e4cd4d90b749b04359b4b747d439c6f3d02bda8a31496c`, requests camera unmute before the SFU publication completes. The camera stays muted at the SFU beyond the 90-second assertion and is still muted in an independent observation at 07:44:46 UTC on 8 November. The callee, which unmutes after publication, publishes normally. | Register pending native tracks for mute changes before awaiting publication. After the SDK attaches its listeners, restore native capture state and explicitly confirm the latest mute state through the publication API. |

The second repair also covers a microphone mute during pending publication,
several intervening changes, replacement tracks, failed publication and retired
track lifetime. `LocalPublicationMute` owns this handoff. It holds no mutex across
an await and keeps weak track registrations.

The listener step is necessary: the vendored LiveKit client snapshots mute into
`AddTrackRequest`, attaches track listeners after the response, and enables the
native track before returning. Updating only a local flag during that interval
does not guarantee that the SFU receives the new state.

## Local verification

The original handoff fails three new regressions. Registering pending tracks
alone passes those, but fails two further SDK-completion regressions. The complete
repair passes all nine, as well as the full workspace's **715 tests, zero failures
and zero ignored tests**. Compilation, formatting and all-target/all-feature
Clippy also pass with Rust 1.96.0 and two workers.

```sh
cargo test -p matrix-rtc-livekit local_publication::tests -- --test-threads=2
cargo check --all --jobs 2
cargo fmt --all -- --check
CARGO_BUILD_JOBS=2 make clippy
cargo test --all --jobs 2 -- --test-threads=2
python3 scripts/test_select_webrtc_jar.py
python3 scripts/test_android_cargo_metadata.py
```

The earlier `.2` binary was a local acceptance candidate, never a published
release. The release must contain both repairs. Actual consuming-app media,
package checksums, historical cards and device acceptance are recorded separately
in [Android PR155](https://github.com/p-num/element-x-android/pull/155), alongside
the original [cancellation report](https://github.com/p-num/element-x-android/issues/156).
These unit results do not establish physical audio routing, production push
delivery or app-store publication.
