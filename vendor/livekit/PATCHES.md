# Letro lifecycle changes in LiveKit 0.9.7

The base is the `livekit-0.9.7.crate` archive from crates.io with SHA-256
`597d8bae50d9de78c82d93b367b17bf08d62b0cd53727d493d7e56e8d3e58665`.
Its Rust SDK revision is `ead3a6936e7e526c6853255e2a731ba7648dd63d`.
The archive's original notices and the Apache-2.0 license are included.

This keeps the shutdown ownership fixes from the previous LiveKit vendor on
the transport and reconnect state machines shipped with MatrixRTC 0.5.0-rc.9.

- A signal client or RTC session stays owned through fallible construction and
  closes if the engine never adopts it.
- Closing the engine seals reconnect admission and promotion. A full reconnect
  retains ownership of an in-flight candidate until it can dispose of it.
- Session shutdown closes peer connections and requests terminal signaling
  shutdown before joining workers that may be waiting on signaling.
- Room shutdown seals recovery events and finalizes the disconnected state,
  even when the room task stops before consuming the engine's event.

The signaling patch now lives in [livekit-signaling](../livekit-signaling/PATCHES.md).
The root workspace and this crate's standalone manifest select the same copy.

Run the retained lifecycle cases on a local SFU with private `LIVEKIT_URL`,
`LIVEKIT_API_KEY` and `LIVEKIT_API_SECRET`, using one test thread and two workers.
