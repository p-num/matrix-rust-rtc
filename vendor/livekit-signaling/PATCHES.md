# Letro terminal signaling changes in LiveKit Signaling 0.1.7

The base is `livekit-signaling-0.1.7.crate` from crates.io with SHA-256
`52fb8712633820ca0d186fadd7ca76c373d1cb3b555d143d7fee232b2493794b`.
Its Rust SDK revision is `fa46e8427050b6ee0c6e40670ff161ee3a2bbf5a`.
The archive's original notices and the Apache-2.0 license are included.

A terminal disconnect interrupts signaling sends and a pending Resume, then
closes the transport under the same ownership boundary as a successful Resume.
The stream stops both I/O tasks when its response receiver disappears. Explicit
close waits for both tasks and gives a writable socket a bounded chance to
send the protocol Leave. Temporary transport loss still allows reconnect.

The regression cases cover a blocked writer, a reconnect holding the state
lock and abandonment of a response receiver. The engine and room integration
cases live in the sibling LiveKit vendor.
