# Foreign membership factory roundtrip

Run `./scripts/test-membership-factory-kotlin.sh` on macOS. The script builds a
signalling-only native library, generates matching JVM bindings, then executes
Kotlin implementations of the factory and its returned backend views against
the real native call stack. This does not package or stage an Android AAR.

The fixture checks both membership formats, one capture across join and acknowledged leave,
same-room rejoin, missing/unknown/retired scopes, the original constructor, an unexpected foreign factory exception, and the Android binding initializer’s
JNA attachment policy for the new factory callbacks. A thrown exception must fail the join
without writing through the root or current backend; another request on the
same client must still work. Routine denial uses a backend view that returns
`FfiBackendError`, rather than throwing from the factory. Timer-driven retries and refreshes are
covered in the Rust suite on its paused executor; this bridge fixture drives
direct operations and observes actual callback acknowledgments.
