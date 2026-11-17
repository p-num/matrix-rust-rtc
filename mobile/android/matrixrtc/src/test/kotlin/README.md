# Android Integration Tests

`MembershipIdentityBindingTest` checks the generated membership DTO and buffer
codec for every identity kind, including zero and maximum unsigned timestamps.
Run `./scripts/build-android-aar.sh` from the repository root first to generate
the Kotlin bindings, then run `./gradlew :matrixrtc:testReleaseUnitTest
--max-workers=2` from `mobile/android`.

## Test Structure

Binding tests compile against generated Kotlin under `src/main/java`:

```kotlin
import org.junit.Test
import org.matrix.rtc.*

class MatrixRtcFfiTest {
    @Test
    fun testSessionCreation() {
        val session = RtcSessionHandle()
        // Test session operations
    }

    @Test
    fun testMembershipSnapshot() {
        val session = RtcSessionHandle()
        val subscription = session.subscribeMembershipSnapshots()
        // Test subscription behavior
    }
}
```

## Running Tests

```bash
cd mobile/android
./gradlew test
```

