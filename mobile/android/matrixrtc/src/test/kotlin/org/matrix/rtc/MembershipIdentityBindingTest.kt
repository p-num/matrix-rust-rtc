// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

package org.matrix.rtc

import java.nio.ByteBuffer
import org.junit.Assert.assertEquals
import org.junit.Test

/** Checks the generated Kotlin DTO and codec, including unsigned timestamps. */
class MembershipIdentityBindingTest {
    @Test
    fun membershipIdentityMetadataSurvivesTheGeneratedCodec() {
        val cases = listOf(
            FfiMembershipIdentityKind.PER_JOIN_MEMBER_ID to null,
            FfiMembershipIdentityKind.LEGACY_MEMBERSHIP_TIMESTAMP to 0uL,
            FfiMembershipIdentityKind.LEGACY_MEMBERSHIP_TIMESTAMP to ULong.MAX_VALUE,
            FfiMembershipIdentityKind.UNAVAILABLE to null,
        )
        for ((kind, timestamp) in cases) {
            val original = JoinedMembership(
                roomId = "!room:example.org",
                slotId = "m.call#room",
                sender = "@alice:example.org",
                senderDeviceId = "DEVICE",
                stickyKey = "member",
                memberId = "member",
                membershipEventId = "\$refresh",
                membershipTs = timestamp,
                identityKind = kind,
                application = "m.call",
                transports = emptyList(),
                canSubscribe = emptyList(),
            )
            val buffer = ByteBuffer.allocate(
                FfiConverterTypeJoinedMembership.allocationSize(original).toInt(),
            )
            FfiConverterTypeJoinedMembership.write(original, buffer)
            buffer.flip()
            val decoded = FfiConverterTypeJoinedMembership.read(buffer)
            assertEquals(original, decoded)
            assertEquals(0, buffer.remaining())
        }
    }
}
