// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

import Foundation

/// Executable contract for the generated Swift DTO and UniFFI buffer codec.
@main
struct MembershipIdentityBindingTest {
    static func main() throws {
        let cases: [(FfiMembershipIdentityKind, UInt64?, String?)] = [
            (.perJoinMemberId, nil, nil),
            (.legacyJoinId, 1, "0123456789abcdef0123456789abcdef"),
            (.legacyMembershipTimestamp, 0, nil),
            (.legacyMembershipTimestamp, UInt64.max, nil),
            (.unavailable, nil, nil),
        ]
        for (kind, timestamp, legacyJoinId) in cases {
            let original = JoinedMembership(
                roomId: "!room:example.org",
                slotId: "m.call#room",
                sender: "@alice:example.org",
                senderDeviceId: "DEVICE",
                stickyKey: "member",
                memberId: "member",
                membershipEventId: "$refresh",
                membershipTs: timestamp,
                legacyJoinId: legacyJoinId,
                identityKind: kind,
                application: "m.call",
                transports: [],
                canSubscribe: []
            )
            let buffer = FfiConverterTypeJoinedMembership_lower(original)
            let decoded = try FfiConverterTypeJoinedMembership_lift(buffer)
            precondition(decoded == original)
        }
        print("Swift membership identity binding contract passed (\(cases.count) cases)")
    }
}
