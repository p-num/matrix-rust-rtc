// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Membership identity contracts at the roster and UniFFI buffer boundaries.
//! These DTO tests keep core memberships independent of native binding types.

use super::*;
use matrix_rtc_core::compat::{MembershipFormat, ingest};

fn member(
    member_id: &str,
    event_id: &str,
    membership_ts: Option<u64>,
    origin_server_ts: u64,
) -> CoreJoinedMembership {
    let mut content = serde_json::json!({
        "slot_id": "m.call#room",
        "msc4354_sticky_key": member_id,
        "member": { "id": member_id, "membership": "join" },
        "application": { "type": "m.call" },
    });
    if let Some(ts) = membership_ts {
        content["created_ts"] = ts.into();
    }
    let raw = ingest::to_core_member_event(
        MembershipFormat::Msc,
        "!room:example.org",
        ingest::RawMemberEventIn {
            event_id: Some(event_id.to_owned()),
            sender: "@alice:example.org".to_owned(),
            sender_device_id: Some("DEVICE".to_owned()),
            was_encrypted: true,
            origin_server_ts: Some(origin_server_ts),
            event_type: "m.rtc.member".to_owned(),
            content,
        },
    )
    .expect("parse membership");
    let matrix_rtc_core::RtcMembershipEvent::Joined(member) =
        raw.try_into_membership_event().expect("joined membership")
    else {
        panic!("expected a join");
    };
    member
}

async fn snapshot_roundtrip(
    member: CoreJoinedMembership,
    format: MembershipFormat,
) -> JoinedMembership {
    let (_sender, receiver) = watch::channel(vec![member]);
    let subscription = MembershipSnapshotSubscription::new(receiver, format);
    let mut snapshot = subscription.next().await.expect("initial roster");
    assert_eq!(snapshot.len(), 1);
    let original = snapshot.remove(0);
    let buffer = <JoinedMembership as uniffi::Lower<UniFfiTag>>::lower(original.clone());
    let lifted = <JoinedMembership as uniffi::Lift<UniFfiTag>>::try_lift(buffer)
        .expect("UniFFI record must preserve identity metadata");
    assert_eq!(original, lifted);
    lifted
}

#[tokio::test]
async fn msc_identity_survives_refresh_and_changes_on_rejoin() {
    let first =
        snapshot_roundtrip(member("join-a", "$first", None, 10), MembershipFormat::Msc).await;
    let refresh = snapshot_roundtrip(
        member("join-a", "$refresh", None, 20),
        MembershipFormat::Msc,
    )
    .await;
    let rejoin =
        snapshot_roundtrip(member("join-b", "$rejoin", None, 30), MembershipFormat::Msc).await;
    for value in [&first, &refresh, &rejoin] {
        assert_eq!(
            value.identity_kind,
            FfiMembershipIdentityKind::PerJoinMemberId
        );
        assert_eq!(value.membership_ts, None);
    }
    assert_eq!(first.member_id, refresh.member_id);
    assert_ne!(first.membership_event_id, refresh.membership_event_id);
    assert_ne!(first.member_id, rejoin.member_id);
}

#[tokio::test]
async fn legacy_identity_preserves_join_timestamp_across_refresh_and_rejoin() {
    let id = "@alice:example.org:DEVICE";
    let first = snapshot_roundtrip(
        member(id, "$first", Some(100), 110),
        MembershipFormat::RoomState,
    )
    .await;
    let refresh = snapshot_roundtrip(
        member(id, "$refresh", Some(100), 120),
        MembershipFormat::RoomState,
    )
    .await;
    let rejoin = snapshot_roundtrip(
        member(id, "$rejoin", Some(u64::MAX), 130),
        MembershipFormat::RoomState,
    )
    .await;
    for value in [&first, &refresh, &rejoin] {
        assert_eq!(
            value.identity_kind,
            FfiMembershipIdentityKind::LegacyMembershipTimestamp
        );
        assert_eq!(value.member_id, id);
    }
    assert_eq!(first.membership_ts, Some(100));
    assert_eq!(first.membership_ts, refresh.membership_ts);
    assert_ne!(first.membership_event_id, refresh.membership_event_id);
    assert_eq!(rejoin.membership_ts, Some(u64::MAX));
}

#[tokio::test]
async fn legacy_without_timestamp_has_no_identity_even_with_event_metadata() {
    // Neither a random-looking ID nor a user/device-shaped one proves freshness.
    for id in ["random-looking-member-id", "@alice:example.org:DEVICE"] {
        let first =
            snapshot_roundtrip(member(id, "$first", None, 10), MembershipFormat::RoomState).await;
        let refresh = snapshot_roundtrip(
            member(id, "$refresh", None, 20),
            MembershipFormat::RoomState,
        )
        .await;
        for value in [&first, &refresh] {
            assert_eq!(value.identity_kind, FfiMembershipIdentityKind::Unavailable);
            assert_eq!(value.membership_ts, None);
        }
        assert_ne!(first.membership_event_id, refresh.membership_event_id);
    }
}
