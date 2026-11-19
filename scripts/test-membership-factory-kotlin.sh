#!/bin/bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FIXTURE_ROOT="$PROJECT_ROOT/tests/kotlin-membership-factory"

if [ "$(uname -s)" != "Darwin" ]; then
    echo 'This roundtrip uses the matching macOS dynamic library.' >&2
    exit 1
fi

export CARGO_BUILD_JOBS=2
export TOKIO_WORKER_THREADS=2
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
cd "$PROJECT_ROOT"
cargo build --locked -p matrix-rtc-ffi --no-default-features
cargo run --locked -p uniffi-bindgen -- generate \
    --library "$PROJECT_ROOT/target/debug/libmatrix_rtc_ffi.dylib" \
    --language kotlin --config "$FIXTURE_ROOT/uniffi.toml" \
    --out-dir "$FIXTURE_ROOT/build/generated" --no-format

"$PROJECT_ROOT/mobile/android/gradlew" --project-dir "$FIXTURE_ROOT" \
    foreignFactoryRoundtrip \
    -PnativeLibrary="$PROJECT_ROOT/target/debug/libmatrix_rtc_ffi.dylib" \
    --max-workers=2 --no-daemon --no-configuration-cache --no-parallel --console=plain "$@"
