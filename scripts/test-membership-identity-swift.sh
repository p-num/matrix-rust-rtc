#!/bin/bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Run the generated Swift membership codec against the matching host library.
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
TEST_DIR="$PROJECT_ROOT/target/membership-identity-swift"
mkdir -p "$TEST_DIR"
cargo build -p matrix-rtc-ffi --jobs 2
cargo run -p uniffi-bindgen --jobs 2 -- generate \
    --library "$PROJECT_ROOT/target/debug/libmatrix_rtc_ffi.dylib" \
    --language swift --out-dir "$TEST_DIR"
swiftc -j 2 \
    -Xcc "-fmodule-map-file=$TEST_DIR/MatrixRtcFFI.modulemap" \
    -I "$TEST_DIR" -L "$PROJECT_ROOT/target/debug" -lmatrix_rtc_ffi \
    "$TEST_DIR/MatrixRtc.swift" mobile/ios/tests/membership_identity.swift \
    -o "$TEST_DIR/membership-identity-test"
DYLD_LIBRARY_PATH="$PROJECT_ROOT/target/debug${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
    "$TEST_DIR/membership-identity-test"
