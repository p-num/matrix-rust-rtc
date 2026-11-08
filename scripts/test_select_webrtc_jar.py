#!/usr/bin/env python3
# Copyright 2026 Element Creations Ltd.
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

"""Exercise Android packaging against Cargo linker records and stale caches."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile


SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_ID = "registry+https://github.com/rust-lang/crates.io-index#webrtc-sys@0.3.51"
ARM = "aarch64-linux-android"
X64 = "x86_64-linux-android"


class SelectWebrtcJarTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rtc jar test ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.target = self.root / "target"
        self.messages = self.root / "cargo.jsonl"
        self.events = []

    def bundle(self, name, content=b"current", order=("Track.class", "Factory.class")):
        bundle = self.target / "mobile-release/build/scratch/out/livekit_webrtc" / name
        (bundle / "lib").mkdir(parents=True)
        (bundle / "lib/libwebrtc.a").write_bytes(b"native archive")
        jar = bundle / "libwebrtc.jar"
        with zipfile.ZipFile(jar, "w") as archive:
            for entry in order:
                archive.writestr(entry, content + entry.encode())
        return jar

    def record(self, jar, target=ARM, package_id=PACKAGE_ID):
        output = self.target / target / "mobile-release/build/webrtc-sys-current/out"
        output.mkdir(parents=True, exist_ok=True)
        self.events.append({
            "reason": "build-script-executed",
            "package_id": package_id,
            "out_dir": str(output),
            "linked_paths": ["native=" + str(jar.parent / "lib")],
        })

    def select(self, *targets):
        self.messages.write_text("".join(json.dumps(event) + "\n" for event in self.events))
        selector = SCRIPT_DIR / "select-webrtc-jar.py"
        return subprocess.run([
            "python3", str(selector), "--messages", str(self.messages),
            "--package-id", PACKAGE_ID, "--target-dir", str(self.target),
            "--profile-dir", "mobile-release", "--targets", *(targets or (ARM,)),
        ], text=True, capture_output=True)

    def test_linked_bundle_wins_over_stale_cached_version(self):
        self.bundle("aaa-old-51ef663", b"old")
        current = self.bundle("zzz-current-a545de4")
        self.record(current)
        result = self.select()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(result.stdout.strip()), current)

    def test_unrelated_cache_cannot_replace_missing_linker_record(self):
        self.bundle("old-unrelated")
        self.assertNotEqual(self.select().returncode, 0)

    def test_missing_linked_jar_cannot_fall_back_to_cache(self):
        self.bundle("aaa-old")
        current = self.bundle("zzz-current")
        self.record(current)
        current.unlink()
        self.assertNotEqual(self.select().returncode, 0)

    def test_host_and_other_package_records_are_ignored(self):
        old = self.bundle("aaa-old")
        current = self.bundle("zzz-current")
        self.record(old, target="aarch64-apple-darwin")
        self.record(old, package_id=PACKAGE_ID.replace("0.3.51", "0.3.48"))
        self.record(current)
        result = self.select()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(result.stdout.strip()), current)

    def test_ambiguous_linked_bundles_fail(self):
        self.record(self.bundle("first"))
        self.record(self.bundle("second"))
        self.assertNotEqual(self.select().returncode, 0)

    def test_duplicate_cached_cargo_records_are_allowed(self):
        current = self.bundle("current")
        self.record(current)
        self.events.append(self.events[0])
        self.assertEqual(self.select().returncode, 0)

    def test_different_abi_java_payloads_fail(self):
        self.record(self.bundle("arm", b"arm"))
        self.record(self.bundle("x64", b"incompatible"), target=X64)
        self.assertNotEqual(self.select(ARM, X64).returncode, 0)

    def test_equivalent_abi_jars_allow_different_zip_order(self):
        current = self.bundle("arm")
        self.record(current)
        self.record(self.bundle("x64", order=("Factory.class", "Track.class")), target=X64)
        result = self.select(ARM, X64)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(result.stdout.strip()), current)

    def test_link_directory_without_native_archive_fails(self):
        current = self.bundle("current")
        self.record(current)
        (current.parent / "lib/libwebrtc.a").unlink()
        self.assertNotEqual(self.select().returncode, 0)

    def test_each_requested_abi_requires_its_own_linker_record(self):
        self.record(self.bundle("arm"))
        self.assertNotEqual(self.select(ARM, X64).returncode, 0)

    def test_corrupt_linked_jar_fails(self):
        current = self.bundle("current")
        self.record(current)
        current.write_bytes(b"not a Java archive")
        self.assertNotEqual(self.select().returncode, 0)


if __name__ == "__main__":
    unittest.main()
