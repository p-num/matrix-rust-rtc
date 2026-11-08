#!/usr/bin/env python3
# Copyright 2026 Element Creations Ltd.
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

"""Check the real NDK environment/build boundary; requires the Android Rust target."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile


SCRIPTS = Path(__file__).resolve().parent
TARGET = "aarch64-linux-android"


class AndroidCargoMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rtc ndk metadata ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.env = dict(os.environ, CARGO_NET_OFFLINE="true", CARGO_BUILD_JOBS="2")
        self.env.pop("CARGO_TARGET_DIR", None)
        (self.root / "src").mkdir()
        (self.root / "Cargo.toml").write_text(
            '[package]\nname="webrtc-sys"\nversion="0.3.51"\nedition="2021"\n')
        (self.root / "src/lib.rs").write_text("pub fn fixture() {}\n")
        self.jar = self.root / "bundles/active/libwebrtc.jar"
        (self.jar.parent / "lib").mkdir(parents=True)
        (self.jar.parent / "lib/libwebrtc.a").write_bytes(b"fixture: check does not link this archive")
        with zipfile.ZipFile(self.jar, "w") as archive:
            archive.writestr("livekit/org/jni_zero/JniZero.class", b"fixture")
        (self.root / "build.rs").write_text(
            'fn main() { println!("cargo:rustc-link-search=native={}", '
            + json.dumps(str(self.jar.parent / "lib")) + "); }\n")
        self.invoke(["cargo", "generate-lockfile", "--offline"])
        self.package_id = self.invoke(["cargo", "pkgid", "--offline"]).stdout.strip()
        self.messages = self.root / "messages.jsonl"

    def invoke(self, args, check=True):
        return subprocess.run(args, cwd=self.root, env=self.env,
                              capture_output=True, text=True, check=check)

    def build(self, check=True):
        return self.invoke([
            "python3", str(SCRIPTS / "cargo-android-json.py"),
            "--messages", str(self.messages), "--targets", TARGET, "--", "check",
            "--offline", "--jobs", "2", "--target-dir", str(self.root / "target"),
        ], check=check)

    def test_fresh_and_cached_cargo_builds_preserve_the_linked_bundle(self):
        for state in ("fresh", "cached"):
            with self.subTest(state=state):
                self.build()
                records = [json.loads(line) for line in self.messages.read_text().splitlines()]
                self.assertEqual(records[-1], {"reason": "build-finished", "success": True})
                result = self.invoke([
                    "python3", str(SCRIPTS / "select-webrtc-jar.py"),
                    "--messages", str(self.messages), "--package-id", self.package_id,
                    "--target-dir", str(self.root / "target"), "--profile-dir", "debug",
                    "--targets", TARGET,
                ])
                self.assertEqual(Path(result.stdout.strip()), self.jar)

    def test_compile_error_preserves_failure_and_drops_previous_success(self):
        self.build()
        (self.root / "src/lib.rs").write_text('compile_error!("expected metadata regression control");\n')
        result = self.build(check=False)
        self.assertEqual(result.returncode, 101)
        self.assertIn("expected metadata regression control", result.stderr)
        records = [json.loads(line) for line in self.messages.read_text().splitlines()]
        self.assertEqual(records[-1], {"reason": "build-finished", "success": False})
        self.assertEqual(sum(record.get("reason") == "build-finished" for record in records), 1)


if __name__ == "__main__":
    unittest.main()
