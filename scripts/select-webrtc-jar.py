#!/usr/bin/env python3
# Copyright 2026 Element Creations Ltd.
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

"""Select Java classes from the WebRTC archive Cargo actually linked."""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import zipfile


def jar_payload(path):
    with zipfile.ZipFile(path) as archive:
        entries = [entry for entry in archive.infolist() if not entry.is_dir()]
        names = [entry.filename for entry in entries]
        if len(names) != len(set(names)) or not any(name.endswith(".class") for name in names):
            raise ValueError(f"Invalid WebRTC Java payload: {path}")
        return {entry.filename: hashlib.sha256(archive.read(entry)).hexdigest() for entry in entries}


def select_jar(records, package_id, target_dir, profile_dir, targets):
    selected = []
    for target in targets:
        build_root = (target_dir / target / profile_dir / "build").resolve()
        candidates = set()
        for record in records:
            if (record.get("reason") != "build-script-executed"
                    or record.get("package_id") != package_id):
                continue
            try:
                Path(record["out_dir"]).resolve().relative_to(build_root)
            except ValueError:
                continue
            for linked_path in record["linked_paths"]:
                directory = Path(linked_path.removeprefix("native=")).resolve()
                if (directory / "libwebrtc.a").is_file():
                    candidates.add(directory.parent / "libwebrtc.jar")
        if len(candidates) != 1:
            raise ValueError(f"Expected one linked WebRTC bundle for {target}, found {len(candidates)}")
        jar = candidates.pop()
        payload = jar_payload(jar)
        if selected and payload != selected[0][1]:
            raise ValueError(f"WebRTC Java classes differ between {targets[0]} and {target}")
        selected.append((jar, payload))
        print(f"{target}: linked WebRTC jar {jar} (sha256 {hashlib.sha256(jar.read_bytes()).hexdigest()})",
              file=sys.stderr)
    return selected[0][0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=Path, required=True)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--profile-dir", required=True)
    parser.add_argument("--targets", nargs="+", required=True)
    args = parser.parse_args()
    try:
        records = [json.loads(line) for line in args.messages.read_text().splitlines() if line.strip()]
        print(select_jar(records, args.package_id, args.target_dir, args.profile_dir, args.targets))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.exit(1, f"Cannot select the linked WebRTC jar: {error}\n")


if __name__ == "__main__":
    main()
