#!/usr/bin/env python3
# Copyright 2026 Element Creations Ltd.
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

"""Capture Cargo records inside the unchanged cargo-ndk build/link environment."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


def capture_cargo():
    environment = dict(os.environ)
    environment.pop("RTC_ANDROID_CARGO_PROXY")
    real_cargo = environment["RTC_ANDROID_REAL_CARGO"]
    environment["CARGO"] = real_cargo
    command = [real_cargo, *sys.argv[1:]]
    if not any(argument.startswith("--message-format") for argument in sys.argv[1:]):
        os.execvpe(real_cargo, command, environment)
    # cargo-ndk consumes build-script messages. Observe its Cargo subprocess
    # before forwarding the exact stream; keep all private NDK linker flags.
    result = subprocess.run(command, env=environment, stdout=subprocess.PIPE)
    with Path(environment["RTC_ANDROID_BUILD_MESSAGES"]).open("ab") as messages:
        messages.write(result.stdout)
    sys.stdout.buffer.write(result.stdout)
    sys.stdout.buffer.flush()
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=Path, required=True)
    parser.add_argument("--targets", nargs="+", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("A Cargo build or check command is required after --")
    try:
        real_cargo = os.environ.get("CARGO") or shutil.which("cargo")
        cargo_ndk = shutil.which("cargo-ndk")
        if not real_cargo or not cargo_ndk:
            raise ValueError("Cargo and cargo-ndk must be installed")
        args.messages.write_bytes(b"")
        environment = dict(os.environ, CARGO=str(Path(__file__).resolve()),
                           RTC_ANDROID_CARGO_PROXY="1", RTC_ANDROID_REAL_CARGO=real_cargo,
                           RTC_ANDROID_BUILD_MESSAGES=str(args.messages.resolve()))
        for target in args.targets:
            subprocess.run([cargo_ndk, "ndk", "--target", target, *command],
                           env=environment, check=True)
    except subprocess.CalledProcessError as error:
        return error.returncode
    except (OSError, ValueError) as error:
        parser.exit(1, f"Cannot build Android native library: {error}\n")
    return 0


if __name__ == "__main__":
    sys.exit(capture_cargo() if os.environ.get("RTC_ANDROID_CARGO_PROXY") == "1" else main())
