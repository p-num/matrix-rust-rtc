#!/usr/bin/env python3
# Copyright 2026 Element Creations Ltd.
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

"""Build with cargo-ndk's environment while preserving Cargo's linker records."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


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
        with args.messages.open("w") as messages:
            for target in args.targets:
                environment = json.loads(subprocess.check_output(
                    ["cargo", "ndk-env", "--target", target, "--json"], text=True))
                if not isinstance(environment, dict) or any(
                        not isinstance(key, str) or not isinstance(value, str)
                        for key, value in environment.items()):
                    raise ValueError("cargo ndk-env did not return string environment variables")
                subprocess.run(["cargo", *command, "--target", target,
                                "--message-format=json-render-diagnostics"],
                               env=dict(os.environ, **environment), stdout=messages, check=True)
    except subprocess.CalledProcessError as error:
        return error.returncode
    except (OSError, ValueError) as error:
        parser.exit(1, f"Cannot build Android native library: {error}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
