#!/usr/bin/env python3
# Copyright 2026 The Buildfarm Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate reproducible, varied CDC data and verify the remote output."""

import argparse
import hashlib
import json
from pathlib import Path

SIZE = 8 * 1024 * 1024
MARKER = b"buildfarm-cdc-output\n"


def generate(seed):
    # SHAKE's specified byte stream is independent of Python/random versions.
    return hashlib.shake_256(b"buildfarm-cdc-fixture-v1\0" + seed.encode()).digest(SIZE)


def digest(data):
    return {"hash": hashlib.sha256(data).hexdigest(), "sizeBytes": str(len(data))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--seed", required=True)
    prepare.add_argument("--workspace", type=Path, required=True)
    prepare.add_argument("--manifest", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        data = generate(args.seed)
        (args.workspace / "input.bin").write_bytes(data)
        manifest = {
            "seed": args.seed,
            "target": "//:artifact",
            "input": digest(data),
            "output": digest(MARKER + data),
        }
        args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps(manifest, indent=2))
    else:
        manifest = json.loads(args.manifest.read_text())
        actual = digest(args.output.read_bytes())
        if actual != manifest["output"]:
            raise SystemExit(
                "CDC output mismatch: expected %s, got %s"
                % (manifest["output"], actual)
            )
        print("Verified CDC artifact: %s/%s" % (actual["hash"], actual["sizeBytes"]))


if __name__ == "__main__":
    main()
