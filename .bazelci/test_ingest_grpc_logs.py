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

"""Synthetic decoder-JSON tests; no cluster, third-party packages, or Bazel needed."""

import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import ingest_grpc_logs as logs

SIZE = 8 * 1024 * 1024


def digest(letter, size=SIZE):
    return {"hash": letter * 64, "sizeBytes": str(size)}


INPUT = digest("a")
OUTPUT = digest("b", SIZE + len(b"buildfarm-cdc-output\n"))
ACTION = digest("c", 100)
MANIFEST = {"input": INPUT, "output": OUTPUT, "target": "//:artifact"}


def entry(method, details=None, status=None):
    return {
        "methodName": method,
        "status": {} if status is None else status,
        "startTime": "2026-01-01T00:00:00.123456789Z",
        "endTime": "2026-01-01T00:00:00.124456789Z",
        "details": details or {},
    }


def phases():
    output_chunks = [
        digest("f", SIZE // 2),
        digest("0", int(OUTPUT["sizeBytes"]) - SIZE // 2),
    ]
    caps = entry(
        logs.CAPABILITIES,
        {
            "getCapabilities": {
                "request": {},
                "response": {
                    "cacheCapabilities": {
                        "splitBlobSupport": True,
                        "spliceBlobSupport": True,
                        "fastCdc2020Params": {"avgChunkSizeBytes": "524288"},
                    }
                },
            }
        },
    )
    execute = entry(
        logs.REMOTE_EXECUTIONS,
        {"execute": {"request": {"actionDigest": ACTION, "skipCacheLookup": True}}},
    )
    execute["metadata"] = {"targetId": "//:artifact"}
    upload = [
        copy.deepcopy(caps),
        entry(
            logs.SPLICE,
            {
                "spliceBlob": {
                    "request": {
                        "blobDigest": OUTPUT,
                        "chunkDigests": output_chunks,
                        "chunkingFunction": "FAST_CDC_2020",
                    },
                    "response": {"blobDigest": OUTPUT},
                }
            },
        ),
    ]
    upload += [
        entry(
            logs.CAS_UPLOAD,
            {
                "write": {
                    "resourceNames": [
                        "uploads/id/blobs/%s/%s" % (d["hash"], d["sizeBytes"]),
                        "",
                    ]
                }
            },
        )
        for d in output_chunks
    ]
    # RemoteExecutionCache bypasses CombinedCache chunking for action inputs.
    cold = [
        copy.deepcopy(caps),
        entry(
            logs.CAS_UPLOAD,
            {
                "write": {
                    "resourceNames": [
                        "uploads/id/blobs/%s/%s" % (INPUT["hash"], INPUT["sizeBytes"])
                    ]
                }
            },
        ),
        execute,
    ]
    cached = [
        copy.deepcopy(caps),
        entry(
            logs.ACTION_CACHE_REQUESTS,
            {
                "getActionResult": {
                    "request": {"actionDigest": ACTION},
                    "response": {
                        "outputFiles": [{"path": "artifact.bin", "digest": OUTPUT}]
                    },
                }
            },
        ),
        entry(
            logs.SPLIT,
            {
                "splitBlob": {
                    "request": {
                        "blobDigest": OUTPUT,
                        "chunkingFunction": "FAST_CDC_2020",
                    },
                    "response": {
                        "chunkDigests": output_chunks,
                        "chunkingFunction": "FAST_CDC_2020",
                    },
                }
            },
        ),
    ]
    cached += [
        entry(
            logs.CAS_DOWNLOADS,
            {
                "read": {
                    "request": {
                        "resourceName": "blobs/%s/%s" % (d["hash"], d["sizeBytes"])
                    }
                }
            },
        )
        for d in output_chunks
    ]
    return copy.deepcopy(upload), copy.deepcopy(cold), copy.deepcopy(cached)


class AnalyzerTest(unittest.TestCase):
    def check(self, upload, cold, cached):
        with contextlib.redirect_stdout(io.StringIO()):
            logs.analyze(upload, "CDC upload")
            logs.analyze(cold, "CDC cold")
            logs.analyze(cached, "CDC cached", cached=True)
            logs.validate_cdc(upload, cold, cached, MANIFEST)

    def test_valid_cdc(self):
        self.check(*phases())

    def test_upload_must_not_execute_remotely(self):
        upload, cold, cached = phases()
        upload.append(entry(logs.REMOTE_EXECUTIONS, status={"code": 14}))
        with self.assertRaisesRegex(
            logs.ValidationError, "unexpected remote execution"
        ):
            self.check(upload, cold, cached)

    def test_whole_output_batch_upload_fallback(self):
        upload, cold, cached = phases()
        upload.append(
            entry(
                logs.BATCH_UPLOAD,
                {"batchUpdateBlobs": {"request": {"requests": [{"digest": OUTPUT}]}}},
            )
        )
        with self.assertRaisesRegex(
            logs.ValidationError, "whole-output BatchUpdateBlobs"
        ):
            self.check(upload, cold, cached)

    def test_upload_requires_successful_associated_chunk_write(self):
        for mutation in ("failed", "unrelated"):
            upload, cold, cached = phases()
            for call in upload[2:]:
                if mutation == "failed":
                    call["status"] = {"code": 14}
                else:
                    call["details"]["write"]["resourceNames"] = [
                        "uploads/id/blobs/%s/%s" % (INPUT["hash"], INPUT["sizeBytes"])
                    ]
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                logs.ValidationError, "missing successful fixture chunk Write"
            ):
                self.check(upload, cold, cached)

    def test_retries_are_not_exact_counts(self):
        upload, cold, cached = phases()
        retry = copy.deepcopy(upload[1])
        retry["status"] = {"code": 14}
        upload.insert(1, retry)
        cached += copy.deepcopy(cached[2:])
        self.check(upload, cold, cached)

    def test_missing_cdc(self):
        for method in (logs.SPLICE, logs.SPLIT):
            with self.subTest(method=method):
                upload, cold, cached = phases()
                upload = [e for e in upload if e["methodName"] != method]
                cached = [e for e in cached if e["methodName"] != method]
                with self.assertRaisesRegex(
                    logs.ValidationError, "missing successful.*" + method
                ):
                    self.check(upload, cold, cached)

    def test_failed_cdc_followed_by_fallback(self):
        for phase_index, method in (
            (0, logs.SPLICE),
            (2, logs.SPLIT),
        ):
            with self.subTest(method=method):
                upload, cold, cached = phases()
                phase = (upload, cold, cached)[phase_index]
                next(e for e in phase if e["methodName"] == method)["status"] = {
                    "code": 12
                }
                transfer = logs.CAS_UPLOAD if phase_index == 0 else logs.CAS_DOWNLOADS
                phase.append(entry(transfer))
                with self.assertRaisesRegex(
                    logs.ValidationError, "missing successful.*" + method
                ):
                    self.check(upload, cold, cached)

    def test_empty_and_non_array_logs(self):
        for data in ([], {}, None, "", [None]):
            with self.subTest(data=data), self.assertRaises(logs.ValidationError):
                logs.analyze(data, "cold")

    def test_unexpected_cached_execution_and_uploads(self):
        for method in logs.FORBIDDEN_CACHED:
            with self.subTest(method=method):
                upload, cold, cached = phases()
                cached.append(entry(method, status={"code": 14}))
                with self.assertRaisesRegex(
                    logs.ValidationError, "unexpected execution/upload"
                ):
                    self.check(upload, cold, cached)

    def test_chunk_metadata(self):
        for name, index in (("spliceBlob", 0), ("splitBlob", 1)):
            for change in (
                "missing",
                "single",
                "sum",
                "hash",
                "size",
                "function",
                "response",
            ):
                with self.subTest(name=name, change=change):
                    upload, cold, cached = phases()
                    call = (upload[1], cached[2])[index]["details"][name]
                    chunk_holder = call["request"] if index == 0 else call["response"]
                    if change == "missing":
                        del chunk_holder["chunkDigests"]
                    elif change == "single":
                        chunk_holder["chunkDigests"] = [INPUT]
                    elif change == "sum":
                        chunk_holder["chunkDigests"][0]["sizeBytes"] = "1"
                    elif change == "hash":
                        chunk_holder["chunkDigests"][0]["hash"] = "bad"
                    elif change == "size":
                        chunk_holder["chunkDigests"][0]["sizeBytes"] = True
                    elif change == "function":
                        chunk_holder["chunkingFunction"] = "UNKNOWN"
                    else:
                        del call["response"]
                    with self.assertRaises(logs.ValidationError):
                        self.check(upload, cold, cached)

    def test_wrong_blob_association(self):
        for index, name in ((0, "spliceBlob"), (1, "splitBlob")):
            upload, cold, cached = phases()
            (upload[1], cached[2])[index]["details"][name]["request"]["blobDigest"] = (
                digest("1")
            )
            with self.subTest(name=name), self.assertRaisesRegex(
                logs.ValidationError, "missing successful"
            ):
                self.check(upload, cold, cached)

    def test_wrong_splice_response_digest(self):
        upload, cold, cached = phases()
        upload[1]["details"]["spliceBlob"]["response"]["blobDigest"] = INPUT
        with self.assertRaisesRegex(logs.ValidationError, "wrong splice response"):
            self.check(upload, cold, cached)

    def test_split_request_and_response_function(self):
        for holder in ("request", "response"):
            upload, cold, cached = phases()
            cached[2]["details"]["splitBlob"][holder]["chunkingFunction"] = "UNKNOWN"
            with self.subTest(holder=holder), self.assertRaisesRegex(
                logs.ValidationError, "not FastCDC"
            ):
                self.check(upload, cold, cached)

    def test_missing_details(self):
        upload, cold, cached = phases()
        del upload[1]["details"]["spliceBlob"]
        with self.assertRaisesRegex(logs.ValidationError, "missing spliceBlob details"):
            self.check(upload, cold, cached)

    def test_missing_remote_fixture_execution(self):
        for mutation in ("remove", "target", "skip", "failed"):
            upload, cold, cached = phases()
            call = cold[2]
            if mutation == "remove":
                cold.remove(call)
            elif mutation == "target":
                call["metadata"]["targetId"] = "//:other"
            elif mutation == "skip":
                call["details"]["execute"]["request"]["skipCacheLookup"] = False
            else:
                call["status"] = {"code": 14}
            with self.subTest(mutation=mutation), self.assertRaises(
                logs.ValidationError
            ):
                self.check(upload, cold, cached)

    def test_cache_hit_action_and_output(self):
        for mutation in ("action", "output", "status"):
            upload, cold, cached = phases()
            value = cached[1]["details"]["getActionResult"]
            if mutation == "action":
                value["request"]["actionDigest"] = digest("1")
            elif mutation == "output":
                value["response"]["outputFiles"][0]["digest"] = INPUT
            else:
                cached[1]["status"] = {"code": 5}
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                logs.ValidationError, "GetActionResult"
            ):
                self.check(upload, cold, cached)

    def test_whole_blob_fallback_even_after_successful_cdc(self):
        upload, cold, cached = phases()
        upload.append(
            entry(
                logs.CAS_UPLOAD,
                {
                    "write": {
                        "resourceNames": [
                            "uploads/id/blobs/%s/%s"
                            % (OUTPUT["hash"], OUTPUT["sizeBytes"])
                        ]
                    }
                },
            )
        )
        with self.assertRaisesRegex(
            logs.ValidationError, "whole-output Write fallback"
        ):
            self.check(upload, cold, cached)
        upload, cold, cached = phases()
        cached.append(
            entry(
                logs.CAS_DOWNLOADS,
                {
                    "read": {
                        "request": {
                            "resourceName": "blobs/%s/%s"
                            % (OUTPUT["hash"], OUTPUT["sizeBytes"])
                        }
                    }
                },
            )
        )
        with self.assertRaisesRegex(logs.ValidationError, "whole-output Read fallback"):
            self.check(upload, cold, cached)

    def test_missing_chunk_transfers(self):
        for method in (logs.CAS_UPLOAD, logs.CAS_DOWNLOADS):
            upload, cold, cached = phases()
            upload = [e for e in upload if e["methodName"] != method]
            cached = [e for e in cached if e["methodName"] != method]
            with self.subTest(method=method), self.assertRaisesRegex(
                logs.ValidationError, "chunk (Write|Read)"
            ):
                self.check(upload, cold, cached)

    def test_advertised_threshold_and_capabilities(self):
        for mutation in ("size", "missing", "split", "splice"):
            upload, cold, cached = phases()
            caps = cold[0]["details"]["getCapabilities"]["response"][
                "cacheCapabilities"
            ]
            if mutation == "size":
                caps["fastCdc2020Params"]["avgChunkSizeBytes"] = str(SIZE)
            elif mutation == "missing":
                del caps["fastCdc2020Params"]
            else:
                caps[mutation + "BlobSupport"] = False
            with self.subTest(mutation=mutation), self.assertRaises(
                logs.ValidationError
            ):
                self.check(upload, cold, cached)

    def test_protobuf_default_and_explicit_ok_status(self):
        self.assertEqual(logs.status_code(entry(logs.SPLIT)), 0)
        self.assertEqual(logs.status_code(entry(logs.SPLIT, status={"code": 0})), 0)

    def test_protobuf_nanosecond_timestamp_and_timezone(self):
        self.assertAlmostEqual(
            logs.calculate_duration(
                "2026-01-01T02:00:00.123456789+02:00", "2026-01-01T00:00:00.124456789Z"
            ),
            1.0,
        )

    def test_missing_and_malformed_status(self):
        for status in (
            None,
            [],
            "OK",
            {"code": "0"},
            {"code": False},
            {"code": -1},
            {"code": 17},
            {"message": 1},
            {"details": {}},
        ):
            call = entry(logs.SPLIT)
            call["status"] = status
            with self.subTest(status=status), self.assertRaises(logs.ValidationError):
                logs.status_code(call)
        call = entry(logs.SPLIT)
        del call["status"]
        with self.assertRaises(logs.ValidationError):
            logs.status_code(call)

    def test_malformed_timestamps_and_methods(self):
        for key, value in (
            ("startTime", "bad"),
            ("endTime", None),
            ("methodName", 42),
            ("methodName", ""),
            ("endTime", "2025-01-01T00:00:00Z"),
        ):
            call = entry(logs.SPLIT)
            call[key] = value
            with self.subTest(key=key), self.assertRaises(logs.ValidationError):
                logs.analyze([call], "cold")

    def test_baseline_requires_successful_cache_activity(self):
        with self.assertRaisesRegex(logs.ValidationError, "missing successful"):
            logs.analyze([entry(logs.CAPABILITIES)], "Build #2", cached=True)
        with self.assertRaisesRegex(logs.ValidationError, "missing successful"):
            logs.analyze(
                [entry(logs.ACTION_CACHE_REQUESTS, status={"code": 5})],
                "Build #2",
                cached=True,
            )

    def invoke(self, *phase_logs, cdc=False, malformed=False):
        with tempfile.TemporaryDirectory() as directory:
            paths = [
                Path(directory) / ("phase%d.json" % index)
                for index in range(len(phase_logs))
            ]
            for path, data in zip(paths, phase_logs):
                path.write_text(json.dumps(data))
            if malformed:
                paths[0].write_text("not JSON")
            args = [str(path) for path in paths]
            if cdc:
                manifest = Path(directory) / "manifest.json"
                manifest.write_text(json.dumps(MANIFEST))
                args = ["--cdc-manifest", str(manifest)] + args
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                return logs.main(args)

    def test_existing_cli_invocation_without_cdc(self):
        cold = [entry(logs.REMOTE_EXECUTIONS)]
        cached = [entry(logs.ACTION_CACHE_REQUESTS)]
        self.assertEqual(self.invoke(cold, cached), 0)

    def test_cdc_cli_invocation(self):
        self.assertEqual(self.invoke(*phases(), cdc=True), 0)
        self.assertEqual(self.invoke(*phases()[1:], cdc=True), 1)
        upload, cold, cached = phases()
        self.assertEqual(self.invoke(cold, upload, cached, cdc=True), 1)
        self.assertEqual(self.invoke(upload, cached, cold, cdc=True), 1)

    def test_cli_rejects_empty_and_malformed_logs(self):
        upload, cold, cached = phases()
        self.assertEqual(self.invoke([], cached), 1)
        self.assertEqual(self.invoke(cold, cached, malformed=True), 1)
        upload[1]["details"] = []
        self.assertEqual(self.invoke(upload, cold, cached, cdc=True), 1)


class FixtureTest(unittest.TestCase):
    def test_harness_decodes_and_forwards_phase_logs(self):
        root = Path(__file__).resolve().parents[1]
        harness = (root / ".bazelci/test_buildfarm_container.sh").read_text()
        # Exercise only log handling, not server startup or process cleanup.
        definitions = (
            harness[harness.index("parse_grpc_logs(){") : harness.index("# Trap EXIT")]
            + harness[
                harness.index("calculate_cache_statistics(){") : harness.index(
                    "\nstart_server_and_worker\n"
                )
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            decoder = directory / "tools_remote/bazel-bin/remote_client"
            decoder.parent.mkdir(parents=True)
            decoder.write_text(
                "#!/bin/bash\n" 'echo "$*" >> "$MOCK_DECODES"\n' "echo '[]'\n"
            )
            decoder.chmod(0o755)
            python = directory / "python3"
            python.write_text("#!/bin/bash\n" 'echo "$*" >> "$MOCK_ANALYSES"\n')
            python.chmod(0o755)
            for mode in ("baseline", "cdc"):
                env = dict(
                    os.environ,
                    PATH=str(directory) + os.pathsep + os.environ["PATH"],
                    CACHE_TEST="true",
                    CACHE_EXPECTATIONS=mode,
                    CDC_ARTIFACTS=str(directory),
                    MOCK_DECODES=str(directory / (mode + "-decodes")),
                    MOCK_ANALYSES=str(directory / (mode + "-analyses")),
                    RAW_GRPC_LOGS1="cold.grpc.log",
                    RAW_GRPC_LOGS2="cached.grpc.log",
                    RAW_GRPC_LOGS_UPLOAD="upload.grpc.log",
                    GRPC_LOGS1="cold.grpc.json",
                    GRPC_LOGS2="cached.grpc.json",
                    GRPC_LOGS_UPLOAD="upload.grpc.json",
                )
                subprocess.run(
                    ["bash", "-e", "-c", definitions + "\nparse_grpc_logs\n"],
                    cwd=directory,
                    env=env,
                    check=True,
                    capture_output=True,
                )
                decoded = Path(env["MOCK_DECODES"]).read_text().splitlines()
                self.assertEqual(len(decoded), 3 if mode == "cdc" else 2)
                analysis = Path(env["MOCK_ANALYSES"]).read_text().strip().split()
                if mode == "cdc":
                    self.assertEqual(
                        analysis[1:3],
                        ["--cdc-manifest", str(directory / "manifest.json")],
                    )
                    self.assertEqual(
                        analysis[3:],
                        ["upload.grpc.json", "cold.grpc.json", "cached.grpc.json"],
                    )
                    self.assertIn("--grpc_log upload.grpc.log", decoded[2])
                else:
                    self.assertEqual(
                        analysis[1:], ["cold.grpc.json", "cached.grpc.json"]
                    )

    def test_runner_isolation_flags_and_fail_fast(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            mock = directory / "bazel"
            calls = directory / "calls.jsonl"
            mock.write_text(
                "#!" + sys.executable + "\n"
                "import json, os, pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "with open(os.environ['MOCK_CALLS'], 'a') as f: f.write(json.dumps(args) + '\\n')\n"
                "if 'version' in args: print('Build label: 9.3.0')\n"
                "elif 'help' in args: print('--[no]experimental_remote_cache_chunking (a boolean)')\n"
                "elif 'build' in args:\n"
                " if os.environ.get('MOCK_FAIL') == '1': sys.exit(23)\n"
                " output = pathlib.Path('bazel-bin'); output.mkdir(exist_ok=True)\n"
                " (output / 'artifact.bin').write_bytes(b'buildfarm-cdc-output\\n' + pathlib.Path('input.bin').read_bytes())\n"
            )
            mock.chmod(0o755)
            env = dict(os.environ, CDC_BAZEL=str(mock), MOCK_CALLS=str(calls))
            for fail in (False, True):
                env["CDC_ARTIFACTS"] = str(
                    directory / ("failure" if fail else "success")
                )
                env["MOCK_FAIL"] = "1" if fail else "0"
                result = subprocess.run(
                    [
                        "bash",
                        str(root / ".bazelci/run_content_defined_chunking_test.sh"),
                    ],
                    env=env,
                    capture_output=True,
                )
                self.assertEqual(
                    result.returncode, 23 if fail else 0, result.stderr.decode()
                )
            builds = [
                json.loads(line)
                for line in calls.read_text().splitlines()
                if '"build"' in line and '"help"' not in line
            ]
            self.assertEqual(
                len(builds), 4
            )  # No cold/cached phase after the failed upload build.
            for call in builds:
                for flag in (
                    "--ignore_all_rc_files",
                    "--batch",
                    "--experimental_remote_cache_chunking",
                    "--remote_download_outputs=all",
                    "--remote_verify_downloads",
                    "--disk_cache=",
                    "--noremote_local_fallback",
                    "--remote_cache=grpc://localhost:8980",
                ):
                    self.assertIn(flag, call)
            self.assertIn("--noremote_accept_cached", builds[0])
            self.assertIn("--spawn_strategy=local", builds[0])
            self.assertIn("--strategy=CdcFixture=local", builds[0])
            self.assertIn("--remote_upload_local_results", builds[0])
            self.assertIn("--remote_cache_async=false", builds[0])
            self.assertFalse(
                any(arg.startswith("--remote_executor=") for arg in builds[0])
            )
            self.assertIn("--noremote_accept_cached", builds[1])
            self.assertIn("--remote_accept_cached", builds[2])
            for call in builds[1:3]:
                self.assertIn("--remote_executor=grpc://localhost:8980", call)
                self.assertIn("--spawn_strategy=remote", call)
                self.assertIn("--strategy=CdcFixture=remote", call)
                self.assertIn("--noremote_upload_local_results", call)
            bases = [
                next(arg for arg in call if arg.startswith("--output_base="))
                for call in builds[:3]
            ]
            self.assertEqual(len(set(bases)), 3)
            for phase, call in zip(("upload", "cold", "cached"), builds):
                self.assertTrue(
                    any(arg.endswith("/%s.grpc.log" % phase) for arg in call)
                )

    def test_reproducible_data_and_output_verification(self):
        script = Path(__file__).resolve().parents[1] / "src/test/cdc/fixture.py"
        spec = importlib.util.spec_from_file_location("cdc_fixture", script)
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        data = fixture.generate("recorded-seed")
        self.assertEqual(len(data), SIZE)
        self.assertEqual(data, fixture.generate("recorded-seed"))
        self.assertNotEqual(data, fixture.generate("other-seed"))
        self.assertNotEqual(fixture.digest(data), fixture.digest(fixture.MARKER + data))
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            manifest = directory / "manifest.json"
            output = directory / "artifact.bin"
            subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "prepare",
                    "--seed",
                    "recorded-seed",
                    "--workspace",
                    str(directory),
                    "--manifest",
                    str(manifest),
                ],
                check=True,
                capture_output=True,
            )
            self.assertEqual((directory / "input.bin").read_bytes(), data)
            output.write_bytes(fixture.MARKER + data)
            command = [
                sys.executable,
                str(script),
                "verify",
                "--manifest",
                str(manifest),
                "--output",
                str(output),
            ]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            output.write_bytes(data)
            self.assertNotEqual(
                subprocess.run(command, capture_output=True).returncode, 0
            )


if __name__ == "__main__":
    unittest.main()
