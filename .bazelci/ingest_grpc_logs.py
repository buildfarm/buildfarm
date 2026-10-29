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

"""Validate decoded Bazel gRPC logs, with optional digest-aware CDC expectations."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
import re
import sys

REAPI = "build.bazel.remote.execution.v2."
REMOTE_EXECUTIONS = REAPI + "Execution/Execute"
CAS_UPLOAD = "google.bytestream.ByteStream/Write"
CAS_DOWNLOADS = "google.bytestream.ByteStream/Read"
ACTION_CACHE_REQUESTS = REAPI + "ActionCache/GetActionResult"
BATCH_UPLOAD = REAPI + "ContentAddressableStorage/BatchUpdateBlobs"
SPLICE = REAPI + "ContentAddressableStorage/SpliceBlob"
SPLIT = REAPI + "ContentAddressableStorage/SplitBlob"
CAPABILITIES = REAPI + "Capabilities/GetCapabilities"
FORBIDDEN_CACHED = {REMOTE_EXECUTIONS, CAS_UPLOAD, BATCH_UPLOAD, SPLICE}


class ValidationError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def status_code(entry):
    status = entry.get("status")
    require(isinstance(status, dict), "missing or malformed status object")
    code = status.get("code", 0)  # Protobuf JSON omits the default OK code.
    require(type(code) is int and 0 <= code <= 16, "malformed status code: %r" % code)
    require(
        "message" not in status or isinstance(status["message"], str),
        "malformed status message",
    )
    require(
        "details" not in status or isinstance(status["details"], list),
        "malformed status details",
    )
    return code


def calculate_duration(start, end):
    def parse(value):
        require(isinstance(value, str), "missing or malformed timestamp")
        # Protobuf timestamps have up to nine fractional digits; Python 3.8
        # (Ubuntu 20.04 CI) accepts only microsecond precision here.
        value = re.sub(r"(\.\d{6})\d+(?=Z|[+-])", r"\1", value)
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(dt.tzinfo is not None, "timestamp must include timezone")
        return dt

    duration = (parse(end) - parse(start)).total_seconds() * 1000
    require(duration >= 0, "endTime precedes startTime")
    return duration


def parse_file(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def successful(entries, method):
    return [e for e in entries if e["methodName"] == method and status_code(e) == 0]


def analyze(entries, phase, cached=False):
    require(isinstance(entries, list) and entries, "%s: empty or non-array log" % phase)
    methods = defaultdict(list)
    statuses = defaultdict(Counter)
    for index, entry in enumerate(entries):
        method = entry.get("methodName") if isinstance(entry, dict) else None
        context = "%s entry #%d method %r" % (phase, index + 1, method)
        try:
            require(isinstance(method, str) and method, "missing methodName")
            code = status_code(entry)
            methods[method].append(
                calculate_duration(entry.get("startTime"), entry.get("endTime"))
            )
            statuses[method][code] += 1
        except (ValueError, TypeError) as error:
            raise ValidationError("%s: %s" % (context, error)) from error
    print("=====================\n%s results" % phase)
    for method, durations in sorted(methods.items()):
        print(
            "Method: %s; count: %d; statuses: %s"
            % (method, len(durations), dict(statuses[method]))
        )
        print(
            "\tMin: %.1f ms; Max: %.1f ms; Avg: %.1f ms"
            % (min(durations), max(durations), sum(durations) / len(durations))
        )
    if cached:
        for method in sorted(FORBIDDEN_CACHED):
            require(
                method not in methods,
                "%s: unexpected execution/upload %s (statuses %s)"
                % (phase, method, dict(statuses[method])),
            )
        require(
            successful(entries, ACTION_CACHE_REQUESTS),
            "%s: missing successful %s" % (phase, ACTION_CACHE_REQUESTS),
        )


def positive_int(value, label):
    require(
        type(value) is int or (isinstance(value, str) and value.isdigit()),
        "%s must be an integer" % label,
    )
    result = int(value)
    require(result > 0, "%s must be positive" % label)
    return result


def digest(value):
    require(isinstance(value, dict), "missing or malformed digest")
    hash_value = value.get("hash")
    require(
        isinstance(hash_value, str) and re.fullmatch(r"[0-9a-f]{64}", hash_value),
        "expected a SHA-256 digest hash",
    )
    return hash_value, positive_int(value.get("sizeBytes"), "digest sizeBytes")


def details(entry, name):
    value = entry.get("details", {}).get(name)
    require(isinstance(value, dict), "missing %s details" % name)
    require(isinstance(value.get("request"), dict), "missing %s request" % name)
    return value


def resource_matches(resource, expected):
    return isinstance(resource, str) and resource.endswith("/%s/%d" % expected)


def validate_chunks(entry, name, expected):
    value = details(entry, name)
    request = value["request"]
    require(digest(request.get("blobDigest")) == expected, "wrong fixture blob digest")
    require(
        request.get("chunkingFunction") == "FAST_CDC_2020", "request is not FastCDC"
    )
    response = value.get("response")
    require(isinstance(response, dict), "missing %s response" % name)
    if name == "spliceBlob":
        require(
            digest(response.get("blobDigest")) == expected,
            "wrong splice response digest",
        )
        chunks = request.get("chunkDigests")
    else:
        require(
            response.get("chunkingFunction") == "FAST_CDC_2020",
            "response is not FastCDC",
        )
        chunks = response.get("chunkDigests")
    require(
        isinstance(chunks, list) and len(chunks) > 1, "expected more than one chunk"
    )
    chunks = [digest(chunk) for chunk in chunks]
    require(
        sum(size for _, size in chunks) == expected[1],
        "chunk sizes do not sum to blob size",
    )
    return chunks


def fixture_cdc(entries, phase, method, name, expected):
    matches = []
    for entry in entries:
        if entry["methodName"] != method:
            continue
        try:
            value = details(entry, name)
            # Unrelated blobs cannot satisfy this expectation.
            if digest(value["request"].get("blobDigest")) != expected:
                continue
            if status_code(entry) == 0:
                matches.append(validate_chunks(entry, name, expected))
        except ValidationError as error:
            raise ValidationError(
                "%s: %s status %s: %s" % (phase, method, status_code(entry), error)
            ) from error
    require(
        matches,
        "%s: missing successful %s for fixture %s/%d (no CDC fallback allowed)"
        % (phase, method, *expected),
    )
    return matches[0]


def validate_cdc(upload, cold, cached, manifest):
    input_digest = digest(manifest.get("input"))
    output_digest = digest(manifest.get("output"))
    require(input_digest != output_digest, "fixture input/output must differ")
    for phase, entries in (
        ("CDC upload", upload),
        ("CDC cold", cold),
        ("CDC cached", cached),
    ):
        capabilities = successful(entries, CAPABILITIES)
        require(capabilities, "%s: missing successful GetCapabilities" % phase)
        for entry in capabilities:
            cache = (
                details(entry, "getCapabilities")
                .get("response", {})
                .get("cacheCapabilities", {})
            )
            require(
                cache.get("splitBlobSupport") is True
                and cache.get("spliceBlobSupport") is True,
                "%s: server must advertise split/splice support" % phase,
            )
            average = positive_int(
                cache.get("fastCdc2020Params", {}).get("avgChunkSizeBytes"),
                "%s advertised FastCDC average" % phase,
            )
            require(
                min(input_digest[1], output_digest[1]) > average * 4,
                "%s: fixture must exceed advertised FastCDC maximum (%d bytes)"
                % (phase, average * 4),
            )

    require(
        not any(e["methodName"] == REMOTE_EXECUTIONS for e in upload),
        "CDC upload: unexpected remote execution in local upload phase",
    )
    upload_chunks = fixture_cdc(
        upload, "CDC upload", SPLICE, "spliceBlob", output_digest
    )
    output_chunks = fixture_cdc(cached, "CDC cached", SPLIT, "splitBlob", output_digest)

    executions = successful(cold, REMOTE_EXECUTIONS)
    action_digests = set()
    for entry in executions:
        target = manifest.get("target", "//:artifact")
        if entry.get("metadata", {}).get("targetId") in {
            target,
            "@" + target,
            "@@" + target,
        }:
            request = details(entry, "execute")["request"]
            require(
                request.get("skipCacheLookup") is True,
                "CDC cold: fixture Execute must skip cache lookup",
            )
            action_digests.add(digest(request.get("actionDigest")))
    require(action_digests, "CDC cold: missing successful remote fixture Execute")
    cache_hits = []
    for entry in successful(cached, ACTION_CACHE_REQUESTS):
        value = details(entry, "getActionResult")
        if digest(value["request"].get("actionDigest")) in action_digests:
            outputs = value.get("response", {}).get("outputFiles", [])
            if any(digest(output.get("digest")) == output_digest for output in outputs):
                cache_hits.append(entry)
    require(
        cache_hits,
        "CDC cached: missing successful fixture GetActionResult with expected output digest",
    )

    writes = successful(upload, CAS_UPLOAD)
    require(
        any(
            resource_matches(resource, chunk)
            for entry in writes
            for resource in entry.get("details", {})
            .get("write", {})
            .get("resourceNames", [])
            for chunk in upload_chunks
        ),
        "CDC upload: missing successful fixture chunk Write",
    )
    for entry in upload:
        if entry["methodName"] == CAS_UPLOAD:
            resources = (
                entry.get("details", {}).get("write", {}).get("resourceNames", [])
            )
            require(
                not any(
                    resource_matches(resource, output_digest) for resource in resources
                ),
                "CDC upload: whole-output Write fallback",
            )
        elif entry["methodName"] == BATCH_UPLOAD:
            requests = details(entry, "batchUpdateBlobs")["request"].get("requests", [])
            require(
                not any(
                    digest(request.get("digest")) == output_digest
                    for request in requests
                ),
                "CDC upload: whole-output BatchUpdateBlobs fallback",
            )
    reads = successful(cached, CAS_DOWNLOADS)
    for chunk in set(output_chunks):
        require(
            any(
                resource_matches(
                    entry.get("details", {})
                    .get("read", {})
                    .get("request", {})
                    .get("resourceName"),
                    chunk,
                )
                for entry in reads
            ),
            "CDC cached: missing successful chunk Read for %s/%d" % chunk,
        )
    for entry in cached:
        if entry["methodName"] == CAS_DOWNLOADS:
            resource = (
                entry.get("details", {})
                .get("read", {})
                .get("request", {})
                .get("resourceName")
            )
            require(
                not resource_matches(resource, output_digest),
                "CDC cached: whole-output Read fallback",
            )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cdc-manifest",
        help="fixture digests; requires upload, cold, cached logs in that order",
    )
    parser.add_argument("logs", nargs="+")
    args = parser.parse_args(argv)
    try:
        if args.cdc_manifest:
            require(
                len(args.logs) == 3,
                "CDC mode requires exactly three phase logs: upload, cold, cached",
            )
        parsed = [parse_file(path) for path in args.logs]
        for index, entries in enumerate(parsed):
            phase = (
                ("CDC upload", "CDC cold", "CDC cached")[index]
                if args.cdc_manifest
                else "Build #%d" % (index + 1)
            )
            analyze(
                entries, phase, cached=index == 2 if args.cdc_manifest else index >= 1
            )
        if args.cdc_manifest:
            validate_cdc(*parsed, parse_file(args.cdc_manifest))
        return 0
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
        print("gRPC log validation failed: %s" % error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
