#!/usr/bin/python2
import json
import time
import sys
from dateutil import parser
from collections import defaultdict

REMOTE_EXECUTIONS = "build.bazel.remote.execution.v2.Execution/Execute"
CAS_UPLOAD = "google.bytestream.ByteStream/Write"
CAS_DOWNLOADS = "google.bytestream.ByteStream/Read"
ACTION_CACHE_REQUESTS = ("build.bazel.remote.execution.v2.ActionCache/"
                         "GetActionResult")


def main():
    n = len(sys.argv)
    for i in range(1, n):
        print("=====================")
        print("Build #%d results" % i)
        methods = defaultdict(list)
        parsed = parse_file(sys.argv[i])
        count_remote_actions(parsed, methods)
        calculate_latency(methods)


def date_to_millis(dt):
    return int(time.mktime(dt.utctimetuple()) * 1000 + dt.microsecond / 1000)


def parse_file(file):
    with open(file, "r") as f:
        data = f.read()
    return json.loads(data)


def count_remote_actions(json_data, methods):
    remote_executions = 0
    cas_uploads = 0
    cas_downloads = 0
    action_cache_requests = 0
    for entry in json_data:
        start_time = entry["startTime"]
        end_time = entry["endTime"]
        methods[entry["methodName"]].append(calculate_duration(start_time,
                                                               end_time))
        if REMOTE_EXECUTIONS in entry["methodName"]:
            remote_executions = remote_executions + 1
        elif CAS_UPLOAD in entry["methodName"]:
            cas_uploads = cas_uploads + 1
        elif CAS_DOWNLOADS in entry["methodName"]:
            cas_downloads = cas_downloads + 1
        elif ACTION_CACHE_REQUESTS in entry["methodName"]:
            action_cache_requests = action_cache_requests + 1
    print("Remote executions made: %d" % remote_executions)
    print("CAS uploads made: %d" % cas_uploads)
    print("CAS downloads made: %d" % cas_downloads)
    print("Action Cache requests made: %d" % action_cache_requests)


def calculate_latency(methods):
    for key in methods:
        print("Method: %s" % key)
        print("\tMin: %d ms" % min(methods[key]))
        print("\tMax: %d ms" % max(methods[key]))
        print("\tAvg: %d ms" % int(sum(methods[key])/len(methods[key])))


def calculate_duration(start, end):
    dt_start = parser.parse(start)
    dt_end = parser.parse(end)
    return (date_to_millis(dt_end) - date_to_millis(dt_start))


if __name__ == "__main__":
    main()