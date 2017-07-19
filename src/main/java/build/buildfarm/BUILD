package(
    default_visibility = ["//src:__subpackages__"],
)

java_library(
    name = "common",
    srcs = glob(["common/**/*.java"]),
    deps = [
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
    ]
)

java_library(
    name = "instance",
    srcs = [
        "instance/Instance.java"
    ],
    deps = [
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
    ]
)

java_library(
    name = "server-instance",
    srcs = [
        "instance/AbstractServerInstance.java",
        "instance/TokenizableIterator.java",
    ],
    deps = [
        "//third_party/grpc:grpc-jar",
        ":common",
        ":instance",
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
    ]
)

java_library(
    name = "memory-instance",
    srcs = glob(["instance/memory/*.java"]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/grpc:grpc-jar",
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        ":common",
        ":instance",
        ":server-instance",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
    ]
)

java_library(
    name = "buildfarm",
    srcs = glob(["server/**/*.java"]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/grpc:grpc-jar",
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        ":common",
        ":instance",
        ":memory-instance",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_grpc",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_watch_v1_java_grpc",
        "@googleapis//:google_watch_v1_java_proto",
    ],
)

java_binary(
    name = "buildfarm-server",
    main_class = "build.buildfarm.server.BuildFarmServer",
    runtime_deps = [
      ":buildfarm",
    ]
)

java_library(
    name = "stub-instance",
    srcs = glob(["instance/stub/*.java"]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/grpc:grpc-jar",
        "//third_party/protobuf:protobuf_java",
        "//third_party:guava",
        ":common",
        ":instance",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
    ]
)

java_binary(
    name = "buildfarm-worker",
    srcs = glob(["worker/**/*.java"]),
    main_class = "build.buildfarm.Worker",
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party:guava",
        "//third_party/grpc:grpc-jar",
        "//third_party/protobuf:protobuf_java",
        ":common",
        ":instance",
        ":stub-instance",
    ],
)
