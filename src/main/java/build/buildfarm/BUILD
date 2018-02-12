package(
    default_visibility = ["//src:__subpackages__"],
)

java_library(
    name = "common",
    srcs = glob(["common/**/*.java"]),
    deps = [
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
    ],
)

java_library(
    name = "instance",
    srcs = [
        "instance/Instance.java",
    ],
    deps = [
        ":common",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
    ],
)

java_library(
    name = "server-instance",
    srcs = [
        "instance/AbstractServerInstance.java",
        "instance/TokenizableIterator.java",
    ],
    deps = [
        ":common",
        ":instance",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
    ],
)

java_library(
    name = "memory-instance",
    srcs = glob(["instance/memory/*.java"]),
    deps = [
        ":common",
        ":instance",
        ":server-instance",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
    ],
)

java_library(
    name = "buildfarm",
    srcs = glob(["server/**/*.java"]),
    deps = [
        ":common",
        ":instance",
        ":memory-instance",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
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
    ],
)

java_library(
    name = "stub-instance",
    srcs = glob(["instance/stub/*.java"]),
    deps = [
        ":common",
        ":instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
    ],
)

java_library(
    name = "worker",
    srcs = glob(["worker/*.java"]),
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
    ],
)

java_library(
    name = "operationqueue-worker",
    srcs = glob(["worker/operationqueue/*.java"]),
    deps = [
        ":worker",
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)

java_binary(
    name = "buildfarm-worker",
    main_class = "build.buildfarm.worker.operationqueue.Worker",
    runtime_deps = [
        ":operationqueue-worker",
    ],
)
