load("@io_bazel_rules_docker//container:container.bzl", "container_image")

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
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
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
        "@googleapis//:google_longrunning_operations_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
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
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
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
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
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
        "@googleapis//:google_longrunning_operations_java_grpc",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_watch_v1_java_grpc",
        "@googleapis//:google_watch_v1_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "@remote_apis//:build_bazel_semver_java_proto",
    ],
)

java_binary(
    name = "buildfarm-server",
    main_class = "build.buildfarm.server.BuildFarmServer",
    runtime_deps = [
        ":buildfarm",
    ],
    visibility = ["//visibility:public"],
)

container_image(
    name = "server.container",
    base = "@java_base//image",
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-server_deploy.jar"
    ],
    cmd = [
        "buildfarm-server_deploy.jar",
        "/config/server.config",
        "--port",
        "8098"
    ],
    visibility = ["//visibility:public"],
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
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
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
    visibility = ["//visibility:public"],
)

container_image(
    name = "worker.container",
    base = "@java_base//image",
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-worker_deploy.jar",
    ],
    cmd = [
        "buildfarm-worker_deploy.jar",
        "/config/worker.config",
    ],
    visibility = ["//visibility:public"],
)
