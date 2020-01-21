load("@io_bazel_rules_docker//container:container.bzl", "container_image")

package(
    default_visibility = ["//src:__subpackages__"],
)

java_library(
    name = "common",
    srcs = glob([
        "common/*.java",
        "common/function/*.java",
        "common/io/*.java",
    ]),
    deps = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/guava:failureaccess",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "common-cache",
    srcs = glob(["common/cache/*.java"]),
    deps = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/guava:failureaccess",
        "//3rdparty/jvm/com/google/j2objc:j2objc_annotations",
        "//3rdparty/jvm/org/checkerframework:checker_qual",
    ],
)

java_library(
    name = "common-grpc",
    srcs = glob([
        "common/grpc/*.java",
    ]),
    deps = [
        ":common",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "instance",
    srcs = [
        "instance/Instance.java",
        "instance/Utils.java",
    ],
    deps = [
        ":common",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/guava:failureaccess",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "server-instance",
    srcs = [
        "instance/AbstractServerInstance.java",
        "instance/OperationsMap.java",
        "instance/WatchFuture.java",
    ],
    deps = [
        ":ac",
        ":cas",
        ":common",
        ":instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "memory-instance",
    srcs = glob(["instance/memory/*.java"]),
    deps = [
        ":ac",
        ":cas",
        ":common",
        ":instance",
        ":server-instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "shard-instance",
    srcs = glob(["instance/shard/*.java"]),
    deps = [
        ":common",
        ":common-cache",
        ":common-grpc",
        ":instance",
        ":server-instance",
        ":stub-instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//3rdparty/jvm/org/apache/commons:commons_pool2",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "server",
    srcs = glob(["server/**/*.java"]),
    deps = [
        ":cas",
        ":common",
        ":common-grpc",
        ":instance",
        ":memory-instance",
        ":shard-instance",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
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
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "@remote_apis//:build_bazel_semver_java_proto",
    ],
)

java_binary(
    name = "buildfarm-server",
    main_class = "build.buildfarm.server.BuildFarmServer",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":server",
    ],
)

container_image(
    name = "server.container",
    base = "@java_base//image",
    cmd = [
        "buildfarm-server_deploy.jar",
        "/config/server.config",
        "--port",
        "8980",
    ],
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-server_deploy.jar",
    ],
    visibility = ["//visibility:public"],
)

java_library(
    name = "stub-instance",
    srcs = glob(["instance/stub/*.java"]),
    deps = [
        ":common",
        ":common-grpc",
        ":instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
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
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "shard-worker",
    srcs = glob(["worker/shard/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        ":cas",
        ":common",
        ":common-grpc",
        ":instance",
        ":memory-instance",
        ":server",
        ":server-instance",
        ":shard-instance",
        ":stub-instance",
        ":worker",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
    ],
)

java_binary(
    name = "buildfarm-shard-worker",
    main_class = "build.buildfarm.worker.shard.Worker",
    runtime_deps = [
        ":shard-worker",
    ],
)

java_library(
    name = "worker",
    srcs = glob(["worker/*.java"]),
    deps = [
        ":cas",
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/com/github/jnr:jffi_jar_native",
        "//3rdparty/jvm/com/github/jnr:jnr_constants",
        "//3rdparty/jvm/com/github/jnr:jnr_ffi",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/github/serceman:jnr_fuse",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
    ],
)

java_library(
    name = "operationqueue-worker",
    srcs = glob(["worker/operationqueue/*.java"]),
    deps = [
        ":common",
        ":common-grpc",
        ":instance",
        ":stub-instance",
        ":worker",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)

java_library(
    name = "cas",
    srcs = glob(["cas/*.java"]),
    deps = [
        ":common",
        ":common-grpc",
        ":stub-instance",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "ac",
    srcs = glob(["ac/*.java"]),
    deps = [
        ":common",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "buildfarm-operationqueue-worker",
    main_class = "build.buildfarm.worker.operationqueue.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":operationqueue-worker",
    ],
)

container_image(
    name = "buildfarm-operationqueue-worker.container",
    base = "@java_base//image",
    cmd = [
        "buildfarm-operationqueue-worker_deploy.jar",
        "/config/worker.config",
    ],
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-operationqueue-worker_deploy.jar",
    ],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "bf-mount",
    srcs = ["Mount.java"],
    main_class = "build.buildfarm.Mount",
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        ":worker",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
    ],
)

java_binary(
    name = "bf-cas",
    srcs = ["CASTest.java"],
    main_class = "build.buildfarm.CASTest",
    deps = [
        ":common",
        ":worker",
        "//3rdparty/jvm/com/google/guava",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-executor",
    srcs = ["Executor.java"],
    main_class = "build.buildfarm.Executor",
    deps = [
        ":common",
        ":stub-instance",
        ":worker",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-extractor",
    srcs = ["Extract.java"],
    main_class = "build.buildfarm.Extract",
    deps = [
        ":common",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-cat",
    srcs = ["Cat.java"],
    main_class = "build.buildfarm.Cat",
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
    ],
)

java_binary(
    name = "bf-hist",
    srcs = ["Hist.java"],
    main_class = "build.buildfarm.Hist",
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "buildfarm-http-proxy",
    main_class = "build.buildfarm.proxy.http.HttpProxy",
    runtime_deps = [":http-proxy"],
)

java_library(
    name = "http-proxy",
    srcs = glob(["proxy/http/*.java"]),
    deps = [
        ":common",
        "//3rdparty/jvm/com/github/pcj:google_options",
        "//3rdparty/jvm/com/google/auth:google_auth_library_credentials",
        "//3rdparty/jvm/com/google/auth:google_auth_library_oauth2_http",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_auth",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//3rdparty/jvm/io/netty:netty_buffer",
        "//3rdparty/jvm/io/netty:netty_codec",
        "//3rdparty/jvm/io/netty:netty_codec_http",
        "//3rdparty/jvm/io/netty:netty_common",
        "//3rdparty/jvm/io/netty:netty_handler",
        "//3rdparty/jvm/io/netty:netty_transport",
        "//3rdparty/jvm/io/netty:netty_transport_native_epoll",
        "//3rdparty/jvm/io/netty:netty_transport_native_kqueue",
        "//3rdparty/jvm/io/netty:netty_transport_native_unix_common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-cancel",
    srcs = ["Cancel.java"],
    main_class = "build.buildfarm.Cancel",
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "//3rdparty/jvm/io/grpc:grpc_api",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
    ],
)
