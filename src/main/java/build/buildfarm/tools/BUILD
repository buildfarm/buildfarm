load("@rules_java//java:java_binary.bzl", "java_binary")
load("@rules_java//java:java_library.bzl", "java_library")
load("@rules_java//java:java_plugin.bzl", "java_plugin")

java_binary(
    name = "bf-mount",
    srcs = ["Mount.java"],
    main_class = "build.buildfarm.tools.Mount",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/java/build/buildfarm/worker",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)

java_plugin(
    name = "io-benchmark-plugins",
    processor_class = "org.openjdk.jmh.generators.BenchmarkProcessor",
    deps = [
        "@buildfarm_maven//:org_openjdk_jmh_jmh_core",
        "@buildfarm_maven//:org_openjdk_jmh_jmh_generator_annprocess",
    ],
)

java_binary(
    name = "bfssh",
    srcs = ["Bfssh.java"],
    main_class = "build.buildfarm.tools.Bfssh",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//third_party/remote-apis:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-executor",
    srcs = ["Executor.java"],
    main_class = "build.buildfarm.tools.Executor",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//third_party/remote-apis:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-extractor",
    srcs = ["Extract.java"],
    main_class = "build.buildfarm.tools.Extract",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-upload",
    srcs = ["Upload.java"],
    main_class = "build.buildfarm.tools.Upload",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-diff",
    srcs = ["Diff.java"],
    main_class = "build.buildfarm.tools.Diff",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/common/resources",
        "//src/main/java/build/buildfarm/common/resources:resource_java_proto",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/java/build/buildfarm/server/services",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)

java_binary(
    name = "bf-cat",
    srcs = ["Cat.java"],
    data = [
        "//examples:example_properties",
    ],
    main_class = "build.buildfarm.tools.Cat",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/common/resources",
        "//src/main/java/build/buildfarm/common/resources:resource_java_proto",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/java/build/buildfarm/server/services",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)

java_binary(
    name = "bf-workerprofile",
    srcs = ["WorkerProfile.java"],
    data = [
        "//examples:example_properties",
    ],
    main_class = "build.buildfarm.tools.WorkerProfile",
    visibility = ["//visibility:public"],
    deps = [
        ":worker-profiler-printer",
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_github_pcj_google_options",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:redis_clients_jedis",
    ],
)

java_binary(
    name = "bf-ac",
    srcs = ["Ac.java"],
    data = [
        "//examples:example_properties",
    ],
    main_class = "build.buildfarm.tools.Ac",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)

java_binary(
    name = "GracefulShutdown",
    srcs = ["GracefulShutdown.java"],
    main_class = "build.buildfarm.tools.GracefulShutdown",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_grpc",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:io_grpc_grpc_api",
    ],
)

java_binary(
    name = "bf-hist",
    srcs = ["Hist.java"],
    main_class = "build.buildfarm.tools.Hist",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/java/build/buildfarm/server/services",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-index-worker",
    srcs = ["IndexWorker.java"],
    main_class = "build.buildfarm.tools.IndexWorker",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)

java_binary(
    name = "buildfarm-http-proxy",
    main_class = "build.buildfarm.proxy.http.HttpProxy",
    visibility = ["//visibility:public"],
    runtime_deps = ["//src/main/java/build/buildfarm/proxy/http"],
)

java_binary(
    name = "bf-cancel",
    srcs = ["Cancel.java"],
    main_class = "build.buildfarm.tools.Cancel",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "@buildfarm_maven//:info_picocli_picocli",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
    ],
)

java_library(
    name = "worker-profiler-printer",
    srcs = ["WorkerProfilePrinter.java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_github_pcj_google_options",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:redis_clients_jedis",
    ],
)
