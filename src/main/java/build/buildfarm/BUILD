load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("//:defs.bzl", "ensure_accurate_metadata")

package(
    default_visibility = ["//src:__subpackages__"],
)

filegroup(
    name = "configs",
    srcs = glob(["logging.properties"]),
    visibility = ["//visibility:public"],
)

genrule(
    name = "tini.binary",
    srcs = ["@tini//file"],
    outs = ["tini"],
    cmd = "cp $< $@ && chmod +x $@",
)

java_library(
    name = "common",
    srcs = glob([
        "common/*.java",
        "common/function/*.java",
        "common/io/*.java",
        "common/redis/*.java",
    ]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_github_jnr_jnr_constants",
        "@maven//:com_github_jnr_jnr_ffi",
        "@maven//:com_github_jnr_jnr_posix",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_failureaccess",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:org_threeten_threetenbp",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "common-cache",
    srcs = glob(["common/cache/*.java"]),
    deps = [
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_failureaccess",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_j2objc_j2objc_annotations",
        "@maven//:org_checkerframework_checker_qual",
    ],
)

java_library(
    name = "operations-finder",
    srcs = [
        "operations/OperationsFinder.java",
        "operations/EnrichedOperationBuilder.java",
    ],
    deps = [
        ":ac",
        ":common",
        ":common-cache",
        ":common-grpc",
        ":common-redis",
        ":instance",
        ":server-instance",
        ":stub-instance",
        ":worker-queues",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_apache_commons_commons_pool2",
        "@maven//:com_jayway_jsonpath_json_path",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        ":operations",
    ],
)

java_library(
    name = "operations",
    srcs = glob([
        "operations/*.java",
    ],
            exclude = [
            "operations/OperationsFinder.java",
            "operations/EnrichedOperationBuilder.java",
        ]),
    deps = [
        ":common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:com_jayway_jsonpath_json_path",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_proto",
        "@googleapis//:google_rpc_error_details_proto",
    ],
)

java_library(
    name = "common-grpc",
    srcs = glob([
        "common/grpc/*.java",
    ]),
    deps = [
        ":common",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "metrics",
    srcs = glob([
        "metrics/AbstractMetricsPublisher.java",
        "metrics/MetricsPublisher.java",
    ]),
    deps = [
        ":prometheus-metrics",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "aws-metrics",
    srcs = glob([
        "metrics/aws/*.java",
    ]),
    deps = [
        ":metrics",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_secretsmanager",
        "@maven//:com_amazonaws_aws_java_sdk_sns",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "log-metrics",
    srcs = glob([
        "metrics/log/*.java",
    ]),
    deps = [
        ":metrics",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "gcp-metrics",
    srcs = glob([
        "metrics/gcp/*.java",
    ]),
    deps = [
        ":metrics",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "prometheus-metrics",
    srcs = glob([
        "metrics/prometheus/PrometheusPublisher.java",
    ]),
    deps = [
        "@maven//:io_prometheus_simpleclient",
        "@maven//:io_prometheus_simpleclient_hotspot",
        "@maven//:io_prometheus_simpleclient_httpserver",
    ],
)

java_library(
    name = "admin",
    srcs = glob([
        "admin/Admin.java",
    ]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "aws-admin",
    srcs = glob([
        "admin/aws/*.java",
    ]),
    deps = [
        ":admin",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_amazonaws_aws_java_sdk_autoscaling",
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_ec2",
        "@maven//:com_amazonaws_aws_java_sdk_secretsmanager",
        "@maven//:com_amazonaws_aws_java_sdk_ssm",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "gcp-admin",
    srcs = glob([
        "admin/gcp/*.java",
    ]),
    deps = [
        ":admin",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "common-redis",
    srcs = glob([
        "common/redis/*.java",
    ]),
    deps = [
        ":common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
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
        ":operations",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_failureaccess",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "server-instance",
    srcs = [
        "instance/AbstractServerInstance.java",
        "instance/GetDirectoryFunction.java",
        "instance/OperationsMap.java",
        "instance/WatchFuture.java",
    ],
    deps = [
        ":ac",
        ":cas",
        ":common",
        ":instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_netty_netty_codec_http",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/java/build/buildfarm:operations",
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
        ":worker-queues",
        ":worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/java/build/buildfarm:operations",
    ],
)

java_library(
    name = "shard-instance",
    srcs = glob(["instance/shard/*.java"]),
    deps = [
        ":ac",
        ":common",
        ":common-cache",
        ":common-grpc",
        ":common-redis",
        ":instance",
        ":prometheus-metrics",
        ":server-instance",
        ":stub-instance",
        ":worker-queues",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_apache_commons_commons_pool2",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/java/build/buildfarm:operations",
         "//src/main/java/build/buildfarm:operations-finder",
    ],
)

java_library(
    name = "worker-queues",
    srcs = glob(["instance/memory/queues/*.java"]),
    deps = [
        ":ac",
        ":cas",
        ":common",
        ":instance",
        ":server-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "server",
    srcs = glob(["server/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        ":admin",
        ":aws-admin",
        ":aws-metrics",
        ":cas",
        ":common",
        ":common-grpc",
        ":gcp-admin",
        ":gcp-metrics",
        ":instance",
        ":log-metrics",
        ":memory-instance",
        ":metrics",
        ":prometheus-metrics",
        ":shard-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_longrunning_operations_java_grpc",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_services",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_threeten_threetenbp",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "@remote_apis//:build_bazel_semver_java_proto",
        "//src/main/java/build/buildfarm:operations",
    ],
)

java_binary(
    name = "buildfarm-server",
    classpath_resources = [
        ":configs",
    ],
    main_class = "build.buildfarm.server.BuildFarmServer",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":server",
    ],
    jvm_flags = ensure_accurate_metadata(),
)

container_image(
    name = "server.container",
    base = "@java_image_base//image",
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
    tags = ["container"],
    visibility = ["//visibility:public"],
)

java_library(
    name = "stub-instance",
    srcs = glob(["instance/stub/*.java"]),
    deps = [
        ":common",
        ":common-grpc",
        ":instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_longrunning_operations_java_grpc",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/java/build/buildfarm:operations",
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
        "prometheus-metrics",
        ":server",
        ":server-instance",
        ":shard-instance",
        ":stub-instance",
        ":worker",
        ":worker-cgroup",
        ":worker-resources",
        ":operations-finder",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/java/build/buildfarm:common-cache",
        "//src/main/java/build/buildfarm:operations",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_services",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_binary(
    name = "buildfarm-shard-worker",
    classpath_resources = [
        ":configs",
    ],
    main_class = "build.buildfarm.worker.shard.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":shard-worker",
    ],
    jvm_flags = ensure_accurate_metadata(),
)

container_image(
    name = "buildfarm-shard-worker.container",
    base = "@java_image_base//image",
    # leverage the implicit target of the buildfarm-shard-worker to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-shard-worker_deploy.jar",
    ],
    tags = ["container"],
    visibility = ["//visibility:public"],
)

java_library(
    name = "worker-cgroup",
    srcs = glob(["worker/cgroup/*.java"]),
    deps = [
        ":worker",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
    ],
)

java_library(
    name = "worker-resources",
    srcs = glob(["worker/resources/*.java"]),
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_googlecode_json_simple_json_simple",
    ],
)

java_library(
    name = "worker",
    srcs = glob(["worker/*.java"]),
    deps = [
        ":cas",
        ":common",
        ":instance",
        ":prometheus-metrics",
        ":stub-instance",
        ":worker-resources",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_github_jnr_jffi_native",
        "@maven//:com_github_jnr_jnr_constants",
        "@maven//:com_github_jnr_jnr_ffi",
        "@maven//:com_github_jnr_jnr_posix",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_github_serceman_jnr_fuse",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_library(
    name = "operationqueue-worker",
    srcs = glob(["worker/operationqueue/*.java"]),
    deps = [
        ":cas",
        ":common",
        ":common-grpc",
        ":instance",
        ":stub-instance",
        ":worker-resources",
        ":worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_library(
    name = "cas",
    srcs = glob(["cas/**/*.java"]),
    runtime_deps = [
        "@maven//:org_xerial_sqlite_jdbc",
    ],
    deps = [
        ":common",
        ":common-grpc",
        ":stub-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_github_jnr_jnr_ffi",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_library(
    name = "ac",
    srcs = glob(["ac/*.java"]),
    deps = [
        ":common",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
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
    jvm_flags = ensure_accurate_metadata(),
)

container_image(
    name = "buildfarm-operationqueue-worker.container",
    base = "@java_image_base//image",
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
    tags = ["container"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "bf-mount",
    srcs = ["Mount.java"],
    main_class = "build.buildfarm.Mount",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        ":worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_binary(
    name = "bf-cache-load",
    srcs = ["CacheLoad.java"],
    classpath_resources = [
        ":configs",
    ],
    main_class = "build.buildfarm.CacheLoad",
    visibility = ["//visibility:public"],
    deps = [
        ":cas",
        ":common",
        "@maven//:com_github_jnr_jnr_constants",
        "@maven//:com_github_jnr_jnr_ffi",
        "@maven//:com_github_jnr_jnr_posix",
        "@maven//:com_google_guava_guava",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_plugin(
    name = "io-benchmark-plugins",
    processor_class = "org.openjdk.jmh.generators.BenchmarkProcessor",
    deps = [
        "@maven//:org_openjdk_jmh_jmh_core",
        "@maven//:org_openjdk_jmh_jmh_generator_annprocess",
    ],
)

java_binary(
    name = "bf-executor",
    srcs = ["Executor.java"],
    main_class = "build.buildfarm.Executor",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":stub-instance",
        ":worker",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-extractor",
    srcs = ["Extract.java"],
    main_class = "build.buildfarm.Extract",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":common-grpc",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-cat",
    srcs = ["Cat.java"],
    classpath_resources = [
        ":configs",
    ],
    main_class = "build.buildfarm.Cat",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_binary(
    name = "bf-workerprofile",
    srcs = ["WorkerProfile.java"],
    classpath_resources = [
        ":configs",
    ],
    main_class = "build.buildfarm.WorkerProfile",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        ":shard-instance",
        ":shard-worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//third_party/jedis",
        "@maven//:com_google_guava_guava",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_binary(
    name = "GracefulShutdownTest",
    srcs = ["GracefulShutdownTest.java"],
    main_class = "build.buildfarm.GracefulShutdownTest",
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
    ],
)

java_binary(
    name = "bf-hist",
    srcs = ["Hist.java"],
    main_class = "build.buildfarm.Hist",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-index-worker",
    srcs = ["IndexWorker.java"],
    main_class = "build.buildfarm.IndexWorker",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-find-operations",
    srcs = ["FindOperations.java"],
    main_class = "build.buildfarm.FindOperations",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
         "//src/main/java/build/buildfarm:operations",
    ],
)

java_binary(
    name = "buildfarm-http-proxy",
    main_class = "build.buildfarm.proxy.http.HttpProxy",
    visibility = ["//visibility:public"],
    runtime_deps = [":http-proxy"],
)

java_library(
    name = "http-proxy",
    srcs = glob(["proxy/http/*.java"]),
    deps = [
        ":common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@googleapis//:google_rpc_status_java_proto",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_auth_google_auth_library_credentials",
        "@maven//:com_google_auth_google_auth_library_oauth2_http",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_auth",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_netty_netty_buffer",
        "@maven//:io_netty_netty_codec",
        "@maven//:io_netty_netty_codec_http",
        "@maven//:io_netty_netty_common",
        "@maven//:io_netty_netty_handler",
        "@maven//:io_netty_netty_transport",
        "@maven//:io_netty_netty_transport_native_epoll",
        "@maven//:io_netty_netty_transport_native_kqueue",
        "@maven//:io_netty_netty_transport_native_unix_common",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "bf-cancel",
    srcs = ["Cancel.java"],
    main_class = "build.buildfarm.Cancel",
    visibility = ["//visibility:public"],
    deps = [
        ":common",
        ":instance",
        ":stub-instance",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)
