load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "prometheus",
    srcs = [
        "PrometheusPublisher.java",
    ],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:io_prometheus_prometheus_metrics_core",
        "@maven//:io_prometheus_prometheus_metrics_exporter_httpserver",
        "@maven//:io_prometheus_prometheus_metrics_instrumentation_jvm",
        "@maven//:org_projectlombok_lombok",
    ],
)
