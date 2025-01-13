load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "prometheus",
    srcs = [
        "PrometheusPublisher.java",
    ],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:io_prometheus_simpleclient",
        "@maven//:io_prometheus_simpleclient_hotspot",
        "@maven//:io_prometheus_simpleclient_httpserver",
        "@maven//:org_projectlombok_lombok",
    ],
)
