load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "prometheus",
    srcs = [
        "PrometheusPublisher.java",
    ],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "@buildfarm_maven//:io_prometheus_simpleclient",
        "@buildfarm_maven//:io_prometheus_simpleclient_hotspot",
        "@buildfarm_maven//:io_prometheus_simpleclient_httpserver",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
