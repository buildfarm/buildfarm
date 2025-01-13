load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "resources",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_code_gson_gson",
        "@maven//:com_google_guava_guava",
        "@maven//:io_prometheus_simpleclient",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_projectlombok_lombok",
    ],
)
