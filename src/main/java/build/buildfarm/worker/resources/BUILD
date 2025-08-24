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
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_code_gson_gson",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:io_prometheus_simpleclient",
        "@buildfarm_maven//:org_apache_commons_commons_lang3",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
