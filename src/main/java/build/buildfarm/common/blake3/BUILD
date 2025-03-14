load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "blake3",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    resources = select({
        "@bazel//src/conditions:darwin": [
            "//src/main/native:libblake3_jni.dylib",
        ],
        "@bazel//src/conditions:windows": [
            "//src/main/native:blake3_jni.dll",
        ],
        "//conditions:default": [
            "//src/main/native:libblake3_jni.so",
        ],
    }),
    visibility = ["//src/main/java/build/buildfarm/common:__pkg__"],
    deps = [
        "//src/main/java/build/buildfarm/common/base",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_projectlombok_lombok",
    ],
)
