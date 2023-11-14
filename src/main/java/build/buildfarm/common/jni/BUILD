load("@rules_java//java:defs.bzl", "java_library")

package(
    default_visibility = ["//src:__subpackages__"],
)

licenses(["notice"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src:__subpackages__"],
)

java_library(
    name = "jni",
    srcs = ["JniLoader.java"],
    resources = select({
        "@bazel//src/conditions:darwin": [
            "//src/main/native:libunix_jni.dylib",
        ],
        "@bazel//src/conditions:windows": [
            "//src/main/native/windows:windows_jni.dll",
        ],
        "//conditions:default": [
            "//src/main/native:libunix_jni.so",
        ],
    }),
    deps = [
        "//src/main/java/build/buildfarm/common/os",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_io_commons_io",
    ],
)
