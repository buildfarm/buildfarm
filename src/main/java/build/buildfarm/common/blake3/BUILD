package(
    default_visibility = ["//src:__subpackages__"],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src:__subpackages__"],
)

java_library(
    name = "blake3",
    srcs = glob(
        [
            "*.java",
        ],
    ),
    deps = [
        "//src/main/java/build/buildfarm/common/jni",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_code_findbugs_jsr305",
        "@com_google_errorprone_error_prone_annotations//jar"
    ],
)
