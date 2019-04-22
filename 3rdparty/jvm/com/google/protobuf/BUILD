java_library(
    name = "protobuf_java",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java",
    ],
)

java_library(
    name = "protobuf_java_util",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java_util",
    ],
    runtime_deps = [
        ":protobuf_java",
        "//3rdparty/jvm/com/google/code/gson",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava",
    ],
)
