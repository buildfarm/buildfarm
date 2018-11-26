java_library(
    name = "protobuf_java",
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "protobuf_java_util",
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java_util"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/code/gson:gson",
        "//3rdparty/jvm/com/google/guava:guava",
        ":protobuf_java"
    ],
    visibility = [
        "//visibility:public"
    ]
)


