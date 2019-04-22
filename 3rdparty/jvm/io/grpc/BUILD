java_library(
    name = "grpc_all",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_all",
    ],
    runtime_deps = [
        ":grpc_auth",
        ":grpc_context",
        ":grpc_core",
        ":grpc_netty",
        ":grpc_okhttp",
        ":grpc_protobuf",
        ":grpc_protobuf_nano",
        ":grpc_stub",
        ":grpc_testing",
    ],
)

java_library(
    name = "grpc_auth",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_auth",
    ],
    runtime_deps = [
        ":grpc_core",
        "//3rdparty/jvm/com/google/auth:google_auth_library_credentials",
    ],
)

java_library(
    name = "grpc_context",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_context",
    ],
)

java_library(
    name = "grpc_core",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_core",
    ],
    runtime_deps = [
        ":grpc_context",
        "//3rdparty/jvm/com/google/android:annotations",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/code/gson",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/io/opencensus:opencensus_api",
        "//3rdparty/jvm/io/opencensus:opencensus_contrib_grpc_metrics",
        "//3rdparty/jvm/org/codehaus/mojo:animal_sniffer_annotations",
    ],
)

java_library(
    name = "grpc_netty",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_netty",
    ],
    runtime_deps = [
        ":grpc_core",
        "//3rdparty/jvm/io/netty:netty_codec_http2",
        "//3rdparty/jvm/io/netty:netty_handler_proxy",
    ],
)

java_library(
    name = "grpc_okhttp",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_okhttp",
    ],
    runtime_deps = [
        ":grpc_core",
        "//3rdparty/jvm/com/squareup/okhttp",
        "//3rdparty/jvm/com/squareup/okio",
    ],
)

java_library(
    name = "grpc_protobuf",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_protobuf",
    ],
    runtime_deps = [
        ":grpc_core",
        ":grpc_protobuf_lite",
        "//3rdparty/jvm/com/google/api/grpc:proto_google_common_protos",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
    ],
)

java_library(
    name = "grpc_protobuf_lite",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_lite",
    ],
    runtime_deps = [
        ":grpc_core",
        "//3rdparty/jvm/com/google/guava",
    ],
)

java_library(
    name = "grpc_protobuf_nano",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_nano",
    ],
    runtime_deps = [
        ":grpc_core",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf/nano:protobuf_javanano",
    ],
)

java_library(
    name = "grpc_stub",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_stub",
    ],
    runtime_deps = [
        ":grpc_core",
    ],
)

java_library(
    name = "grpc_testing",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_testing",
    ],
    runtime_deps = [
        ":grpc_core",
        ":grpc_stub",
        "//3rdparty/jvm/junit",
    ],
)
