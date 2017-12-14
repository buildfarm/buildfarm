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
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/instrumentation:instrumentation_api",
        "//3rdparty/jvm/io/opencensus:opencensus_api",
        "//3rdparty/jvm/io/opencensus:opencensus_contrib_grpc_metrics",
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
        "//3rdparty/jvm/io/netty:netty_codec_http2",
        "//3rdparty/jvm/io/netty:netty_handler_proxy",
    ],
)

java_library(
    name = "grpc_okhttp",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_okhttp",
    ],
    runtime_deps = [
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
        ":grpc_protobuf_lite",
        "//3rdparty/jvm/com/google/api/grpc:proto_google_common_protos",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
    ],
)

java_library(
    name = "grpc_protobuf_lite",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_lite",
    ],
)

java_library(
    name = "grpc_protobuf_nano",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_nano",
    ],
    runtime_deps = [
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
)
