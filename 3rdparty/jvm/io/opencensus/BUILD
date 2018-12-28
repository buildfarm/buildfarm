java_library(
    name = "opencensus_api",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/opencensus/opencensus_api",
    ],
)

java_library(
    name = "opencensus_contrib_grpc_metrics",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/opencensus/opencensus_contrib_grpc_metrics",
    ],
    runtime_deps = [
        ":opencensus_api",
    ],
)
