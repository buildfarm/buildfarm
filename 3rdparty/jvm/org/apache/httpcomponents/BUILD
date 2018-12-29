java_library(
    name = "httpclient",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/apache/httpcomponents/httpclient",
    ],
    runtime_deps = [
        ":httpcore",
        "//3rdparty/jvm/commons_codec",
        "//3rdparty/jvm/commons_logging",
    ],
)

java_library(
    name = "httpcore",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore",
    ],
)
