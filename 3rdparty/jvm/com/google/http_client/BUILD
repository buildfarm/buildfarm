java_library(
    name = "google_http_client",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/google/http_client/google_http_client",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/org/apache/httpcomponents:httpclient",
    ],
)

java_library(
    name = "google_http_client_jackson2",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/google/http_client/google_http_client_jackson2",
    ],
    runtime_deps = [
        ":google_http_client",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
    ],
)
