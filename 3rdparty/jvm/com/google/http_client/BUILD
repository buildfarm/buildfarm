java_library(
    name = "google_http_client",
    exports = [
        "//external:jar/com/google/http_client/google_http_client"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/org/apache/httpcomponents:httpclient"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)



java_library(
    name = "google_http_client_jackson2",
    exports = [
        "//external:jar/com/google/http_client/google_http_client_jackson2"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        ":google_http_client"
    ],
    visibility = [
        "//3rdparty/jvm:__subpackages__"
    ]
)


