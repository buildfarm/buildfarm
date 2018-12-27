java_library(
    name = "google_auth_library_credentials",
    exports = [
        "//external:jar/com/google/auth/google_auth_library_credentials"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "google_auth_library_oauth2_http",
    exports = [
        "//external:jar/com/google/auth/google_auth_library_oauth2_http"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/google/guava:guava",
        "//3rdparty/jvm/com/google/http_client:google_http_client",
        "//3rdparty/jvm/com/google/http_client:google_http_client_jackson2",
        ":google_auth_library_credentials"
    ],
    visibility = [
        "//visibility:public"
    ]
)


