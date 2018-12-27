java_library(
    name = "netty_buffer",
    exports = [
        "//external:jar/io/netty/netty_buffer"
    ],
    runtime_deps = [
        ":netty_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_codec",
    exports = [
        "//external:jar/io/netty/netty_codec"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/github/jponge:lzma_java",
        "//3rdparty/jvm/com/google/protobuf/nano:protobuf_javanano",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/jcraft:jzlib",
        "//3rdparty/jvm/com/ning:compress_lzf",
        "//3rdparty/jvm/net/jpountz/lz4:lz4",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_codec_http",
    exports = [
        "//external:jar/io/netty/netty_codec_http"
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/jcraft:jzlib",
        ":netty_codec"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_codec_http2",
    exports = [
        "//external:jar/io/netty/netty_codec_http2"
    ],
    runtime_deps = [
        ":netty_codec_http",
        ":netty_handler"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_codec_socks",
    exports = [
        "//external:jar/io/netty/netty_codec_socks"
    ],
    runtime_deps = [
        ":netty_codec"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_common",
    exports = [
        "//external:jar/io/netty/netty_common"
    ],
    runtime_deps = [
        "//3rdparty/jvm/commons_logging:commons_logging",
        "//3rdparty/jvm/org/apache/logging/log4j:log4j_api",
        "//3rdparty/jvm/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_handler",
    exports = [
        "//external:jar/io/netty/netty_handler"
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_handler_proxy",
    exports = [
        "//external:jar/io/netty/netty_handler_proxy"
    ],
    runtime_deps = [
        ":netty_codec_http",
        ":netty_codec_socks",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_resolver",
    exports = [
        "//external:jar/io/netty/netty_resolver"
    ],
    runtime_deps = [
        ":netty_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_transport",
    exports = [
        "//external:jar/io/netty/netty_transport"
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_resolver"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_transport_native_epoll",
    exports = [
        "//external:jar/io/netty/netty_transport_native_epoll"
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
        ":netty_transport_native_unix_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_transport_native_kqueue",
    exports = [
        "//external:jar/io/netty/netty_transport_native_kqueue"
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
        ":netty_transport_native_unix_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty_transport_native_unix_common",
    exports = [
        "//external:jar/io/netty/netty_transport_native_unix_common"
    ],
    runtime_deps = [
        ":netty_common",
        ":netty_transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)


