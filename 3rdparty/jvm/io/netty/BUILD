java_library(
    name = "netty_buffer",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_buffer",
    ],
    runtime_deps = [
        ":netty_common",
    ],
)

java_library(
    name = "netty_codec",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_codec",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_codec_http",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_codec_http",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_handler",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_codec_http2",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_codec_http2",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_codec_http",
        ":netty_common",
        ":netty_handler",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_codec_socks",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_codec_socks",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_common",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_common",
    ],
)

java_library(
    name = "netty_handler",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_handler",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_handler_proxy",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_handler_proxy",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_codec_http",
        ":netty_codec_socks",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_resolver",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_resolver",
    ],
    runtime_deps = [
        ":netty_common",
    ],
)

java_library(
    name = "netty_transport",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_resolver",
    ],
)

java_library(
    name = "netty_transport_native_epoll",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_native_epoll",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_transport_native_kqueue",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_native_kqueue",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
        ":netty_transport_native_unix_common",
    ],
)

java_library(
    name = "netty_transport_native_unix_common",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_native_unix_common",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
    ],
)
