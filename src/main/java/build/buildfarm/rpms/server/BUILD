load("@rules_pkg//pkg:rpm.bzl", "pkg_rpm")

pkg_rpm(
    name = "buildfarm-server-rpm",
    data = [
        ":server-service",
        "//examples:example_configs",
        "//examples:example_properties",
        "//src/main/java/build/buildfarm:buildfarm-server_deploy.jar",
    ],
    release = "0",
    spec_file = "buildfarm-server.spec",
    tags = ["manual"],
    version = "1.10.0",
)

filegroup(
    name = "server-service",
    srcs = ["buildfarm-server.service"],
)
