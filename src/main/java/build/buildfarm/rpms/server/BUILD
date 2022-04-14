package(default_visibility = ["//visibility:public"])
load("@rules_pkg//:rpm.bzl", "pkg_rpm")
load("@rules_pkg//:mappings.bzl", "pkg_filegroup", "pkg_files")


pkg_rpm(
    name = "buildfarm-server-rpm",
    data = [
        ":server-service",
        "//examples:example_configs",
        "//src/main/java/build/buildfarm:buildfarm-server_deploy.jar",
        "//src/main/java/build/buildfarm:configs",
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





pkg_rpm(
    name = "buildfarm-server-rpm2",
    data = [
        ":buildfarm-server-filegroup"
    ],
    release = "1",
    spec_file = "buildfarm-server.spec",
    tags = ["manual"],
    version = "1.10.0",
)


pkg_filegroup(
    name = "buildfarm-server-filegroup",
    srcs = [":buildfarm-server-files"],
)

pkg_files(
    name = "buildfarm-server-files",
    srcs = [
        "//src/main/java/build/buildfarm:buildfarm-server",
    ],
    prefix = "/usr/lib/oss-audit-example",
)