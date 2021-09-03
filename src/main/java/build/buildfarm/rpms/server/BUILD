load("@rules_pkg//:rpm.bzl", "pkg_rpm")

pkg_rpm(
    name = "buildfarm-server-rpm",
    data = [
       "//examples:example_configs",
       "//src/main/java/build/buildfarm:buildfarm-server_deploy.jar",
       "//src/main/java/build/buildfarm:configs",
       ":server-service"
    ],
    release = "0",
    spec_file = "buildfarm-server.spec",
    version = "1.10.0",
)

filegroup(
    name = "server-service",
    srcs = ["buildfarm-server.service"]
)
