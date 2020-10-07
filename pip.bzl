load("@io_bazel_rules_docker//repositories:pip_repositories.bzl", "pip_deps")

def buildfarm_pip():

    pip_deps()