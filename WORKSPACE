workspace(name = "build_buildfarm")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")

# Needed for "well-known protos" and @com_google_protobuf//:protoc.
http_archive(
    name = "com_google_protobuf",
    sha256 = "33cba8b89be6c81b1461f1c438424f7a1aa4e31998dbe9ed6f8319583daac8c7",
    strip_prefix = "protobuf-3.10.0",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.10.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Needed for @grpc_java//compiler:grpc_java_plugin.
http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "11f2930cf31c964406e8a7e530272a263fbc39c5f8d21410b2b927b656f4d9be",
    strip_prefix = "grpc-java-1.26.0",
    urls = ["https://github.com/grpc/grpc-java/archive/v1.26.0.zip"],
)

http_archive(
    name = "googleapis",
    build_file = "@build_buildfarm//:BUILD.googleapis",
    sha256 = "7b6ea252f0b8fb5cd722f45feb83e115b689909bbb6a393a873b6cbad4ceae1d",
    strip_prefix = "googleapis-143084a2624b6591ee1f9d23e7f5241856642f4d",
    url = "https://github.com/googleapis/googleapis/archive/143084a2624b6591ee1f9d23e7f5241856642f4d.zip",
)

# The API that we implement.
http_archive(
    name = "remote_apis",
    patches = ["@build_buildfarm//third_party/remote-apis:remote-apis.patch"],
    patch_args = ["-p1"],
    build_file = "@build_buildfarm//:BUILD.remote_apis",
    sha256 = "21ad15be502ef529ca07fdda56d25d6678647b954d41f08a040241ea5e43dce1",
    strip_prefix = "remote-apis-b5123b1bb2853393c7b9aa43236db924d7e32d61",
    url = "https://github.com/bazelbuild/remote-apis/archive/b5123b1bb2853393c7b9aa43236db924d7e32d61.zip",
)

http_archive(
    name = "bazel_skylib",
    sha256 = "2ea8a5ed2b448baf4a6855d3ce049c4c452a6470b1efd1504fdb7c1c134d220a",
    strip_prefix = "bazel-skylib-0.8.0",
    url = "https://github.com/bazelbuild/bazel-skylib/archive/0.8.0.tar.gz",
)

# Download the rules_docker repository at release v0.10.1
http_archive(
    name = "io_bazel_rules_docker",
    patches = ["@build_buildfarm//third_party/rules_docker:rules_docker.patch"],
    patch_args = ["-p1"],
    sha256 = "df13123c44b4a4ff2c2f337b906763879d94871d16411bf82dcfeba892b58607",
    strip_prefix = "rules_docker-0.13.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.13.0.tar.gz"],
)

http_jar(
    name = "jedis",
    sha256 = "10c844cb3338884da468608f819c11d5c90354b170c3fe445203497000c06ba3",
    urls = [
        "https://github.com/werkt/jedis/releases/download/jedis-3.0.1-8209fd5a88/jedis-3.0.1-8209fd5a88.jar",
    ],
)

load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories(
    omit_com_google_guava = True,
    omit_com_google_guava_failureaccess = True,
)

load("@remote_apis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "bazel_remote_apis_imports",
    java = True,
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")
load(
    "@io_bazel_rules_docker//java:image.bzl",
    _java_image_repos = "repositories",
)

_java_image_repos()

container_pull(
    name = "java_base",
    digest = "sha256:8c1769cb253bdecc257470f7fba05446a55b70805fa686f227a11655a90dfe9e",
    registry = "gcr.io",
    repository = "distroless/java",
)

bind(
    name = "jar/redis/clients/jedis",
    actual = "@jedis//jar",
)
