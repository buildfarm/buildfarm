workspace(name = "build_buildfarm")

load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_jar")

# Needed for "well-known protos" and @com_google_protobuf//:protoc.
http_archive(
    name = "com_google_protobuf",
    patch_args = ["-p1"],
    patches = [
        "//third_party/com_google_protobuf:six.patch",
    ],
    sha256 = "cfcba2df10feec52a84208693937c17a4b5df7775e1635c1e3baffc487b24c9b",
    strip_prefix = "protobuf-3.9.2",
    urls = ["https://github.com/google/protobuf/archive/v3.9.2.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Needed for @grpc_java//compiler:grpc_java_plugin.
http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "b1dcce395bdb6c620d3142597b5017f7175c527b0f9ae46c456726940876347e",
    strip_prefix = "grpc-java-1.23.0",
    urls = ["https://github.com/grpc/grpc-java/archive/v1.23.0.zip"],
)

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories(
    omit_com_google_guava = True,
    omit_com_google_guava_failureaccess = True,
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
    build_file = "@build_buildfarm//:BUILD.remote_apis",
    sha256 = "69c47e26efbfac605e07d9963fdeda9f5ed22a73e692f290a0b6150207207cb5",
    strip_prefix = "remote-apis-e7282cf0f0e16e7ba84209be5417279e6815bee7",
    url = "https://github.com/bazelbuild/remote-apis/archive/e7282cf0f0e16e7ba84209be5417279e6815bee7.zip",
)

load("@remote_apis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "bazel_remote_apis_imports",
    java = True,
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
    sha256 = "9ff889216e28c918811b77999257d4ac001c26c1f7c7fb17a79bc28abf74182e",
    strip_prefix = "rules_docker-0.10.1",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.10.1/rules_docker-v0.10.1.tar.gz"],
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

http_jar(
    name = "jedis",
    sha256 = "10c844cb3338884da468608f819c11d5c90354b170c3fe445203497000c06ba3",
    urls =  [
        "https://github.com/werkt/jedis/releases/download/jedis-3.0.1-8209fd5a88/jedis-3.0.1-8209fd5a88.jar",
    ],
)

bind(
    name = "jar/redis/clients/jedis",
    actual = "@jedis//jar",
)

_java_image_repos()

container_pull(
    name = "java_base",
    digest = "sha256:8c1769cb253bdecc257470f7fba05446a55b70805fa686f227a11655a90dfe9e",
    registry = "gcr.io",
    repository = "distroless/java",
)
