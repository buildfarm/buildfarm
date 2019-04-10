workspace(name = "build_buildfarm")

load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_jar")

# Needed for "well-known protos" and @com_google_protobuf//:protoc.
http_archive(
    name = "com_google_protobuf",
    patch_args = ["-p1"],
    patches = [
        "//third_party/com_google_protobuf:b6375e03aa.patch",
        "//third_party/com_google_protobuf:7e1d9e419e.patch",
        "//third_party/com_google_protobuf:qualified_error_prone_annotations.patch",
    ],
    sha256 = "b50be32ea806bdb948c22595ba0742c75dc2f8799865def414cf27ea5706f2b7",
    strip_prefix = "protobuf-3.7.0",
    urls = ["https://github.com/google/protobuf/archive/v3.7.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Needed for @grpc_java//compiler:grpc_java_plugin.
http_archive(
    name = "io_grpc_grpc_java",
    patch_args = ["-p1"],
    patches = [
        "//third_party/io_grpc_grpc_java:054def3c63.patch",
        "//third_party/io_grpc_grpc_java:0959a846c8.patch",
        "//third_party/io_grpc_grpc_java:952a767b9c.patch",
        "//third_party/io_grpc_grpc_java:3c24dc6fe1.patch",
    ],
    sha256 = "f5d0bdebc2a50d0e28f0d228d6c35081d3e973e6159f2695aa5c8c7f93d1e4d6",
    strip_prefix = "grpc-java-1.19.0",
    urls = ["https://github.com/grpc/grpc-java/archive/v1.19.0.zip"],
)

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories(
    omit_com_google_guava = True,
    omit_com_google_guava_failureaccess = True
)

http_archive(
    name = "googleapis",
    sha256 = "7b6ea252f0b8fb5cd722f45feb83e115b689909bbb6a393a873b6cbad4ceae1d",
    url = "https://github.com/googleapis/googleapis/archive/143084a2624b6591ee1f9d23e7f5241856642f4d.zip",
    strip_prefix = "googleapis-143084a2624b6591ee1f9d23e7f5241856642f4d",
    build_file = "@build_buildfarm//:BUILD.googleapis",
)

# The API that we implement.
http_archive(
    name = "remote_apis",
    sha256 = "6f22ba09356f8dbecb87ba03cacf147939f77fef1c9cfaffb3826691f3686e9b",
    url = "https://github.com/bazelbuild/remote-apis/archive/cfe8e540cbb424e3ebc649ddcbc91190f70e23a6.zip",
    strip_prefix = "remote-apis-cfe8e540cbb424e3ebc649ddcbc91190f70e23a6",
    build_file = "@build_buildfarm//:BUILD.remote_apis",
)

http_archive(
    name = "bazel_skylib",
    sha256 = "eb5c57e4c12e68c0c20bc774bfbc60a568e800d025557bc4ea022c6479acc867",
    strip_prefix = "bazel-skylib-0.6.0",
    urls = ["https://github.com/bazelbuild/bazel-skylib/archive/0.6.0.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "aed1c249d4ec8f703edddf35cbe9dfaca0b5f5ea6e4cd9e83e99f3b0d1136c3d",
    strip_prefix = "rules_docker-0.7.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.7.0.tar.gz"],
)

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

load(
    "@io_bazel_rules_docker//java:image.bzl",
    _java_image_repos = "repositories",
)

http_jar(
    name = "jedis",
    url = "https://github.com/werkt/jedis/releases/download/3.0.1-2418e3e2c6/jedis-3.0.1-2418e3e2c6.jar",
    sha256 = "9af738bf341a5f34ed38a7ab5c7666f76d17624e29071b49cc2113f5acbf21e5",
)

bind(
    name = "jar/redis/clients/jedis",
    actual = "@jedis//jar",
)

_java_image_repos()

container_pull(
  name = "java_base",
  registry = "gcr.io",
  repository = "distroless/java",
  digest = "sha256:8c1769cb253bdecc257470f7fba05446a55b70805fa686f227a11655a90dfe9e",
)
