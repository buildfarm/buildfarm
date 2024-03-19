"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")

def archive_dependencies(third_party):
    return [
        # Needed for "well-known protos" and @com_google_protobuf//:protoc.
        # {
        #     "name": "com_google_protobuf",
        #     "sha256": "79082dc68d8bab2283568ce0be3982b73e19ddd647c2411d1977ca5282d2d6b3",
        #     "strip_prefix": "protobuf-25.0",
        #     "urls": ["https://github.com/protocolbuffers/protobuf/archive/v25.0.zip"],
        # },
        # Needed for @grpc_java//compiler:grpc_java_plugin.
        {
            "name": "io_grpc_grpc_java",
            "sha256": "5d617856c295d863307f4036a1b1e93f9eeaf6da41424d2de7c9b330a810fc3b",
            "strip_prefix": "grpc-java-1.62.2",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.62.2.zip"],
            # Bzlmod: Waiting for https://github.com/bazelbuild/bazel-central-registry/issues/353
        },
        # The APIs that we implement.
        {
            "name": "googleapis",
            "build_file": "%s:BUILD.googleapis" % third_party,
            "patch_cmds": ["find google -name 'BUILD.bazel' -type f -delete"],
            "patch_cmds_win": ["Remove-Item google -Recurse -Include *.bazel"],
            "sha256": "1980dc4a4d02420d4da588665e3ecbe55e02a1c2e32d8906a2268c67d1085e0b",
            "strip_prefix": "googleapis-5f8a02d6b7e77bd26e0375a00ca20eb2f3ee1ba2",
            "url": "https://github.com/googleapis/googleapis/archive/5f8a02d6b7e77bd26e0375a00ca20eb2f3ee1ba2.zip",
        },

        # Bazel is referenced as a dependency so that buildfarm can access the linux-sandbox as a potential execution wrapper.
        {
            "name": "bazel",
            "sha256": "06d3dbcba2286d45fc6479a87ccc649055821fc6da0c3c6801e73da780068397",
            "strip_prefix": "bazel-6.0.0",
            "urls": ["https://github.com/bazelbuild/bazel/archive/refs/tags/6.0.0.tar.gz"],
            "patch_args": ["-p1"],
            "patches": ["%s/bazel:bazel_visibility.patch" % third_party],
        },

        # Optional execution wrappers
        {
            "name": "skip_sleep",
            "build_file": "%s:BUILD.skip_sleep" % third_party,
            "sha256": "03980702e8e9b757df68aa26493ca4e8573770f15dd8a6684de728b9cb8549f1",
            "strip_prefix": "TARDIS-f54fa4743e67763bb1ad77039b3d15be64e2e564",
            "url": "https://github.com/Unilang/TARDIS/archive/f54fa4743e67763bb1ad77039b3d15be64e2e564.zip",
        },
    ]

def _buildfarm_extension_impl(ctx):
    """
    Define all 3rd party archive rules for buildfarm

    Args:
      repository_name: the name of the repository
    """
    repository_name = "@build_buildfarm"
    third_party = "//third_party"
    for dependency in archive_dependencies(third_party):
        params = {}
        params.update(**dependency)
        http_archive(**params)

    http_jar(
        name = "opentelemetry",
        sha256 = "eccd069da36031667e5698705a6838d173d527a5affce6cc514a14da9dbf57d7",
        urls = [
            "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.28.0/opentelemetry-javaagent.jar",
        ],
    )

    http_file(
        name = "tini",
        sha256 = "12d20136605531b09a2c2dac02ccee85e1b874eb322ef6baf7561cd93f93c855",
        urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini"],
    )

build_deps = module_extension(
    implementation = _buildfarm_extension_impl,
)
