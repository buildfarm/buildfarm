"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

RULES_JVM_EXTERNAL_TAG = "4.2"
RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

def archive_dependencies(third_party):
    return [
        {
            "name": "rules_jvm_external",
            "strip_prefix": "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
            "sha256": RULES_JVM_EXTERNAL_SHA,
            "url": "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
        },

        # Kubernetes rules.  Useful for local development with tilt.
        {
            "name": "io_bazel_rules_k8s",
            "strip_prefix": "rules_k8s-0.5",
            "url": "https://github.com/bazelbuild/rules_k8s/archive/v0.5.tar.gz",
            "sha256": "773aa45f2421a66c8aa651b8cecb8ea51db91799a405bd7b913d77052ac7261a",
        },

        # Needed for "well-known protos" and @com_google_protobuf//:protoc.
        {
            "name": "com_google_protobuf",
            "sha256": "dd513a79c7d7e45cbaeaf7655289f78fd6b806e52dbbd7018ef4e3cf5cff697a",
            "strip_prefix": "protobuf-3.15.8",
            "urls": ["https://github.com/protocolbuffers/protobuf/archive/v3.15.8.zip"],
        },
        {
            "name": "com_github_bazelbuild_buildtools",
            "sha256": "a02ba93b96a8151b5d8d3466580f6c1f7e77212c4eb181cba53eb2cae7752a23",
            "strip_prefix": "buildtools-3.5.0",
            "urls": ["https://github.com/bazelbuild/buildtools/archive/3.5.0.tar.gz"],
        },

        # Needed for @grpc_java//compiler:grpc_java_plugin.
        {
            "name": "io_grpc_grpc_java",
            "sha256": "101b21af120901e9bf342384988f57af3332b59d997f64d5f41a1e24ffb96f19",
            "strip_prefix": "grpc-java-1.42.0",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.42.0.zip"],
        },

        # The APIs that we implement.
        {
            "name": "googleapis",
            "build_file": "%s:BUILD.googleapis" % third_party,
            "patch_cmds": ["find google -name 'BUILD.bazel' -type f -delete"],
            "patch_cmds_win": ["Remove-Item google -Recurse -Include *.bazel"],
            "sha256": "745cb3c2e538e33a07e2e467a15228ccbecadc1337239f6740d57a74d9cdef81",
            "strip_prefix": "googleapis-6598bb829c9e9a534be674649ffd1b4671a821f9",
            "url": "https://github.com/googleapis/googleapis/archive/6598bb829c9e9a534be674649ffd1b4671a821f9.zip",
        },
        {
            "name": "remote_apis",
            "build_file": "%s:BUILD.remote_apis" % third_party,
            "patch_args": ["-p1"],
            "patches": ["%s/remote-apis:remote-apis.patch" % third_party],
            "sha256": "743d2d5b5504029f3f825beb869ce0ec2330b647b3ee465a4f39ca82df83f8bf",
            "strip_prefix": "remote-apis-636121a32fa7b9114311374e4786597d8e7a69f3",
            "url": "https://github.com/bazelbuild/remote-apis/archive/636121a32fa7b9114311374e4786597d8e7a69f3.zip",
        },
        {
            "name": "rules_cc",
            "sha256": "34b2ebd4f4289ebbc27c7a0d854dcd510160109bb0194c0ba331c9656ffcb556",
            "strip_prefix": "rules_cc-daf6ace7cfeacd6a83e9ff2ed659f416537b6c74",
            "url": "https://github.com/bazelbuild/rules_cc/archive/daf6ace7cfeacd6a83e9ff2ed659f416537b6c74.tar.gz",
        },

        # Used to format proto files
        {
            "name": "com_grail_bazel_toolchain",
            "sha256": "98e9fe7bcd2035164efa948ba91ea01ccbc4f933fd19f0320c9cbc536cfe22b7",
            "strip_prefix": "bazel-toolchain-f14a8a5de8f7e98a011a52163d4855572c07a1a3",
            "url": "https://github.com/grailbio/bazel-toolchain/archive/f14a8a5de8f7e98a011a52163d4855572c07a1a3.tar.gz",
        },
        {
            "name": "io_bazel_rules_docker",
            "sha256": "59536e6ae64359b716ba9c46c39183403b01eabfbd57578e84398b4829ca499a",
            "strip_prefix": "rules_docker-0.22.0",
            "urls": ["https://github.com/bazelbuild/rules_docker/releases/download/v0.22.0/rules_docker-v0.22.0.tar.gz"],
        },

        # Bazel is referenced as a dependency so that buildfarm can access the linux-sandbox as a potential execution wrapper.
        {
            "name": "bazel",
            "sha256": "bca2303a43c696053317a8c7ac09a5e6d90a62fec4726e55357108bb60d7a807",
            "strip_prefix": "bazel-3.7.2",
            "urls": ["https://github.com/bazelbuild/bazel/archive/3.7.2.tar.gz"],
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

def buildfarm_dependencies(repository_name = "build_buildfarm"):
    """
    Define all 3rd party archive rules for buildfarm

    Args:
      repository_name: the name of the repository
    """
    third_party = "@%s//third_party" % repository_name
    for dependency in archive_dependencies(third_party):
        params = {}
        params.update(**dependency)
        name = params.pop("name")
        maybe(http_archive, name, **params)

    # Enhanced jedis 3.2.0 containing several convenience, performance, and
    # robustness changes.
    # Notable features include:
    #   Cluster request pipelining, used for batching requests for operation
    #   monitors and CAS index.
    #   Blocking request (b* prefix) interruptibility, using client
    #   connection reset.
    #   Singleton-redis-as-cluster - support treating a non-clustered redis
    #   endpoint as a cluster of 1 node.
    # Other changes are redis version-forward treatment of spop and visibility
    # into errors in cluster unreachable and cluster retry exhaustion.
    # Details at https://github.com/werkt/jedis/releases/tag/3.2.0-e82e68e2f7
    maybe(
        http_jar,
        "jedis",
        sha256 = "294ff5e4e6ae3fda5ff00f0a3c398fa50c1ffa3bc9313800b32e34a75fbb93f3",
        urls = [
            "https://github.com/werkt/jedis/releases/download/3.2.0-e82e68e2f7/jedis-3.2.0-e82e68e2f7.jar",
        ],
    )

    maybe(
        http_jar,
        "opentelemetry",
        sha256 = "0523287984978c091be0d22a5c61f0bce8267eeafbbae58c98abaf99c9396832",
        urls = [
            "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.11.0/opentelemetry-javaagent.jar",
        ],
    )

    http_file(
        name = "tini",
        urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini"],
    )
