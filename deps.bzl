"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

RULES_JVM_EXTERNAL_COMMIT = "560d150c19a22d0b7d76d476a477c3e4cb1694dc"
RULES_JVM_EXTERNAL_SHA = "4d4709ec5c5c31b1e8ab40bf1d49575d563008533f9967f4d0a3f5ca37a89e96"

def archive_dependencies(third_party):
    return [
        {
            "name": "platforms",
            "urls": [
                "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
                "https://github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
            ],
            "sha256": "5308fc1d8865406a49427ba24a9ab53087f17f5266a7aabbfc28823f3916e1ca",
        },
        {
            "name": "rules_jvm_external",
            "strip_prefix": "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_COMMIT,
            "sha256": RULES_JVM_EXTERNAL_SHA,
            "url": "https://github.com/bazelbuild/rules_jvm_external/archive/%s.tar.gz" % RULES_JVM_EXTERNAL_COMMIT,
        },
        {
            "name": "io_bazel_rules_go",
            "strip_prefix": "rules_go-de2074e0af1df1a34a862ca6108ed15f760520df",
            "sha256": "ea809c38ffa3e78c626d52cabe6c3ea23d6549494696f44775ed8248db051f9e",
            "url": "https://github.com/bazelbuild/rules_go/archive/de2074e0af1df1a34a862ca6108ed15f760520df.zip",
        },
        {
            "name": "bazel_gazelle",
            "strip_prefix": "bazel-gazelle-8adf04f8f7587ec64dc0d616b7b5243cf49b3c9d",
            "sha256": "019772279a0688f8f7c7c5f953ea82a3d380417027d59063f0797d660b0310b7",
            "url": "https://github.com/bazelbuild/bazel-gazelle/archive/8adf04f8f7587ec64dc0d616b7b5243cf49b3c9d.zip",
        },

        # Kubernetes rules.  Useful for local development with tilt.
        {
            "name": "io_bazel_rules_k8s",
            "strip_prefix": "rules_k8s-0.7",
            "url": "https://github.com/bazelbuild/rules_k8s/archive/refs/tags/v0.7.tar.gz",
            "sha256": "ce5b9bc0926681e2e7f2147b49096f143e6cbc783e71bc1d4f36ca76b00e6f4a",
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
            "sha256": "2484054e9ac47d3b4d4a797b9a0caaf4f50f23e13efb5b23ce3703b363f13023",
            "strip_prefix": "grpc-java-1.52.1",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.52.1.zip"],
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
            "sha256": "ee74a364a978fa3c85ea56d736010bfc44ea22b439691e9cefdf72284d6c9b93",
            "strip_prefix": "bazel-toolchain-d46339675a83e3517d955f5456e525501c3e05b8",
            "url": "https://github.com/grailbio/bazel-toolchain/archive/d46339675a83e3517d955f5456e525501c3e05b8.tar.gz",
            "patch_args": ["-p1"],
            "patches": ["%s:clang_toolchain.patch" % third_party],
        },
        {
            "name": "io_bazel_rules_docker",
            "sha256": "c7f9e97bb9f498f6bbbc842a0bcfaed1016b4355e9a7317f7ba0f413bc59ad71",
            "strip_prefix": "rules_docker-fc729d85f284225cfc0b8c6d1d838f4b3e037749",
            "urls": ["https://github.com/bazelbuild/rules_docker/archive/fc729d85f284225cfc0b8c6d1d838f4b3e037749.tar.gz"],
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
        {
            "name": "rules_oss_audit",
            "sha256": "02962810bcf82d0c66f929ccc163423f53773b8b154574ca956345523243e70d",
            "strip_prefix": "rules_oss_audit-1b2690cefd5a960c181e0d89bf3c076294a0e6f4",
            "url": "https://github.com/vmware/rules_oss_audit/archive/1b2690cefd5a960c181e0d89bf3c076294a0e6f4.zip",
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
    # Details at https://github.com/werkt/jedis/releases/tag/3.2.0-594c20da20
    maybe(
        http_jar,
        "jedis",
        sha256 = "72c749c02b775c0371cfc8ebcf713032910b7c6f365d958c3c000838f43f6a65",
        urls = [
            "https://github.com/werkt/jedis/releases/download/3.2.0-594c20da20/jedis-3.2.0-594c20da20.jar",
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
        sha256 = "12d20136605531b09a2c2dac02ccee85e1b874eb322ef6baf7561cd93f93c855",
        urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini"],
    )
