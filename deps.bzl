"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

RULES_JVM_EXTERNAL_TAG = "5.3"
RULES_JVM_EXTERNAL_SHA = "d31e369b854322ca5098ea12c69d7175ded971435e55c18dd9dd5f29cc5249ac"

def archive_dependencies(third_party):
    return [
        {
            "name": "platforms",
            "urls": [
                "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.7/platforms-0.0.7.tar.gz",
                "https://github.com/bazelbuild/platforms/releases/download/0.0.7/platforms-0.0.7.tar.gz",
            ],
            "sha256": "3a561c99e7bdbe9173aa653fd579fe849f1d8d67395780ab4770b1f381431d51",
        },
        {
            "name": "rules_jvm_external",
            "strip_prefix": "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
            "sha256": RULES_JVM_EXTERNAL_SHA,
            "url": "https://github.com/bazelbuild/rules_jvm_external/releases/download/%s/rules_jvm_external-%s.tar.gz" % (RULES_JVM_EXTERNAL_TAG, RULES_JVM_EXTERNAL_TAG),
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
            "sha256": "25f1292d4ea6666f460a2a30038eef121e6c3937ae0f61d610611dfb14b0bd32",
            "strip_prefix": "protobuf-3.19.1",
            "urls": ["https://github.com/protocolbuffers/protobuf/archive/v3.19.1.zip"],
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
            "sha256": "b8fb7ae4824fb5a5ae6e6fa26ffe2ad7ab48406fdeee54e8965a3b5948dd957e",
            "strip_prefix": "grpc-java-1.56.1",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.56.1.zip"],
        },
        {
            "name": "rules_pkg",
            "sha256": "335632735e625d408870ec3e361e192e99ef7462315caa887417f4d88c4c8fb8",
            "urls": [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
                "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
            ],
        },
        {
            "name": "rules_license",
            "sha256": "6157e1e68378532d0241ecd15d3c45f6e5cfd98fc10846045509fb2a7cc9e381",
            "urls": [
                "https://github.com/bazelbuild/rules_license/releases/download/0.0.4/rules_license-0.0.4.tar.gz",
                "https://mirror.bazel.build/github.com/bazelbuild/rules_license/releases/download/0.0.4/rules_license-0.0.4.tar.gz",
            ],
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
            "sha256": "2037875b9a4456dce4a79d112a8ae885bbc4aad968e6587dca6e64f3a0900cdf",
            "strip_prefix": "rules_cc-0.0.9",
            "url": "https://github.com/bazelbuild/rules_cc/releases/download/0.0.9/rules_cc-0.0.9.tar.gz",
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

        # Used to build release container images
        {
            "name": "io_bazel_rules_docker",
            "sha256": "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
            "urls": ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
            "patch_args": ["-p0"],
            "patches": ["%s:docker_go_toolchain.patch" % third_party],
        },

        # Updated versions of io_bazel_rules_docker dependencies for bazel compatibility
        {
            "name": "io_bazel_rules_go",
            "sha256": "278b7ff5a826f3dc10f04feaf0b70d48b68748ccd512d7f98bf442077f043fe3",
            "urls": [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
                "https://github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
            ],
        },
        {
            "name": "bazel_gazelle",
            "sha256": "d3fa66a39028e97d76f9e2db8f1b0c11c099e8e01bf363a923074784e451f809",
            "urls": ["https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.33.0/bazel-gazelle-v0.33.0.tar.gz"],
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
