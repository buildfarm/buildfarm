"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")

def archive_dependencies(third_party):
    return [
        # Bazel is referenced as a dependency so that buildfarm can access the linux-sandbox as a potential execution wrapper.
        {
            "name": "bazel",
            "sha256": "de54ed570e59445246bed5f44c16863ca83646c9cbd0ceaf4ca1e4ce9581f805",
            "strip_prefix": "bazel-7.3.2",
            "urls": ["https://github.com/bazelbuild/bazel/archive/refs/tags/7.3.2.tar.gz"],
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

def _buildfarm_extension_impl(_ctx):
    """
    Define all 3rd party archive rules for buildfarm
    """
    third_party = "//third_party"
    for dependency in archive_dependencies(third_party):
        params = {}
        params.update(**dependency)
        http_archive(**params)

    http_jar(
        name = "opentelemetry",
        sha256 = "16f8e28fa1ddcd56ed85bf633bd1d1fbc78ea7c4cc50e8c5726b2a319f5058c8",
        urls = [
            "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.14.0/opentelemetry-javaagent.jar",
        ],
    )

    http_file(
        name = "tini",
        sha256 = "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini"],
    )
    http_file(
        name = "tini_arm64v8",
        sha256 = "07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81",
        urls = ["https://github.com/krallin/tini/releases/download/v0.19.0/tini-arm64"],
    )

build_deps = module_extension(
    implementation = _buildfarm_extension_impl,
)
