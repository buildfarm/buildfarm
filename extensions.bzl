"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file", "http_jar")

def archive_dependencies(third_party):
    return [
        # Bazel is referenced as a dependency so that buildfarm can access the linux-sandbox as a potential execution wrapper.
        {
            "name": "bazel",
            "sha256": "03c2a5cfaeb7af45666f77bd7b56d768cb684551925ace1edd96d419f3d53260",
            "strip_prefix": "bazel-b4216efd8c13c564e92115dae25dd6620423bac1",
            "urls": ["https://github.com/bazelbuild/bazel/archive/b4216efd8c13c564e92115dae25dd6620423bac1.tar.gz"],
            "patch_args": ["-p1"],
            "patches": ["%s/bazel:bazel_visibility.patch" % third_party],
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
