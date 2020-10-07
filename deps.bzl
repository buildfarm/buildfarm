"""
buildfarm dependencies that can be imported into other WORKSPACE files
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_jar")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

RULES_JVM_EXTERNAL_TAG = "3.3"
RULES_JVM_EXTERNAL_SHA = "d85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab"

def archive_dependencies(third_party):
    return [
        {
            "name": "rules_jvm_external",
            "strip_prefix": "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
            "sha256": RULES_JVM_EXTERNAL_SHA,
            "url": "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
        },

        # Needed for "well-known protos" and @com_google_protobuf//:protoc.
        {
            "name": "com_google_protobuf",
            "sha256": "1c744a6a1f2c901e68c5521bc275e22bdc66256eeb605c2781923365b7087e5f",
            "strip_prefix": "protobuf-3.13.0",
            "urls": ["https://github.com/protocolbuffers/protobuf/archive/v3.13.0.zip"],
        },

        # Needed for @grpc_java//compiler:grpc_java_plugin.
        {
            "name": "io_grpc_grpc_java",
            "patch_args": ["-p1"],
            "sha256": "2705d274ce79b324f3520414202481a09640b4b14e58d3124841b3318d9b6e19",
            "strip_prefix": "grpc-java-1.32.1",
            "urls": ["https://github.com/grpc/grpc-java/archive/v1.32.1.zip"],
        },

        # The APIs that we implement.
        {
            "name": "googleapis",
            "build_file": "%s:BUILD.googleapis" % third_party,
            "sha256": "7b6ea252f0b8fb5cd722f45feb83e115b689909bbb6a393a873b6cbad4ceae1d",
            "strip_prefix": "googleapis-143084a2624b6591ee1f9d23e7f5241856642f4d",
            "url": "https://github.com/googleapis/googleapis/archive/143084a2624b6591ee1f9d23e7f5241856642f4d.zip",
        },

        {
            "name": "remote_apis",
            "build_file": "%s:BUILD.remote_apis" % third_party,
            "patch_args": ["-p1"],
            "patches": ["%s/remote-apis:remote-apis.patch" % third_party],
            "sha256": "03433a21ed97517f0fbda03c759854850336775a22dc737bab918949ceeddac9",
            "strip_prefix": "remote-apis-f54876595da9f2c2d66c98c318d00b60fd64900b",
            "url": "https://github.com/bazelbuild/remote-apis/archive/f54876595da9f2c2d66c98c318d00b60fd64900b.zip",
        },

        # Download the rules_docker repository at release v0.14.4
        {
            "name": "io_bazel_rules_docker",
            "patch_args": ["-p1"],
            "patches": [
                "%s/rules_docker:rules_docker.patch" % third_party,
            ],
            "sha256": "4521794f0fba2e20f3bf15846ab5e01d5332e587e9ce81629c7f96c793bb7036",
            "strip_prefix": "rules_docker-0.14.4",
            "urls": ["https://github.com/bazelbuild/rules_docker/archive/v0.14.4.tar.gz"],
        },
    ]

def buildfarm_dependencies(repository_name="build_buildfarm"):
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

    maybe(
        http_jar,
        "jedis",
        sha256 = "294ff5e4e6ae3fda5ff00f0a3c398fa50c1ffa3bc9313800b32e34a75fbb93f3",
        urls = [
            "https://github.com/werkt/jedis/releases/download/3.2.0-e82e68e2f7/jedis-3.2.0-e82e68e2f7.jar",
        ])
