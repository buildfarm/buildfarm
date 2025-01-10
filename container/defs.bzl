"""Rules for ENV"""

load("@aspect_bazel_lib//lib:transitions.bzl", "platform_transition_filegroup")
load("@rules_oci//oci:defs.bzl", "oci_image_index", "oci_push")

def _oci_image_env_impl(ctx):
    """
    Helper method to write out a "key=value" pair on separate lines. This file is fed into oci_image() in the `env` kwargs.
    """
    envs = {
        "CONFIG_PATH": ctx.attr.configpath,
        "JAVA_TOOL_OPTIONS": " ".join(ctx.attr.jvm_args),
    }
    builder = ctx.actions.declare_file("_%s.env.txt" % ctx.label.name)
    ctx.actions.write(
        output = builder,
        content = "\n".join(["{}={}".format(key, value) for (key, value) in envs.items()]),
    )
    return [
        DefaultInfo(
            files = depset([builder]),
        ),
    ]

oci_image_env = rule(
    implementation = _oci_image_env_impl,
    attrs = {
        "configpath": attr.string(mandatory = True),
        "jvm_args": attr.string_list(mandatory = True, allow_empty = False),
    },
)

def multiarch_oci_image(name, image):
    for arch in ["amd64", "arm64"]:
        platform_transition_filegroup(
            name = "_%s.transitioned.%s" % (name, arch),
            srcs = [image],
            target_platform = "@hermetic_cc_toolchain//toolchain/platform:linux_" + arch,
            tags = ["container"],
        )

    oci_image_index(
        name = name,
        images = [
            "_%s.transitioned.%s" % (name, arch)
            for arch in [
                "amd64",
                "arm64",
            ]
        ],
        tags = ["container"],
    )

    # Below targets push public docker images to bazelbuild dockerhub.
    oci_push(
        name = "public_push_%s" % name,
        image = name,
        repository = "index.docker.io/bazelbuild/%s" % name,
        # Specify the tag with `bazel run public_push_buildfarm-server public_push_buildfarm-worker -- --tag latest`
        tags = ["container"],
    )
