"""Rules for ENV"""

def _oci_image_env_impl(ctx):
    """
    Helper method to write out a "key=value" pair on sepaarate lines. This file is fed into oci_image() in the `env` kwargs.
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
