"""
Make the values of certain environment variables available to the rest
of the build as function values.

This is a way to pass parameters into the build that bazel macros can
consume.  We cannot use Starlark's user defined build settings for this,
as these are themselves bazel rules, so their values are not available
when bazel processes macros.  To get around this limitation, we here
define a custom repository rule that dynamically generates bazel macros
based on the values of environment variables.
"""

def _many_params_impl(repository_ctx):
    params = [
        {
            "path": "cc-binaries.bzl",
            "name": "cc_binaries",
            "value": repository_ctx.os.environ.get("MANY_CC_BINARIES", "1"),
        },
        {
            "path": "cc-libraries.bzl",
            "name": "cc_libraries",
            "value": repository_ctx.os.environ.get("MANY_CC_LIBRARIES", "1"),
        },
        {
            "path": "cc-library-sources.bzl",
            "name": "cc_library_sources",
            "value": repository_ctx.os.environ.get("MANY_CC_LIBRARY_SOURCES", "1"),
        },
    ]

    for param in params:
        content = """\
def {name}():
    return {value}
""".format(name = param["name"], value = param["value"])

        repository_ctx.file(
            param["path"],
            content = content,
            executable = False,
        )

    # Need to have a BUILD file to define a bazel package for the rules files above.
    repository_ctx.file(
        "BUILD",
        content = "",
        executable = False,
    )

    return None

many_params = repository_rule(
    implementation = _many_params_impl,
    local = True,
    environ = [
        "MANY_CC_BINARIES",
        "MANY_CC_LIBRARIES",
        "MANY_CC_LIBRARY_SOURCES",
    ],
)
