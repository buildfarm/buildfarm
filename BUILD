load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")

buildifier(
    name = "buildifier",
)

# These are execution wrappers that buildfarm may choose to use when executing actions.
# For their availability on a worker, they should be provided to a java_image as a "runtime_dep".
# The relevant configuration for workers is the "execution policy".
# That is where these binaries can be used and stacked.
genrule(
    name = "process-wrapper.binary",
    srcs = ["@bazel//src/main/tools:process-wrapper"],
    outs = ["process-wrapper"],
    cmd = "cp $< $@;",
)

genrule(
    name = "linux-sandbox.binary",
    srcs = ["@bazel//src/main/tools:linux-sandbox"],
    outs = ["linux-sandbox"],
    cmd = "cp $< $@;",
)

genrule(
    name = "tini.binary",
    srcs = ["@tini//file"],
    outs = ["tini"],
    cmd = "cp $< $@ && chmod +x $@",
)

cc_binary(
    name = "as-nobody.binary",
    srcs = select({
        "//config:windows": ["as-nobody-windows.c"],
        "//conditions:default": ["as-nobody.c"],
    }),
)
