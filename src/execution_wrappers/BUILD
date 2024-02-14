package(default_visibility = ["//visibility:public"])

# == Execution Wrappers ==
# Execution wrappers are programs that buildfarm chooses to use when running REAPI actions.  They are used for
# both sandboxing, as well as changing runtime behavior of actions.  Buildfarm workers can be configured
# to use execution wrappers directly through a configuration called "execution policy", as well as sandbox configs.
# Buildfarm may also choose different execution wrappers dynamically based on exec_properties.  From a bazel perspective,
# you can think of "execution wrappers" like bazel's --run_under.  Wrappers can be used for all remote actions and have
# different semantics to being selected than run_under.  Wrappers can also be stacked (i.e. actions can run under multiple
# wrappers). This is convenient both for migrations, as well as running experimental tests on actions. In order to have
# them available to the worker, they should be provided to a java_image as a "runtime_dep". Buildfarm workers will warn
# about any missing execution wrappers during startup and what features are unavailable due to their absence.

# == Execution Wrapper Compatibility ==
# "process-wrapper" and "linux-sandbox" are sourced directly from bazel.  Users may want to ensure that the same
# bazel version is used in buildfarm agents as is used by bazel clients.  There has not been any known issues due
# to version mismatch, but we state the possibility here.  Some execution wrappers will not be compatible with all
# operating systems.  We make a best effort and ensure they all work in the below images.

# == Recomendations ==

# == Security ==
# We recomend you restrict permissions on actions using at least 1 type of execution wrapper.
# The "linux-sandbox" is a good choice because it is the same sandbox that bazel uses internally.
# In particular, you want to avoid build actions running as root and causing issues on the host.
# Other restrictions are recomended such as enabling tmpfs, block-network, fake-hostname to avoid
# build flakiness.

# == Resources ==
# We also recomend you restrict resources used by the actions.  Cgroups is a system installed dependency
# that buildfarm knows how to use for core and memory restriction.  This will avoid noisy neighbor problems
# between actions and prevent overloading the host where the buildfarm worker runs.

java_library(
    name = "execution_wrappers",
    data = [
        ":as-nobody",
        ":delay",
        ":linux-sandbox.binary",
        ":macos-wrapper",
        ":process-wrapper.binary",
        ":skip_sleep.binary",
        ":skip_sleep.preload",
        ":tini.binary",
    ],
)

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
    name = "as-nobody",
    srcs = select({
        "//config:windows": ["as-nobody-windows.c"],
        "//conditions:default": ["as-nobody.c"],
    }),
)

genrule(
    name = "skip_sleep.binary",
    srcs = ["@skip_sleep"],
    outs = ["skip_sleep"],
    cmd = "cp $< $@;",
)

genrule(
    name = "skip_sleep.preload",
    srcs = ["@skip_sleep//:skip_sleep_preload"],
    outs = ["skip_sleep_preload.so"],
    cmd = "cp $< $@;",
)

# The delay wrapper is only intended to be used with the "skip_sleep" wrapper.
sh_binary(
    name = "delay",
    srcs = ["delay.sh"],
)

sh_binary(
    name = "macos-wrapper",
    srcs = ["macos-wrapper.sh"],
)
