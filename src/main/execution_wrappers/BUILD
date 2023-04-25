# == Execution Wrappers ==
# Execution wrappers are programs that buildfarm chooses to use when running REAPI actions.  They are used for
# both sandboxing, as well as changing runtime behavior of actions.  Buildfarm workers can be configured
# to use execution wrappers directly through a configuration called "execution policy".  Buildfarm may also 
# choose different execution wrappers dynamically based on exec_properties.
# 
# 
# Execution wrappers
# Wrappers can also be stacked (i.e. actions can run under multiple wrappers).    This is convenient both for migrations, as well as running experimental runtimes on the actions.  
# In order to have them available to the worker, they should
# be provided to a java_image as a "runtime_dep".  Buildfarm workers will warn about any missing execution wrappers
# during startup and what features are unavailable due to their absence.

# == Execution Wrapper Compatibility ==
# "process-wrapper" and "linux-sandbox" are sourced directly from bazel.  Users may want to ensure that the same
# bazel version is used in buildfarm agents as is used by bazel clients.  There has not been any known issues due
# to version mismatch, but we state the possibility here.  Some execution wrappers will not be compatible with all
# operating systems.  We make a best effort and ensure they all work in the below images.
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

java_library(
    name = "telemetry_tools",
    data = [
        ":opentelemetry-javaagent",
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

genrule(
    name = "opentelemetry-javaagent",
    srcs = ["@opentelemetry//jar"],
    outs = ["opentelemetry-javaagent.jar"],
    cmd = "cp $< $@;",
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