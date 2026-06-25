load("@rules_java//java:java_plugin.bzl", "java_plugin")

# NullAway static analysis plugin for null pointer dereference detection
# See https://github.com/uber/NullAway for more information
#
# SETUP REQUIRED:
# Before using this plugin, you need to update the maven_install.json lock file
# to include the NullAway dependency. To do this:
#
# 1. Temporarily set fail_if_repin_required = False in MODULE.bazel (line 173)
# 2. Run: bazel build //tools/nullaway:nullaway
#    This will download NullAway and its dependencies
# 3. Run: REPIN=1 bazel run @buildfarm_maven//:pin
#    This will update maven_install.json
# 4. Restore fail_if_repin_required = True in MODULE.bazel
#
# To use NullAway in your java_library or java_binary:
#   java_library(
#       name = "my_library",
#       srcs = ["MyClass.java"],
#       plugins = ["//tools/nullaway"],
#       deps = [...],
#   )
#
# NullAway requires the following compiler flags (configured in .bazelrc):
#   --javacopt=-XepDisableWarningsInGeneratedCode
#   --javacopt=-Xep:NullAway:ERROR
#   --javacopt=-XepOpt:NullAway:AnnotatedPackages=build.buildfarm
#
# You can also annotate specific packages or exclude certain classes:
#   --javacopt=-XepOpt:NullAway:UnannotatedSubPackages=build.buildfarm.generated
#   --javacopt=-XepOpt:NullAway:ExcludedClasses=build.buildfarm.common.Generated.*

java_plugin(
    name = "nullaway",
    generates_api = False,
    visibility = ["//visibility:public"],
    deps = [
        "@buildfarm_maven//:com_google_errorprone_error_prone_core",
        "@buildfarm_maven//:com_uber_nullaway_nullaway",
    ],
)
