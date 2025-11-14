load("@buildifier_prebuilt//:rules.bzl", "buildifier")
load("@rules_cc//cc:cc_binary.bzl", "cc_binary")

package(default_visibility = ["//visibility:public"])

_lint_warnings = [
    "+native-cc-binary",
    "+native-cc-library",
    "+native-cc-shared-library-info",
    "+native-java-binary",
    "+native-java-common",
    "+native-java-import",
    "+native-java-library",
    "+native-java-plugin",
    "+native-java-proto",
    "+native-java-test",
    "+native-java-toolchain",
    "+native-proto",
    "+native-proto-common",
    "+native-proto-info",
]

# Made available for formatting
buildifier(
    name = "buildifier",
    lint_mode = "fix",
    lint_warnings = _lint_warnings,
)

cc_binary(
    name = "as-nobody",
    srcs = select({
        "//config:windows": ["as-nobody-windows.c"],
        "//conditions:default": ["as-nobody.c"],
    }),
    tags = ["container"],
)

exports_files([
    # keep sorted
    "cgexec-wrapper",
    "delay.sh",
    "macos-wrapper.sh",
])
