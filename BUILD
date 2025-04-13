load("@buildifier_prebuilt//:rules.bzl", "buildifier")

package(default_visibility = ["//visibility:public"])

# Made available for formatting
buildifier(
    name = "buildifier",
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
