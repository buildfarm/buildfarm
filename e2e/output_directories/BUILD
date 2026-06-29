load("@bazel_lib//lib:run_binary.bzl", "run_binary")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

sh_binary(
    name = "create_dir_with_files",
    srcs = ["create_dir_with_files.sh"],
)

run_binary(
    name = "colors",
    args = [
        "colors",
        "$(@D)",  # This arg expands to the output directory specified below
    ],
    out_dirs = ["colors"],
    tool = ":create_dir_with_files",
)

run_binary(
    name = "fruits",
    args = [
        "fruits",
        "$(@D)",  # This arg expands to the output directory specified below
    ],
    out_dirs = ["fruits"],
    tool = ":create_dir_with_files",
)
