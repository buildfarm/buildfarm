sh_library(
    name = "bashunit",
    srcs = ["unittest.bash"],
    data = [
        "testenv.sh",
    ],
    visibility = ["//src/test/shell:__subpackages__"],
)

filegroup(
    name = "test-deps",
    testonly = 1,
    srcs = [
        "integration_test_setup.sh",
        "//src/test/shell:bashunit",
    ],
    visibility = ["//src/test/shell:__subpackages__"],
)
