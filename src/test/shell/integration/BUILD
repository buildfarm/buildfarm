sh_test(
    name = "bazel_operationqueue_remote_execution_test",
    size = "large",
    timeout = "eternal",
    srcs = ["bazel_operationqueue_remote_execution_test.sh"],
    data = [
        "//src/test/shell:test-deps",
        "//src/main/java/build/buildfarm:buildfarm-operationqueue-worker",
        "//src/main/java/build/buildfarm:buildfarm-server",
    ],
)
