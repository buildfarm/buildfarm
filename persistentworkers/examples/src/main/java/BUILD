java_library(
    name = "adder-lib",
    srcs = glob(["**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers",
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "@buildfarm_maven//:com_google_guava_guava",
    ],
)

java_binary(
    name = "adder-bin",
    main_class = "adder.Adder",
    visibility = ["//visibility:public"],
    runtime_deps = [":adder-lib"],
)
