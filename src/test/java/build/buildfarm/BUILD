load("@rules_java//java:java_library.bzl", "java_library")

filegroup(
    name = "configs",
    srcs = ["logging.properties"],
    visibility = [":__subpackages__"],
)

java_library(
    name = "test_runner",
    testonly = True,
    srcs = [
        "AllTests.java",
        "Classpath.java",
        "CustomSuite.java",
        "TestSuiteBuilder.java",
    ],
    visibility = ["//src/test/java/build/buildfarm:__subpackages__"],
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
    ],
)
