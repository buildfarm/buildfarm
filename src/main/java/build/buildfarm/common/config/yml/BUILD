java_plugin(
    name = "lombok-java",
    generates_api = True,
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    visibility = ["//visibility:public"],
    deps = ["@maven//:org_projectlombok_lombok"],
)

java_library(
    name = "yml",
    srcs = glob([
        "*.java",
    ]),
    plugins = [":lombok-java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:org_projectlombok_lombok",
        "@maven//:org_yaml_snakeyaml",
    ],
)
