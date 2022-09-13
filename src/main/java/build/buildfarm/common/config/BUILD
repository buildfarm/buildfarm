java_library(
    name = "config",
    srcs = glob([
        "*.java",
    ]),
    plugins = [":lombok-java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_longrunning_operations_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:org_projectlombok_lombok",
        "@maven//:org_yaml_snakeyaml",
    ],
)

java_plugin(
    name = "lombok-java",
    generates_api = True,
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    visibility = ["//visibility:public"],
    deps = ["@maven//:org_projectlombok_lombok"],
)
