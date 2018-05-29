java_library(
    name = "test_runner",
    srcs = [
        "AllTests.java",
        "Classpath.java",
        "CustomSuite.java",
        "TestSuiteBuilder.java",
    ],
    deps = [
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/junit",
    ],
)

java_test(
    name = "common-tests",
    size = "small",
    srcs = glob(["common/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/truth",
        "//src/main/java/build/buildfarm:common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)

java_test(
    name = "worker-tests",
    size = "small",
    srcs = glob(["worker/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/jimfs",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/truth",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//src/main/java/build/buildfarm:common",
        "//src/main/java/build/buildfarm:instance",
        "//src/main/java/build/buildfarm:stub-instance",
        "//src/main/java/build/buildfarm:worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
    ],
)

java_test(
    name = "server-tests",
    size = "small",
    srcs = glob(["server/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/truth",
        "//3rdparty/jvm/io/grpc:grpc_context",
        "//3rdparty/jvm/io/grpc:grpc_core",
        "//3rdparty/jvm/io/grpc:grpc_netty",
        "//3rdparty/jvm/io/grpc:grpc_protobuf",
        "//3rdparty/jvm/io/grpc:grpc_stub",
        "//src/main/java/build/buildfarm",
        "//src/main/java/build/buildfarm:common",
        "//src/main/java/build/buildfarm:stub-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@com_google_protobuf//:protobuf_java",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_grpc",
        "@googleapis//:google_rpc_code_java_proto",
    ],
)

java_test(
    name = "operationqueue-worker-tests",
    size = "small",
    srcs = glob(["worker/operationqueue/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//src/main/java/build/buildfarm:operationqueue-worker",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)

java_test(
    name = "stub-instance-tests",
    size = "small",
    srcs = glob(["instance/stub/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/truth",
        "//src/main/java/build/buildfarm:stub-instance",
    ],
)

java_test(
    name = "memory-instance-tests",
    size = "small",
    srcs = glob(["instance/memory/*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        ":test_runner",
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java_util",
        "//3rdparty/jvm/com/google/truth",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//src/main/java/build/buildfarm:common",
        "//src/main/java/build/buildfarm:instance",
        "//src/main/java/build/buildfarm:memory-instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_longrunning_operations_java_grpc",
    ],
)
