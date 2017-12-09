java_test(
    name = "BuildFarmServerTest",
    srcs = ["BuildFarmServerTest.java"],
    deps = [
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
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
        "@googleapis//:google_rpc_code_java_proto",
    ],
)

java_test(
    name = "BuildFarmWorkerTest",
    srcs = ["BuildFarmWorkerTest.java"],
    deps = [
        "//src/main/java/build/buildfarm:buildfarm-worker-impl",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)

java_test(
    name = "ByteStringIteratorInputStreamTest",
    srcs = ["ByteStringIteratorInputStreamTest.java"],
    deps = [
        "//3rdparty/jvm/com/google/guava",
        "//3rdparty/jvm/com/google/protobuf:protobuf_java",
        "//3rdparty/jvm/com/google/truth",
        "//src/main/java/build/buildfarm:stub-instance",
    ],
)
