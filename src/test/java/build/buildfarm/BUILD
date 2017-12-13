java_test(
  name = "BuildFarmServerTest",
  srcs = ["BuildFarmServerTest.java"],
  deps = [
      "//src/main/java/build/buildfarm:common",
      "//src/main/java/build/buildfarm:buildfarm",
      "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_grpc",
      "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
      "//third_party/grpc:grpc-jar",
      "//third_party:guava",
      "//third_party:truth",
      "@com_google_protobuf//:protobuf_java",
      "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
      "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
      "@googleapis//:google_rpc_code_java_proto",
  ]
)

java_test(
  name = "BuildFarmWorkerTest",
  srcs = ["BuildFarmWorkerTest.java"],
  deps = [
      "//src/main/java/build/buildfarm:common",
      "//src/main/java/build/buildfarm:buildfarm-worker-impl",
      "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
  ],
)

java_test(
  name = "ByteStreamServiceTest",
  srcs = ["ByteStreamServiceTest.java"],
  deps = [
      "//src/main/java/build/buildfarm:buildfarm",
      "//src/main/java/build/buildfarm:common",
      "//third_party/grpc:grpc-jar",
      "@com_google_protobuf//:protobuf_java",
      "@googleapis//:google_bytestream_bytestream_java_grpc",
      "@googleapis//:google_bytestream_bytestream_java_proto",
      "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_grpc",
      "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_java_proto",
      "@googleapis//:google_rpc_code_java_proto",
      "@org_mockito//jar",
  ],
  size="small",
)

java_test(
  name = "ByteStringIteratorInputStreamTest",
  srcs = ["ByteStringIteratorInputStreamTest.java"],
  deps = [
      "//src/main/java/build/buildfarm:stub-instance",
      "//third_party:guava",
      "//third_party:truth",
      "@com_google_protobuf//:protobuf_java",
  ]
)
