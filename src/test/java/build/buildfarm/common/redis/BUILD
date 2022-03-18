config_setting(
    name = "native-redis",
    values = {"define": "native-redis=true"},
)

COMMON_DEPS = [
    "//src/main/java/build/buildfarm/instance/shard",
    "//src/main/java/build/buildfarm/common",
    "//src/main/java/build/buildfarm/common/redis",
    "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    "//src/test/java/build/buildfarm:test_runner",
    "//third_party/jedis",
    "@maven//:com_google_truth_truth",
    "@maven//:io_grpc_grpc_api",
    "@maven//:org_mockito_mockito_core",
]

NATIVE_REDIS_TESTS = [
    "BalancedRedisQueueTest.java",
    "RedisNodeHashesTest.java",
    "RedisQueueTest.java",
    "RedisPriorityQueueTest.java",
]

filegroup(
    name = "lua_resources",
    srcs = ["//src/main/java/build/buildfarm:zpoplpush.lua"],
    visibility = ["//visibility:public"],
)

java_test(
    name = "tests",
    size = "small",
    srcs = glob(
        ["*.java"],
        exclude = NATIVE_REDIS_TESTS,
    ) + select({
        ":native-redis": NATIVE_REDIS_TESTS,
        "//conditions:default": [],
    }),
    classpath_resources = [":lua_resources"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "priority_queue_test",
    size = "small",
    srcs = select({
        ":native-redis": "RedisPriorityQueueTest.java",
        "//conditions:default": [],
    }),
    classpath_resources = [":lua_resources"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "queue_test",
    size = "small",
    srcs = select({
        ":native-redis": "RedisQueueTest.java",
        "//conditions:default": [],
    }),
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "balanced_queue_test",
    size = "small",
    srcs = select({
        ":native-redis": "BalancedRedisQueueTest.java",
        "//conditions:default": [],
    }),
    classpath_resources = [":lua_resources"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)
