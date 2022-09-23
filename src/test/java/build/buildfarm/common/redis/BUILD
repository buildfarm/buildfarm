COMMON_DEPS = [
    "//src/main/java/build/buildfarm/instance/shard",
    "//src/main/java/build/buildfarm/common",
    "//src/main/java/build/buildfarm/common/config",
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
    "RedisHashMapTest.java",
]

java_test(
    name = "tests",
    size = "small",
    srcs = glob(
        ["*.java"],
        exclude = NATIVE_REDIS_TESTS,
    ),
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "priorityqueue",
    size = "small",
    srcs = ["RedisPriorityQueueMockTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "priorityqueue-redis",
    size = "small",
    srcs = ["RedisPriorityQueueTest.java"],
    tags = [
        "exclusive",
        "redis",
    ],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "queue",
    size = "small",
    srcs = ["RedisQueueMockTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "queue-redis",
    size = "small",
    srcs = ["RedisQueueTest.java"],
    tags = [
        "exclusive",
        "redis",
    ],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "balancedqueue",
    size = "small",
    srcs = ["BalancedRedisQueueMockTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "balancedqueue-redis",
    size = "small",
    srcs = ["BalancedRedisQueueTest.java"],
    tags = [
        "exclusive",
        "redis",
    ],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "hashmap",
    size = "small",
    srcs = ["RedisHashMapMockTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)

java_test(
    name = "hashmap-redis",
    size = "small",
    srcs = ["RedisHashMapTest.java"],
    tags = [
        "exclusive",
        "redis",
    ],
    test_class = "build.buildfarm.AllTests",
    deps = COMMON_DEPS,
)
