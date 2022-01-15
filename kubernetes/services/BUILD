load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")

k8s_object(
    name = "redis-cluster",
    kind = "service",
    template = ":redis-cluster.yaml",
)

k8s_object(
    name = "shard-worker",
    kind = "service",
    template = ":shard-worker.yaml",
)
