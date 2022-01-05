load("@io_bazel_rules_k8s//k8s:object.bzl", "k8s_object")

k8s_object(
    name = "kubernetes",
    kind = "deployment",
    template = ":kubernetes.yaml",
)

k8s_object(
    name = "server",
    kind = "deployment",
    template = ":server.yaml",
)

k8s_object(
    name = "shard-worker",
    kind = "deployment",
    template = ":shard-worker.yaml",
)

k8s_object(
    name = "redis-cluster",
    kind = "deployment",
    template = ":redis-cluster.yaml",
)
