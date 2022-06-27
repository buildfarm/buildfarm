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

k8s_object(
    name = "open-telemetry",
    kind = "service",
    template = ":open-telemetry.yaml",
)

k8s_object(
    name = "jaeger",
    kind = "service",
    template = ":jaeger.yaml",
)

k8s_object(
    name = "grafana",
    kind = "service",
    template = ":grafana.yaml",
)
