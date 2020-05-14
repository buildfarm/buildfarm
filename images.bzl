load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")
load(
    "@io_bazel_rules_docker//java:image.bzl",
    _java_image_repos = "repositories",
)

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

def buildfarm_images():
    container_deps()

    container_pull(
        name = "java_base",
        digest = "sha256:8c1769cb253bdecc257470f7fba05446a55b70805fa686f227a11655a90dfe9e",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    _java_image_repos()
