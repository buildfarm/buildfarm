"""
buildfarm images that can be imported into other WORKSPACE files
"""

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")
load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

def buildfarm_images():
    """
    Pull the necessary base containers to be used for image definitions.
    """

    container_deps()

    container_pull(
        name = "java_image_base",
        digest = "sha256:8c1769cb253bdecc257470f7fba05446a55b70805fa686f227a11655a90dfe9e",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:57c99181c9dea202a185970678f723496861b4ce3c534f35f29fe58964eb720c",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    # Base mantic worker image for public releases (built via github action from ci/base-worker-image/mantic/Dockerfile)
    container_pull(
        name = "ubuntu-mantic",
        registry = "index.docker.io",
        repository = "bazelbuild/buildfarm-worker-base",
        tag = "mantic",
    )

    # Base worker image for public releases (built via github action from ci/base-worker-image/jammy/Dockerfile)
    container_pull(
        name = "ubuntu-jammy",
        registry = "index.docker.io",
        repository = "bazelbuild/buildfarm-worker-base",
        tag = "jammy",
    )

    # Server base image
    container_pull(
        name = "amazon_corretto_java_image_base",
        registry = "public.ecr.aws/amazoncorretto",
        repository = "amazoncorretto",
        tag = "21",
    )
