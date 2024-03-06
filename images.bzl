"""
buildfarm images that can be imported into other WORKSPACE files
"""

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")
load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

def buildfarm_images():
    """
    Pull the necessary base containers to be used for image definitions.
    """

    container_deps()

    # Base mantic worker image for public releases (built via github action from ci/base-worker-image/mantic/Dockerfile)
    container_pull(
        name = "ubuntu-mantic",
        registry = "index.docker.io",
        repository = "bazelbuild/buildfarm-worker-base",
        tag = "mantic",
    )

    # Server base image
    container_pull(
        name = "amazon_corretto_java_image_base",
        registry = "public.ecr.aws/amazoncorretto",
        repository = "amazoncorretto",
        tag = "21",
    )
