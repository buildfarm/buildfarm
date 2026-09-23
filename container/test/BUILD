"""Tests for our OCI image outputs"""

load("@container_structure_test//:defs.bzl", "container_structure_test")

DRIVER = "docker"  # Use tar if your host is not amd64, but it's a lot slower. (for example, you use hardware from Apple and you have aarch64)

# Platform should match the host architecture for Docker driver
PLATFORM = select({
    "@platforms//cpu:aarch64": "linux/arm64",
    "//conditions:default": "linux/amd64",
})

container_structure_test(
    name = "worker_test",
    configs = [
        # keep sorted
        "example_config.yaml",
        "telemetry_tools.yaml",
        "worker.yaml",
        #"worker_wrappers.yaml", # TODO these depend on your host platform (arm/x86) and isn't consistent yet.
    ],
    driver = DRIVER,
    image = "//container:_buildfarm-worker.image",
    platform = PLATFORM,
    tags = ["container"],
)

container_structure_test(
    name = "server_test",
    configs = [
        # keep sorted
        "example_config.yaml",
        "server.yaml",
        "telemetry_tools.yaml",
    ],
    driver = DRIVER,
    image = "//container:_buildfarm-server.image",
    platform = PLATFORM,
    tags = ["container"],
)
