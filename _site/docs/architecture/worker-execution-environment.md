---
layout: default
title: Worker Execution Environment
parent: Architecture
nav_order: 3
---

A Buildfarm worker executes actions on behalf of remote-execution clients. Its operating system, libraries, compiler toolchains, runtimes, and execution wrappers therefore form part of the execution environment exposed to those clients. For fully hermetic builds, declare every required toolchain and runtime in the client build. In practice, a worker image is also useful as a compatible, supported baseline while teams adopt hermetic toolchains.

Buildfarm's maintained worker base is [`index.docker.io/bazelbuild/buildfarm-worker-base:noble`](https://hub.docker.com/r/bazelbuild/buildfarm-worker-base). It is hosted on Docker Hub, based on **Ubuntu 24.04 (Noble Numbat)**, and includes OpenJDK 21, `build-essential`, `libfuse2`, and `cgroup-tools`. The Buildfarm release image uses this same base. It is published for both `linux/amd64` and `linux/arm64`.

This page shows how to compose a worker image with Bzlmod and [`rules_oci`](https://github.com/bazel-contrib/rules_oci). It does not require a local registry or the Docker daemon while building the image.

## Build a worker image

Start with a Buildfarm checkout (or fork) at the revision you intend to deploy. Buildfarm's root [`MODULE.bazel`](https://github.com/buildfarm/buildfarm/blob/main/MODULE.bazel) is a Bzlmod module: it declares `rules_oci`, pulls the pinned Noble worker base through the `oci` module extension, and exposes it as `@ubuntu_noble`. It also makes architecture-specific image repositories available for multi-platform image definitions.

```shell
git clone https://github.com/buildfarm/buildfarm.git
cd buildfarm
```

To customize the worker image, add a package such as `container/custom/BUILD.bazel` to the checkout or your fork:

```starlark
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")

package(default_visibility = ["//visibility:public"])

oci_image(
    name = "buildfarm_worker_image",
    base = "@ubuntu_noble",
    entrypoint = [
        "/tini",
        "--",
        "java",
        "-jar",
        "/app/build_buildfarm/buildfarm-shard-worker_deploy.jar",
    ],
    env = {
        "CONFIG_PATH": "/app/build_buildfarm/config.minimal.yml",
        "JAVA_TOOL_OPTIONS": "-XX:+UseContainerSupport -XX:MaxRAMPercentage=80.0",
    },
    tars = [
        # Buildfarm supplies the worker binary, default configuration, logging,
        # configuration, execution wrappers, telemetry agent, and tini layer.
        "//container:layer_tini",
        "//container:layer_logging_config",
        "//container:layer_minimal_config",
        "//container:execution_wrappers",
        "//container:telemetry_tools",
        "//container:layer_buildfarm_worker",
    ],
)

# Imports the OCI image into the local Docker-compatible image store. This is
# intentionally a separate step from image construction.
oci_load(
    name = "load_buildfarm_worker",
    image = ":buildfarm_worker_image",
    repo_tags = ["example-buildfarm-worker:ubuntu24"],
)
```

Build the OCI layout without requiring Docker:

```shell
bazel build //container/custom:buildfarm_worker_image
```

When Docker Desktop, Podman, or another Docker-compatible image store is available, import the completed image and inspect it:

```shell
bazel run //container/custom:load_buildfarm_worker
docker image inspect example-buildfarm-worker:ubuntu24
```

The image's default `CONFIG_PATH` points to Buildfarm's example configuration. Before running a worker in a real deployment, supply a configuration that identifies the deployment's backplane and worker settings, for example by mounting a configuration file and overriding `CONFIG_PATH`:

```shell
docker run --rm \
  -v "$PWD/worker.yml:/config/worker.yml:ro" \
  -e CONFIG_PATH=/config/worker.yml \
  example-buildfarm-worker:ubuntu24
```

The worker must be able to reach its configured Buildfarm backplane. Its execution environment must also contain every tool expected by the actions it accepts. Extend the image with additional `pkg_tar` layers when a non-hermetic workload requires system packages; prefer declared Bazel toolchains and runfiles for new workloads so clients do not depend on undeclared packages in the worker image.
