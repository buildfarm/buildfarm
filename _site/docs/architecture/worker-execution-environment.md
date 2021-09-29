---
layout: default
title: Workers
parent: Architecture
nav_order: 3
---

Since workers are expected to execute programs in a way that makes using remote transparent to build users, there is a great deal of nuance to their definition. Operating System, Distribution, runtimes, compilers, their versions, and standard libraries make this a tricky proposition. While hermeticizing a build by declaring the full set of tools a compilation requires is the 'right' solution for a build, it may be too big of a hill to climb for everyone, particularly those trying to ease into remote execution.

Under Linux at least, this is made somewhat easier through docker, even if the steps to get to a conformant environment are a bit complicated. Here we will provide an example of creating a target execution environment container capable of running buildfarm, and this should be similar for all major distributions, particularly those with released docker hub bases and standard package managers including java runtimes and toolchain software. You should choose the versions of relevant software at first based on the client environment you want to support.

For this example, we're going to assume a target of ubuntu-20 (focal), with a gcc9 compiler supporting C++, and a java runtime from openjdk-14 supplied by its package manager. We will need:

- a bazel 3.3.1 install (the older version is required by rules_docker at the buildfarm version, this will be updated eventually).
- docker daemon running

First we will pull our intended base image from dockerhub:

`docker pull ubuntu:focal`

Next we will create our Dockerfile for our customized environment. Here is the content of that Dockerfile

```
# choose our ubuntu distribution with version 20 (focal)
from ubuntu:focal

# get our current software suite. apt-get is preferred to apt due to warnings
# about a non-stable CLI
run apt-get update

# install the basic apt-utils to limit warnings due to its absence
run DEBIAN_FRONTEND=noninteractive apt-get install --no-install-suggests apt-utils

# upgrade to the current software suite versions
run DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y

# install the required packages for our execution environment and buildfarm runtime
run DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-suggests \
    g++-9 g++ openjdk-14-jdk-headless
```

And build it with `docker build -t ubuntu20-java14:latest .`

```
Sending build context to Docker daemon   2.56kB
Step 1/5 : from ubuntu:focal
 ---> 9140108b62dc

...

Removing intermediate container 505236be00e4
 ---> 77a8c2ae4d16
Successfully built 77a8c2ae4d16
Successfully tagged ubuntu20-java14:latest
```

In our example's case, we want to use this image as a base to build a buildfarm worker container image. Unfortunately, rules_docker does not know how to interact with the `docker images` hosted by a local docker daemon, only docker registries like `dockerhub.io`. We will get past this by running the local registry utility container. If you really want to host this base elsewhere, I'll assume that you know how to translate the host:port references below, and you can skip running your own registry.

First we need to tag our image so that it gets pushed to our local registry:

`docker tag ubuntu20-java14:latest localhost.localdomain:5000/ubuntu20-java14:latest`

Then we can start our registry with a recognizable name for later shutdown:

`docker run -d --rm --name local-registry -p 5000:5000 registry`

And push our newly tagged image:

`docker push localhost.localdomain:5000/ubuntu20-java14:latest`

Take note of the `sha256:<sha256sum>` output that was produced with this command, as we will need to use it to specify our base.

Now we have a referent docker image that can be identified as a base to apply buildfarm's worker installation onto. In a suitable location, create a WORKSPACE containing:

```
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

BUILDFARM_EXTERNAL_COMMIT = "04fc2635f5546a4f5dc19dea35bb1fca5569ce24"
BUILDFARM_EXTERNAL_SHA256 = "f45215ef075c8aff230b737ca3bc5ba183c1137787fcbbb10dd407463f76edb6"

http_archive(
    name = "build_buildfarm",
    strip_prefix = "bazel-buildfarm-%s" % BUILDFARM_EXTERNAL_COMMIT,
    sha256 = BUILDFARM_EXTERNAL_SHA256,
    url = "https://github.com/bazelbuild/bazel-buildfarm/archive/%s.zip" % BUILDFARM_EXTERNAL_COMMIT,
)

load("@build_buildfarm//:deps.bzl", "buildfarm_dependencies")

buildfarm_dependencies()

load("@build_buildfarm//:defs.bzl", "buildfarm_init")

buildfarm_init()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

container_deps()

container_pull(
    name = "ubuntu20_java14_image_base",
    digest = "sha256:<sha256sum>",
    registry = "localhost:5000",
    repository = "ubuntu20-java14",
)
```

Be sure to substitute the `sha256:<sha256sum>` with your content from above.

Next we will create a BUILD file to create our target image. We will use the shard variant here, but the memory worker (with supporting server execution) will work as well. The content of the BUILD file should be:

```
load("@io_bazel_rules_docker//java:image.bzl", "java_image")

java_image(
    name = "buildfarm-shard-worker-ubuntu20-java14",
    base = "@ubuntu20_java14_image_base//image",
    deps = ["//src/main/java/build/buildfarm:buildfarm-shard-worker"],
    main_class = "build.buildfarm.worker.shard.Worker",
)
```

And now that this is in place, we can use the following to build the container and make it available to our local docker daemon:

`bazel run :buildfarm-shard-worker-ubuntu20-java14`