---
layout: default
title: Local Development
parent: Contribute
nav_order: 1
---

## Developer Information

The recommended solution for deploying a complete dev environment is to use [tilt](https://tilt.dev/).
Follow the installation instructions [here](https://docs.tilt.dev/).
Make sure you can use a local kubernetes cluster by following [these steps](https://docs.tilt.dev/choosing_clusters.html).
If everything is installed correctly, you can go to the root of the repo and run `tilt up`.
Tilt will prompt you to open a web UI and see all the running services.

Below is information for running services directly.

### Setting up Redis for local testing

This is done using [`examples/development-redis-cluster.sh`](examples/development-redis-cluster.sh).

Tested with Redis `6.0.10`, other versions probably work fine as well.

First of all you need Redis installed:
* macOS: `brew install redis`
* Debian / Ubuntu: `sudo apt-get update && sudo apt-get install redis-server redis-tools`

Then you need eight terminal panes for this. Six for [a minimal Redis
cluster](https://redis.io/topics/cluster-tutorial#creating-and-using-a-redis-cluster),
one for the Buildfarm server and one for a Buildfarm worker.

* `./examples/development-redis-cluster.sh 0`
* `./examples/development-redis-cluster.sh 1`
* `./examples/development-redis-cluster.sh 2`
* `./examples/development-redis-cluster.sh 3`
* `./examples/development-redis-cluster.sh 4`
* `./examples/development-redis-cluster.sh 5`
* ```sh
  redis-cli --cluster create 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383 127.0.0.1:6384 --cluster-replicas 1
  ```

Your Redis cluster is now up, and you can now start your Buildfarm server talking to it:
```sh
bazel run //src/main/java/build/buildfarm:buildfarm-server $PWD/examples/config.yml
```

And your Buildfarm worker:
```sh
mkdir /tmp/worker
bazel run //src/main/java/build/buildfarm:buildfarm-shard-worker $PWD/examples/config.yml
```

### Setting up intelliJ

1. Check [which IntelliJ versions are supported by the Bazel
   plugin](https://plugins.jetbrains.com/plugin/8609-bazel/versions)
1. Make sure you have a supported IntelliJ version, otherwise [download one
   here](https://www.jetbrains.com/idea/download/other.html)
1. Follow [the Bazel plugin
   instructions](https://ij.bazel.build/docs/import-project.html) and import
   [`ij.bazelproject`](ij.bazelproject)
1. Once IntelliJ is done loading your project, open
   [`BuildFarmServer.java`](src/main/java/build/buildfarm/server/BuildFarmServer.java)
   and find the `main()` method at the bottom
1. Press the green play button symbol in the gutter next to `main()` to create a
   Bazel build configuration for starting a server. Launching this configuration
   should get you a help text from Buildfarm Server indicating missing a config
   file.

   This indicates a successful launch!
1. To add a config file, edit your new run configuration and enter the absolute
   path to [`examples/config.minimal.yml`](examples/config.minimal.yml) in
   the "Executable flags" text box.

Now, you should have something like this, and you can now run / debug Buildfarm
Server from inside of IntelliJ, just like any other program:

![IntelliJ Buildfarm Server run configuration]]({{site.url}}{{site.baseurl}}/assets/images/intellij-server-run-config.png)
