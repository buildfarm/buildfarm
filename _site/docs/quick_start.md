---
layout: default
title: Quick Start
nav_order: 3
---

# Quick Start

Here we describe how to use bazel remote caching or remote execution with buildfarm. We will create a single client workspace that can be used for both.

## Setup

You can run this quick start on a single computer running any flavor of linux that bazel supports. A C++ compiler is used here to demonstrate action execution. This computer is the localhost for the rest of the description.

### Backplane

Buildfarm requires a backplane to store information that is shared between cluster members. A [redis](https://redis.io) server can be used to meet this requirement.

Download/Install a redis-server instance and run it on your localhost. The default redis port of 6379 will be used by the default buildfarm configs.

## Workspace

Let's start with a bazel workspace with a single file to compile into an executable:

Create a new directory for our workspace and add the following files:

`main.cc`:

```c
#include <iostream>

int main( int argc, char *argv[] )
{
  std::cout << "Hello, World!" << std::endl;
}
```

`BUILD`:

```python
cc_binary(
    name = "main",
    srcs = ["main.cc"],
)
```

And an empty WORKSPACE file.

As a test, verify that `bazel run :main` builds your main program and runs it, and prints `Hello, World!`. This will ensure that you have properly installed `bazel` and a C++ compiler, and have a working target before moving on to remote caching or remote execution.

Download and extract the buildfarm repository. Each command sequence below will have the intended working directory indicated, between the client (workspace running bazel), and buildfarm.

This tutorial assumes that you have a bazel binary in your path and you are in the root of your buildfarm clone/release, and has been tested to work with bash on linux.

## Remote Caching

A Buildfarm cluster can be used strictly as an ActionCache (AC) and ContentAddressableStorage (CAS) to improve build performance. This is an example of running a bazel client that will retrieve results if available, otherwise store them on a cache miss after executing locally.

Download the buildfarm repository and change into its directory, then:

 * run `bazel run src/main/java/build/buildfarm:buildfarm-server $PWD/examples/config.minimal.yml`

This will wait while the server runs, indicating that it is ready for requests.

A server alone does not itself store the content of action results. It acts as an endpoint for any number of workers that present storage, so we must also start a single worker.

From another prompt (i.e. a separate terminal) in the buildfarm repository directory:

 * run `bazel run src/main/java/build/buildfarm:buildfarm-shard-worker -- --prometheus_port=9091 $PWD/examples/config.minimal.yml`

The `--` option is bazel convention to treat all subsequent arguments as parameters to the running app, like our `--prometheus_port`, instead of interpreting them with `run`
The `--prometheus_port=9091` option allows this worker to run alongside our server, who will have started and logged that it has started a service on port `9090`. You can also turn this option off (with `--` separator), with `--prometheus_option=0` for either server or worker.
This will also wait while the worker runs, indicating it will be available to store cache content.

From another prompt in your newly created workspace directory from above:

 * run `bazel clean`
 * run `bazel run --remote_cache=grpc://localhost:8980 :main`

Why do we clean here? Since we're verifying re-execution and caching, this ensures that we will execute any actions in the `run` step and interact with the remote cache. We should be attempting to retrieve cached results, and then when we miss - since we just started this memory resident server - bazel will upload the results of the execution for later use. There will be no change in the output of this bazel run if everything worked, since bazel does not provide output each time it uploads results.

To prove that we have placed something in the action cache, we need to do the following:

 * run `bazel clean`
 * run `bazel run --remote_cache=grpc://localhost:8980 :main`

This should now print statistics on the `processes` line that indicate that you've retrieved results from the cache for your actions:

```
INFO: 2 processes: 2 remote cache hit.
```

## Remote Execution (and caching)

Now we will use buildfarm for remote execution with a minimal configuration with a worker on the localhost that can execute a single process at a time, via a bazel invocation on our workspace.

First, to clean out the results from the previous cached actions, flush your local redis database:

 * run `redis-cli flushdb`

Next, we should restart the buildfarm server, and delete the worker's cas storage to ensure that we get remote execution (this can also be forced from the client by using `--noremote_accept_cached`). From the buildfarm server prompt and directory:

 * interrupt the running `buildfarm-server` (i.e. Ctrl-C)
 * run `bazel run src/main/java/build/buildfarm:buildfarm-server $PWD/examples/config.minimal.yml`

You can leave the worker running from the Remote Caching step, it will not require a restart

From another prompt, in your client workspace:

 * run `bazel run --remote_executor=grpc://localhost:8980 :main`

Your build should now print out the following on its `processes` summary line:

```
INFO: 2 processes: 2 remote.
```

That `2 remote` indicates that your compile and link ran remotely. Congratulations, you just build something through remote execution!

## Container Quick Start

To bring up a minimal buildfarm cluster, you can run:

```shell
$ ./examples/bf-run start
```

This will start all of the necessary containers at the latest version.
Once the containers are up, you can build with `bazel run --remote_executor=grpc://localhost:8980 :main`.

To stop the containers, run:

```shell
$ ./examples/bf-run stop
```

## Next Steps

We've started our worker on the same host as our server, and also the same host on which we built with bazel, but these services can be spread across many machines, per 'remote'. A large number of workers, with a relatively small number of servers (10:1 and 100:1 ratios have been used in practice), consolidating large disks and beefy multicore cpus/gpus on workers, with specialization of what work they perform for bazel builds (or other client work), and specializing servers to have hefty network connections to funnel content traffic. A buildfarm deployment can service hundreds or thousands of developers or CI processes, enabling them to benefit from each others' shared context in the AC/CAS, and the pooled execution of a fleet of worker hosts eager to consume operations and deliver results.

## Buildfarm Manager

You can now easily launch a new Buildfarm cluster locally or in AWS using an open sourced [Buildfarm Manager](https://github.com/80degreeswest/bfmgr).

```shell
$ wget https://github.com/80degreeswest/bfmgr/releases/download/1.0.7/bfmgr-1.0.7.jar
$ java -jar bfmgr-1.0.7.jar
$ open http://localhost
```
