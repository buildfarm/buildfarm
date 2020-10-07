[![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg?branch=master)](https://buildkite.com/bazel/buildfarm-postsubmit)

# Bazel Buildfarm

This repository hosts the [Bazel](https://bazel.build) remote caching and execution system.

Background information on the status of caching and remote execution in bazel can be
found in the [bazel documentation](https://docs.bazel.build/versions/master/remote-caching.html).

File issues here for bugs or feature requests, and ask questions via build team [slack](https://join.slack.com/t/buildteamworld/shared_invite/zt-4zy8f5j5-KwiJuBoAAUorB_mdQHwF7Q) in the #buildfarm channel.

[Buildfarm Wiki](https://github.com/bazelbuild/bazel-buildfarm/wiki)

## Usage

All commandline options override corresponding config settings.

### Bazel Buildfarm Server

Run via

```
bazel run //src/main/java/build/buildfarm:buildfarm-server <configfile> [<-p|--port> PORT]
```

- **`configfile`** has to be in Protocol Buffer text format, corresponding to a [BuildFarmServerConfig](https://github.com/bazelbuild/bazel-buildfarm/blob/master/src/main/protobuf/build/buildfarm/v1test/buildfarm.proto#L55) definition.

  For an example, see the [examples](examples) directory, which contains the working example [examples/server.config.example](examples/server.config.example).
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`PORT`** to expose service endpoints on

### Bazel Buildfarm Worker

Run via

```
bazel run //src/main/java/build/buildfarm:buildfarm-operationqueue-worker <configfile> [--root ROOT] [--cas_cache_directory CAS_CACHE_DIRECTORY]
```

- **`configfile`** has to be in Protocol Buffer text format, corresponding to a [WorkerConfig](https://github.com/bazelbuild/bazel-buildfarm/blob/master/src/main/protobuf/build/buildfarm/v1test/buildfarm.proto#L459) definition.

  For an example, see the [examples](examples) directory, which contains the working example [examples/worker.config.example](examples/worker.config.example).
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`ROOT`** base directory path for all work being performed.

- **`CAS_CACHE_DIRECTORY`** is (absolute or relative) directory path to cached files from CAS.

### Bazel Client

To use the example configured buildfarm with bazel (version 1.0 or higher), you can configure your `.bazelrc` as follows:

```
$ cat .bazelrc
build --remote_executor=grpc://localhost:8980
```

Then run your build as you would normally do.

### Debugging

Buildfarm uses [Java's Logging framework](https://docs.oracle.com/javase/10/core/java-logging-overview.htm) and outputs all routine behavior to the NICE [Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).

You can use typical Java logging configuration to filter these results and observe the flow of executions through your running services.
An example `logging.properties` file has been provided at [examples/debug.logging.properties](examples/debug.logging.properties) for use as follows:

```
bazel run //src/main/java/build/buildfarm:buildfarm-server -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/debug.logging.properties $PWD/examples/server.config.example
```

and

```
bazel run //src/main/java/build/buildfarm:buildfarm-operationqueue-worker -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/debug.logging.properties $PWD/examples/worker.config.example
```

To attach a remote debugger, run the executable with the `--debug=<PORT>` flag. For example:

```
bazel run //src/main/java/build/buildfarm:buildfarm-server -- --debug=5005 $PWD/examples/server.config.example
```

## Developer Information

### Setting up intelliJ

1. Follow the instructions in https://github.com/bazelbuild/intellij to install the bazel plugin for intelliJ
1. Import the project using `ij.bazelproject`

### Third-party Dependencies

Most third-party dependencies (e.g. protobuf, gRPC, ...) are managed automatically via
[rules_jvm_external](https://github.com/bazelbuild/rules_jvm_external). These dependencies are enumerated in
the WORKSPACE with a `maven_install` `artifacts` parameter.

Things that aren't supported by `rules_jvm_external` are being imported as manually managed remote repos via
the `WORKSPACE` file.

### Deployments

Buildfarm can be used as an external repository for composition into a deployment of your choice.

Add the following to your WORKSPACE to get access to buildfarm targets, filling in the commit and sha256 values:

```starlark
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

BUILDFARM_EXTERNAL_COMMIT = "<revision commit id>"
BUILDFARM_EXTERNAL_SHA256 = "<sha256 digest of url below>"

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

load("@build_buildfarm//:docker.bzl", "buildfarm_docker")

buildfarm_docker()

load("@build_buildfarm//:pip.bzl", "buildfarm_pip")

buildfarm_pip()
```

Optionally, if you want to use the buildfarm docker container image targets, you can add this:

```starlark
load("@build_buildfarm//:images.bzl", "buildfarm_images")

buildfarm_images()
```
