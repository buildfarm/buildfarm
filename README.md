[![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg)](https://buildkite.com/bazel/buildfarm-postsubmit)

# Bazel Buildfarm

This repository hosts the [Bazel](https://bazel.build) remote caching and execution system.

This project is just getting started; background information on the status of caching and remote execution in bazel can be
found in the [bazel documentation](https://docs.bazel.build/versions/master/remote-caching.html).

Read the [meeting notes](https://docs.google.com/document/d/1EtQMTn-7sKFMTxIMlb0oDGpvGCMAuzphVcfx58GWuEM/edit).
Get involved by joining the general api discussion on the [remote execution apis mailing list](https://groups.google.com/forum/#!forum/remote-execution-apis).

## Usage

All commandline options override corresponding config settings.

### Bazel Buildfarm Server

Run via

    bazel run //src/main/java/build/buildfarm:buildfarm-server <configfile> [<-p|--port> PORT]

- **`configfile`** has to be in Protocol Buffer text format, corresponding to a [BuildFarmServerConfig](https://github.com/bazelbuild/bazel-buildfarm/blob/master/src/main/protobuf/build/buildfarm/v1test/buildfarm.proto#L55) definition.

  For an example, see the [examples](examples) directory, which contains the working example [examples/server.config.example](examples/server.config.example).
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`PORT`** to expose service endpoints on

### Bazel Buildfarm Worker

Run via

    bazel run //src/main/java/build/buildfarm:buildfarm-operationqueue-worker <configfile> [--root ROOT] [--cas_cache_directory CAS_CACHE_DIRECTORY]

- **`configfile`** has to be in Protocol Buffer text format, corresponding to a [WorkerConfig](https://github.com/bazelbuild/bazel-buildfarm/blob/master/src/main/protobuf/build/buildfarm/v1test/buildfarm.proto#L459) definition.

  For an example, see the [examples](examples) directory, which contains the working example [examples/worker.config.example](examples/worker.config.example).
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`ROOT`** base directory path for all work being performed.

- **`CAS_CACHE_DIRECTORY`** is (absolute or relative) directory path to cached files from CAS.

### Bazel Client

To use the example configured buildfarm with bazel (version 1.0 or higher), you can configure your `.bazelrc` as follows:

```
$ cat .bazelrc
build --spawn_strategy=remote --genrule_strategy=remote --strategy=Javac=remote --strategy=Closure=remote --remote_executor=grpc://localhost:8980
```

Then run your build as you would normally do.

### Debugging

Buildfarm uses [Java's Logging framework](https://docs.oracle.com/javase/10/core/java-logging-overview.htm) and outputs all routine behavior to the NICE [Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).

You can use typical Java logging configuration to filter these results and observe the flow of executions through your running services.
An example `logging.properties` file has been provided at [examples/debug.logging.properties](examples/debug.logging.properties) for use as follows:

    bazel run //src/main/java/build/buildfarm:buildfarm-server --jvm_flag=-Djava.util.logging.config.file=examples/debug.logging.properties ...

and

    bazel run //src/main/java/build/buildfarm/buildfarm-operationqueue-worker --jvm_flag=-Djava.util.logging.config.file=examples/debug.logging.properties ...

To attach a remote debugger, run the executable with the `--debug=<PORT>` flag. For example:

    bazel run src/main/java/build/buildfarm/buildfarm-server --debug=5005 \
        $PWD/config/server.config

## Developer Information

### Setting up intelliJ

1. Follow the instructions in https://github.com/bazelbuild/intellij to install the bazel plugin for intelliJ
1. Import the project using `ij.bazelproject`

### Third-party Dependencies

Most third-party dependencies (e.g. protobuf, gRPC, ...) are managed automatically via
[bazel-deps](https://github.com/johnynek/bazel-deps). After changing the `dependencies.yaml` file,
just run this to regenerate the 3rdparty folder:

```bash
git clone https://github.com/johnynek/bazel-deps.git ../bazel-deps
cd ../bazel-deps
bazel build //src/scala/com/github/johnynek/bazel_deps:parseproject_deploy.jar
cd ../bazel-buildfarm
../bazel-deps/gen_maven_deps.sh generate -r `pwd` -s 3rdparty/workspace.bzl -d dependencies.yaml
```

Things that aren't supported by bazel-deps are being imported as manually managed remote repos via
the `WORKSPACE` file.
