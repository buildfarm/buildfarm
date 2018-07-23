[![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg)](https://buildkite.com/bazel/buildfarm-postsubmit)

# Bazel Buildfarm

This repository hosts the [Bazel](https://bazel.build) remote caching and execution system.

This project is just getting started; background information on the status of caching and remote execution in bazel can be
found in the [bazel documentation](https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/remote/README.md#remote-caching-using-the-grpc-protocol).

Read the [meeting notes](https://docs.google.com/document/d/1EtQMTn-7sKFMTxIMlb0oDGpvGCMAuzphVcfx58GWuEM/edit).
Get involved by joining the discussion on the [dedicated mailing list](https://groups.google.com/forum/#!forum/bazel-buildfarm).

## Usage

In general do not execute server binaries with bazel run, since bazel does not support running multiple targets.

All commandline options override corresponding config settings.

### Bazel BuildFarmServer

Run via

    bazel build //src/main/java/build/buildfarm:buildfarm-server && \
    bazel-bin/src/main/java/build/buildfarm/buildfarm-server <configfile> [-p PORT] [--port PORT]

- **`configfile`** has to be in (undocumented) Protocol Buffer text format.

  For an example, see the `examples` directory.
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`PORT`** to expose service endpoints on

### Bazel BuildFarmWorker

Run via

    bazel build //src/main/java/build/buildfarm:buildfarm-worker && \
    bazel-bin/src/main/java/build/buildfarm/buildfarm-worker <configfile> [--root ROOT] [--cas_cache_directory CAS_CACHE_DIRECTORY]

- **`configfile`** has to be in (undocumented) Protocol Buffer text format.

  For an example, see the `examples` directory.
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`ROOT`** base directory path for all work being performed.

- **`CAS_CACHE_DIRECTORY`** is (absolute or relative) directory path to cached files from CAS.

### Bazel Itself

To have bazel use the bazel buildfarm configured using the example configs provided in the `examples` directory, you could configure your
`.bazelrc` as follows:

```
$ cat .bazelrc
build --spawn_strategy=remote --genrule_strategy=remote --strategy=Javac=remote --strategy=Closure=remote --remote_executor=localhost:8980
```

Then run your build as you would normally do.

## Developer Information

### Third-party Dependencies

Most third-party dependencies (e.g. protobuf, gRPC, ...) are managed automatically via
[bazel-deps](https://github.com/johnynek/bazel-deps). After changing the `dependencies.yaml` file,
just run this to regenerate the 3rdparty folder:

```bash
git clone https://github.com/johnynek/bazel-deps.git ../bazel-deps2
cd ../bazel-deps
bazel build //src/scala/com/github/johnynek/bazel_deps:parseproject_deploy.jar
cd ../bazel-buildfarm
../bazel-deps/gen_maven_deps.sh generate -r `pwd` -s 3rdparty/workspace.bzl -d dependencies.yaml
```

Things that aren't supported by bazel-deps are being imported as manually managed remote repos via
the `WORKSPACE` file.
