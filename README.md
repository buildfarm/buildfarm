# Bazel Buildfarm

![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg?branch=main)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/bazelbuild/bazel-buildfarm/badge)](https://securityscorecards.dev/viewer/?uri=github.com/bazelbuild/bazel-buildfarm)
![GitHub License](https://img.shields.io/github/license/bazelbuild/bazel-buildfarm)
![GitHub Release](https://img.shields.io/github/v/release/bazelbuild/bazel-buildfarm)


This repository hosts the [Bazel](https://bazel.build) remote caching and execution system.

Background information on the status of caching and remote execution in bazel can be
found in the [bazel documentation](https://docs.bazel.build/versions/master/remote-caching.html).

File issues here for bugs or feature requests, and ask questions via build team [slack](https://join.slack.com/t/buildteamworld/shared_invite/zt-4zy8f5j5-KwiJuBoAAUorB_mdQHwF7Q) in the #buildfarm channel.

[Buildfarm Docs](https://bazelbuild.github.io/bazel-buildfarm/)

## Usage

All commandline options override corresponding config settings.

### Redis

Run via

```shell
$ docker run -d --rm --name buildfarm-redis -p 6379:6379 redis:5.0.9
redis-cli config set stop-writes-on-bgsave-error no
```

### Bazel Buildfarm Server

Run via

```shell
$ bazelisk run //src/main/java/build/buildfarm:buildfarm-server -- <logfile> <configfile>

Ex: bazelisk run //src/main/java/build/buildfarm:buildfarm-server -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
```
**`logfile`** has to be in the [standard java util logging format](https://docs.oracle.com/cd/E57471_01/bigData.100/data_processing_bdd/src/rdp_logging_config.html) and passed as a --jvm_flag=-Dlogging.config=file:
**`configfile`** has to be in [yaml format](https://bazelbuild.github.io/bazel-buildfarm/docs/configuration).

### Bazel Buildfarm Worker

Run via

```shell
$ bazelisk run //src/main/java/build/buildfarm:buildfarm-shard-worker -- <logfile> <configfile>

Ex: bazelisk run //src/main/java/build/buildfarm:buildfarm-shard-worker -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml

```
**`logfile`** has to be in the [standard java util logging format](https://docs.oracle.com/cd/E57471_01/bigData.100/data_processing_bdd/src/rdp_logging_config.html) and passed as a --jvm_flag=-Dlogging.config=file:
**`configfile`** has to be in [yaml format](https://bazelbuild.github.io/bazel-buildfarm/docs/configuration).

### Bazel Client

To use the example configured buildfarm with bazel (version 1.0 or higher), you can configure your `.bazelrc` as follows:

```shell
$ cat .bazelrc
$ build --remote_executor=grpc://localhost:8980
```

Then run your build as you would normally do.

### Debugging

Buildfarm uses [Java's Logging framework](https://docs.oracle.com/javase/10/core/java-logging-overview.htm) and outputs all routine behavior to the NICE [Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).

You can use typical Java logging configuration to filter these results and observe the flow of executions through your running services.
An example `logging.properties` file has been provided at [examples/logging.properties](examples/logging.properties) for use as follows:

```shell
$ bazel run //src/main/java/build/buildfarm:buildfarm-server -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
```

and

``` shell
$ bazel run //src/main/java/build/buildfarm:buildfarm-shard-worker -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
```

To attach a remote debugger, run the executable with the `--debug=<PORT>` flag. For example:

```shell
$ bazel run //src/main/java/build/buildfarm:buildfarm-server -- --debug=5005 $PWD/examples/config.minimal.yml
```


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

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()
```

Optionally, if you want to use the buildfarm docker container image targets, you can add this:

```starlark
load("@build_buildfarm//:images.bzl", "buildfarm_images")

buildfarm_images()
```

### Helm Chart

To install with helm:

```bash
# https://github.com/bazelbuild/bazel-buildfarm/releases/download/helm%2F0.3.0/buildfarm-0.3.0.tgz
CHART_VER="0.3.0"
helm install \
  -n bazel-buildfarm \
  --create-namespace \
  bazel-buildfarm \
  "https://github.com/bazelbuild/bazel-buildfarm/releases/download/helm%2F${CHART_VER}/buildfarm-${CHART_VER}.tgz"
```
