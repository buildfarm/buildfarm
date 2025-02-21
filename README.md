# Buildfarm

![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg?branch=main)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/buildfarm/buildfarm/badge)](https://securityscorecards.dev/viewer/?uri=github.com/buildfarm/buildfarm)
![GitHub License](https://img.shields.io/github/license/buildfarm/buildfarm)
![GitHub Release](https://img.shields.io/github/v/release/buildfarm/buildfarm)
![Docker Pulls](https://img.shields.io/docker/pulls/bazelbuild/buildfarm-server)


This repository hosts a [remote caching and execution](https://github.com/bazelbuild/remote-apis) system, compatible with the build systems [Bazel](https://bazel.build), buck2, pants, and more.

Background information on the status of caching and remote execution in bazel can be
found in the [bazel documentation](https://docs.bazel.build/versions/master/remote-caching.html).

File issues here for bugs or feature requests, and ask questions via build team [slack](https://join.slack.com/t/buildteamworld/shared_invite/zt-4zy8f5j5-KwiJuBoAAUorB_mdQHwF7Q) in the #buildfarm channel.

[Buildfarm Docs](https://buildfarm.github.io/buildfarm/)

## Usage

All commandline options override corresponding config settings.

### Redis

Run via

```shell
$ docker run -d --rm --name buildfarm-redis -p 6379:6379 redis:7.2.4
redis-cli config set stop-writes-on-bgsave-error no
```

### Buildfarm Server

Run via

```shell
$ bazelisk run //src/main/java/build/buildfarm:buildfarm-server -- <logfile> <configfile>

Ex: bazelisk run //src/main/java/build/buildfarm:buildfarm-server -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml
```
**`logfile`** has to be in the [standard java util logging format](https://docs.oracle.com/cd/E57471_01/bigData.100/data_processing_bdd/src/rdp_logging_config.html) and passed as a --jvm_flag=-Dlogging.config=file:
**`configfile`** has to be in [yaml format](https://buildfarm.github.io/buildfarm/docs/configuration).

### Buildfarm Worker

Run via

```shell
$ bazelisk run //src/main/java/build/buildfarm:buildfarm-shard-worker -- <logfile> <configfile>

Ex: bazelisk run //src/main/java/build/buildfarm:buildfarm-shard-worker -- --jvm_flag=-Djava.util.logging.config.file=$PWD/examples/logging.properties $PWD/examples/config.minimal.yml

```
**`logfile`** has to be in the [standard java util logging format](https://docs.oracle.com/cd/E57471_01/bigData.100/data_processing_bdd/src/rdp_logging_config.html) and passed as a --jvm_flag=-Dlogging.config=file:
**`configfile`** has to be in [yaml format](https://buildfarm.github.io/buildfarm/docs/configuration).

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
See also the documentation site in the [Worker Execution Environment](https://buildfarm.github.io/buildfarm/docs/architecture/worker-execution-environment/) section.

Add the following to your `MODULE.bazel` to get access to buildfarm targets, filling in the `<COMMIT-SHA>` values:

```starlark
bazel_dep(name = "build_buildfarm")
git_override(
    module_name = "build_buildfarm",
    commit = "<COMMIT-SHA>",
    remote = "https://github.com/buildfarm/buildfarm.git",
)

# Transitive!
# TODO: remove this after https://github.com/bazelbuild/remote-apis/pull/293 is merged
bazel_dep(name = "remoteapis", version = "eb433accc6a666b782ea4b787eb598e5c3d27c93")
archive_override(
    module_name = "remoteapis",
    integrity = "sha256-68wzxNAkPZ49/zFwPYQ5z9MYbgxoeIEazKJ24+4YqIQ=",
    strip_prefix = "remote-apis-eb433accc6a666b782ea4b787eb598e5c3d27c93",
    urls = [
        "https://github.com/bazelbuild/remote-apis/archive/eb433accc6a666b782ea4b787eb598e5c3d27c93.zip",
    ],
)

bazel_dep(name = "googleapis", version = "0.0.0-20240326-1c8d509c5", repo_name = "com_google_googleapis")
bazel_dep(name = "grpc-java", version = "1.62.2")

googleapis_switched_rules = use_extension("@googleapis//:extensions.bzl", "switched_rules")
googleapis_switched_rules.use_languages(
    grpc = True,
    java = True,
)
use_repo(googleapis_switched_rules, "com_google_googleapis_imports")

```

You can then use the existing layer targets to build your own OCI images, for example:

``` starlark
oci_image(
    name = "mycompany-buildfarm-server",
    base = "@<YOUPROVIDE>",
    entrypoint = [
        "/usr/bin/java",
        "-jar",
        "/app/build_buildfarm/buildfarm-server_deploy.jar",
    ],
    tars = [
        "@build_buildfarm//:layer_buildfarm_server",
    ],
)
```

where `@<YOUPROVIDE>` is the name of an `oci.pull(name = 'YOUPROVIDE', ...)` in your `MODULE.bazel`

### Helm Chart

To install OCI bundled Helm chart:

```bash
helm install \
  -n bazel-buildfarm \
  --create-namespace \
  bazel-buildfarm \
  oci://ghcr.io/buildfarm/buildfarm \
  --version "0.2.9"
```
