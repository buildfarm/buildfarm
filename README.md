# Bazel Buildfarm

This repository will host a [Bazel](https://bazel.build) remote caching and execution system.

This project is just getting started.

Read the [meeting notes](https://docs.google.com/document/d/1EtQMTn-7sKFMTxIMlb0oDGpvGCMAuzphVcfx58GWuEM/edit).
Get involved by joining the discussion on the [dedicated mailing list](https://groups.google.com/forum/#!forum/bazel-buildfarm).

## Usage

### Bazel BuildFarmServer

Run via

    bazel run //src/main/java/build/buildfarm:buildfarm-server -- config_file [-p PORT] [--port PORT]

- **`config_file`** has to be a (undocumented) Protocol Buffer text format.
  
  For format details see [here](https://stackoverflow.com/questions/18873924/what-does-the-protobuf-text-format-look-like). Protocol Buffer structure at [src/main/protobuf/build/buildfarm/v1test/buildfarm.proto](src/main/protobuf/build/buildfarm/v1test/buildfarm.proto)

- **`PORT`** to expose service endpoints on

  Overrides port setting in `config_file`.

