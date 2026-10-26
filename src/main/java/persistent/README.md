Persistent Workers for Buildfarm
===
An attempt at an interface for Persistent Workers,
based off Bazel's persistent workers implementation.

___

We split persistent workers into two targets:
`persistentworkers/src/main/java/persistent/common:persistent-common`
`persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers`


`persisent-common` aims to provide an interface for general persistent worker processes,
as well as a sort of "code as documentation".

`bazel-persistent-workers` implements `persistent-common` in the context of Bazel's persistent workers model.

Buildfarm workers will then be able to spin up and execute actions on persistent workers,
similar to how Bazel itself uses persistent workers.
