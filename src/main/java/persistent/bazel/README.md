Implements Bazel's Persistent Worker idea, using interfaces from our own `:persistent-common`: https://bazel.build/remote/persistent
We use Apache Commons Pool (commons-pool2) as the object pool backend.

Much of the code is ripped from Bazel itself, with a few adjustments specifically for Buildfarm.

Remote persistent workers (for Buildfarm) rely on the `--experimental_mark_tool_inputs` proposal:
https://github.com/bazelbuild/proposals/blob/main/designs/2021-03-06-remote-persistent-workers.md

There may be some confusion in the naming between "Buildfarm Workers" and "Bazel Persistent Workers".
Whereas Buildfarm workers are a (execute and/or CAS) node in Buildfarm,
Bazel persistent workers are a persistent *process* which run certain Bazel actions.

We want to wrap the the persistent worker process in a useful API.
This API will be used on the "client" side, i.e. the client submitting requests (i.e. actions) to
the persistent worker process.
