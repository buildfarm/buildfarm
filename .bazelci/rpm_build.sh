#!/bin/bash
# Ensure that RPM packages can be built.
# Currently this is done inside a container since rules_pkg has a system dependency on rpmbuild
docker build -t buildfarm .
docker run buildfarm /bin/bash -c "cd buildfarm; ./bazelw build --host_force_python=PY2 //src/main/java/build/buildfarm/rpms/worker:buildfarm-worker-rpm"