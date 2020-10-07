workspace(name = "build_buildfarm")

load(":deps.bzl", "buildfarm_dependencies")

buildfarm_dependencies()

load(":defs.bzl", "buildfarm_init")

buildfarm_init()

load(":docker.bzl", "buildfarm_docker")

buildfarm_docker()

load(":pip.bzl", "buildfarm_pip")

buildfarm_pip()

load(":images.bzl", "buildfarm_images")

buildfarm_images()
