workspace(name = "build_buildfarm")

load(":deps.bzl", "buildfarm_dependencies")

buildfarm_dependencies()

load(":defs.bzl", "buildfarm_init")

buildfarm_init()

load("@rules_oss_audit//oss_audit:setup.bzl", "rules_oss_audit_setup")

rules_oss_audit_setup()

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

load(":images.bzl", "buildfarm_images")

buildfarm_images()
