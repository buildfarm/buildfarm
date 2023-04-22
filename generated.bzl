"""
buildfarm generated definitions that can be imported into other WORKSPACE files
"""

load("@maven//:compat.bzl", "compat_repositories")
load("@python3_9//:defs.bzl", "interpreter")
load("@rules_python//python:pip.bzl", "pip_parse")

def buildfarm_generated():
    compat_repositories()

    pip_parse(
        name = "oss_audit_deps",
        python_interpreter_target = interpreter,
        requirements = "@rules_oss_audit//oss_audit/tools:requirements.txt",
    )
