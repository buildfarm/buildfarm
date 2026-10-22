"""Java rules wrappers that auto-inject the NullAway Error Prone plugin.

All java_library, java_binary, and java_test targets that have srcs will
automatically get the //tools/nullaway plugin added. NullAway is disabled
by default (via .bazelrc) and enabled with --config=nullaway.
"""

load("@rules_java//java:java_binary.bzl", _java_binary = "java_binary")
load("@rules_java//java:java_library.bzl", _java_library = "java_library")
load("@rules_java//java:java_plugin.bzl", _java_plugin = "java_plugin")
load("@rules_java//java:java_test.bzl", _java_test = "java_test")

java_plugin = _java_plugin

_NULLAWAY_PLUGIN = "//tools/nullaway"

def java_library(name, srcs = None, plugins = None, **kwargs):
    """Wrapper for java_library with NullAway plugin.

    Args:
      name: target name.
      srcs: source files.
      plugins: annotation processor plugins.
      **kwargs: passed through to java_library.
    """
    plugins = list(plugins or [])

    if srcs and _NULLAWAY_PLUGIN not in plugins:
        plugins.append(_NULLAWAY_PLUGIN)

    _java_library(
        name = name,
        srcs = srcs,
        plugins = plugins if plugins else None,
        **kwargs
    )

def java_binary(name, srcs = None, plugins = None, **kwargs):
    """Wrapper for java_binary with NullAway plugin.

    Args:
      name: target name.
      srcs: source files.
      plugins: annotation processor plugins.
      **kwargs: passed through to java_binary.
    """
    plugins = list(plugins or [])

    if srcs and _NULLAWAY_PLUGIN not in plugins:
        plugins.append(_NULLAWAY_PLUGIN)

    _java_binary(
        name = name,
        srcs = srcs,
        plugins = plugins if plugins else None,
        **kwargs
    )

def java_test(name, srcs = None, plugins = None, **kwargs):
    """Wrapper for java_test with NullAway plugin.

    Args:
      name: target name.
      srcs: source files.
      plugins: annotation processor plugins.
      **kwargs: passed through to java_test.
    """
    plugins = list(plugins or [])

    if srcs and _NULLAWAY_PLUGIN not in plugins:
        plugins.append(_NULLAWAY_PLUGIN)

    _java_test(
        name = name,
        srcs = srcs,
        plugins = plugins if plugins else None,
        **kwargs
    )
