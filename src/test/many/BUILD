"""
Provide a simple C++ build graph as large as desired.

You may alter the values of cc_binaries(), cc_libraries(), and
cc_library_sources() via the environment variables MANY_CC_BINARIES,
MANY_CC_LIBRARIES, and MANY_CC_LIBRARY_SOURCES. For instance:

    MANY_CC_BINARIES=20 MANY_CC_LIBRARIES=10 MANY_CC_LIBRARY_SOURCES=5 bazel build //:cc
"""

load("@many-params//:cc-binaries.bzl", "cc_binaries")
load("@many-params//:cc-libraries.bzl", "cc_libraries")
load("@many-params//:cc-library-sources.bzl", "cc_library_sources")
load(":many-cc.bzl", "many_cc")

many_cc(
    name = "cc",
    binary_count = cc_binaries(),
    library_count = cc_libraries(),
    library_source_count = cc_library_sources(),
)
