genrule(
    name = "copy_link_jni_md_header",
    srcs = select({
        "@bazel//src/conditions:darwin": ["@bazel_tools//tools/jdk:jni_md_header-darwin"],
        "@bazel//src/conditions:freebsd": ["@bazel_tools//tools/jdk:jni_md_header-freebsd"],
        "@bazel//src/conditions:openbsd": ["@bazel_tools//tools/jdk:jni_md_header-openbsd"],
        "@bazel//src/conditions:windows": ["@bazel_tools//tools/jdk:jni_md_header-windows"],
        "//conditions:default": ["@bazel_tools//tools/jdk:jni_md_header-linux"],
    }),
    outs = ["jni_md.h"],
    cmd = "cp -f $< $@",
    visibility = ["//src/main/native:__subpackages__"],
)

genrule(
    name = "copy_link_jni_header",
    srcs = ["@bazel_tools//tools/jdk:jni_header"],
    outs = ["jni.h"],
    cmd = "cp -f $< $@",
    visibility = ["//src/main/native:__subpackages__"],
)

cc_library(
    name = "blake3_jni",
    srcs = [
        "blake3_jni.cc",
        ":jni.h",
        ":jni_md.h",
    ],
    includes = ["."],  # For jni headers.
    visibility = ["//src/main/native:__subpackages__"],
    deps = [
        "@blake3",
    ],
    alwayslink = 1,
)

cc_binary(
    name = "libunix_jni.so",
    copts = [
        "-fPIC",
        "-DBLAZE_JAVA_CPU=\"k8\"",
    ],
    includes = ["."],  # For jni headers.
    linkopts = select({
        "//conditions:default": [],
    }),
    linkshared = 1,
    visibility = ["//src/main/java/build/buildfarm/common/jni:__pkg__"],
    deps = [
        ":blake3_jni",
    ],
)

# TODO(bazel-team): Come up with a way to support platform-specific dynamic
# library extensions.  This is issue #914.
genrule(
    name = "mac-compat",
    srcs = ["libunix_jni.so"],
    outs = ["libunix_jni.dylib"],
    cmd = "cp $< $@",
    output_to_bindir = 1,
    visibility = ["//src/main/java/build/buildfarm/common/jni:__pkg__"],
)
