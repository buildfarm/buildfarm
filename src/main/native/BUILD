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
    name = "blake3",
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
    name = "libblake3_jni.so",
    linkshared = 1,
    visibility = ["//src/main/java/build/buildfarm/common/blake3:__pkg__"],
    deps = [
        ":blake3",
    ],
)

genrule(
    name = "mac-compat",
    srcs = ["libblake3_jni.so"],
    outs = ["libblake3_jni.dylib"],
    cmd = "cp $< $@",
    output_to_bindir = 1,
    visibility = ["//src/main/java/build/buildfarm/common/blake3:__pkg__"],
)

cc_binary(
    name = "blake3_jni.dll",
    linkshared = 1,
    visibility = [
        "//src/main/java/build/buildfarm/common/blake3:__pkg__",
    ],
    deps = [
        ":blake3",
    ],
)
