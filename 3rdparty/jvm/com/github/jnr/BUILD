java_library(
    name = "jffi_native",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jffi_native",
    ],
)

java_library(
    name = "jffi",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jffi",
    ],
    runtime_deps = [":jffi_native"],
)

java_library(
    name = "jnr_constants",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_constants",
    ],
)

java_library(
    name = "jnr_ffi",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_ffi",
    ],
    runtime_deps = [
        ":jffi",
        ":jnr_x86asm",
        "//3rdparty/jvm/org/ow2/asm",
        "//3rdparty/jvm/org/ow2/asm:asm_analysis",
        "//3rdparty/jvm/org/ow2/asm:asm_commons",
        "//3rdparty/jvm/org/ow2/asm:asm_tree",
        "//3rdparty/jvm/org/ow2/asm:asm_util",
    ],
)

java_library(
    name = "jnr_posix",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_posix",
    ],
)

java_library(
    name = "jnr_x86asm",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_x86asm",
    ],
)
