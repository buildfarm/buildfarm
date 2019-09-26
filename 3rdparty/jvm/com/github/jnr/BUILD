java_library(
    name = "jffi",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jffi",
    ],
)

java_library(
    name = "jffi_jar_native",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/github/jnr/jffi_jar_native",
    ],
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
        ":jffi_jar_native",
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
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_posix",
    ],
    runtime_deps = [
        ":jnr_constants",
        ":jnr_ffi",
    ],
)

java_library(
    name = "jnr_x86asm",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/github/jnr/jnr_x86asm",
    ],
)
