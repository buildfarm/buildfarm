java_library(
    name = "asm",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/ow2/asm/asm",
    ],
)

java_library(
    name = "asm_analysis",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/ow2/asm/asm_analysis",
    ],
    runtime_deps = [
        ":asm_tree",
    ],
)

java_library(
    name = "asm_commons",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/ow2/asm/asm_commons",
    ],
    runtime_deps = [
        ":asm_tree",
    ],
)

java_library(
    name = "asm_tree",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/ow2/asm/asm_tree",
    ],
    runtime_deps = [
        ":asm",
    ],
)

java_library(
    name = "asm_util",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/ow2/asm/asm_util",
    ],
    runtime_deps = [
        ":asm_tree",
    ],
)
