package(default_visibility = ["//visibility:public"])

java_import(
    name = "netty",
    jars = ["netty/netty-all-4.1.8.Final.jar"],
)

# As of Guava 20.0, Guava code uses Error Prone annotations. This isn't a
# problem when compiling with java 8, but is a problem when compiling Bazel
# with java 7 (the Error Prone jars need to be on the javac classpath). So,
# until we drop support for compiling Bazel with java 7, we unfortunately need
# to ensure that the Error Prone jars are always used together with the Guava
# jars.
java_import(
    name = "guava",
    jars = [
        "error_prone/error_prone_annotations-2.0.19.jar",
        "jcip_annotations/jcip-annotations-1.0-1.jar",
        "jsr305/jsr-305.jar",
        "guava/guava-21.0.jar",
    ],
)

java_import(
    name = "instrumentation",
    jars = [
        "instrumentation/instrumentation-api-0.3.0.jar",
    ],
)

java_import(
    name = "jsr305",
    jars = ["jsr305/jsr-305.jar"],
)

java_import(
    name = "truth",
    jars = ["truth/truth-0.32.jar"],
)
