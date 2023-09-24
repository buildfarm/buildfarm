"""
buildfarm definitions that can be imported into other WORKSPACE files
"""

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@remote_apis//:repository_rules.bzl", "switched_rules_by_language")
load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)
load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS", "grpc_java_repositories")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
load("@com_grail_bazel_toolchain//toolchain:rules.bzl", "llvm_toolchain")
load("@io_bazel_rules_k8s//k8s:k8s.bzl", "k8s_repositories")

IO_NETTY_MODULES = [
    "buffer",
    "codec",
    "codec-http",
    "codec-http2",
    "codec-socks",
    "common",
    "handler",
    "handler-proxy",
    "resolver",
    "transport",
    "transport-native-epoll",
    "transport-native-kqueue",
    "transport-native-unix-common",
]

IO_GRPC_MODULES = [
    "api",
    "auth",
    "core",
    "context",
    "netty",
    "stub",
    "protobuf",
    "testing",
    "services",
    "netty-shaded",
]

COM_AWS_MODULES = [
    "s3",
    "secretsmanager",
]

def buildfarm_init(name = "buildfarm"):
    """
    Initialize the WORKSPACE for buildfarm-related targets

    Args:
      name: the name of the repository
    """
    maven_install(
        artifacts = ["com.amazonaws:aws-java-sdk-%s:1.11.729" % module for module in COM_AWS_MODULES] +
                    [
                        "com.fasterxml.jackson.core:jackson-databind:2.15.0",
                        "com.github.ben-manes.caffeine:caffeine:2.9.0",
                        "com.github.docker-java:docker-java:3.2.11",
                        "com.github.fppt:jedis-mock:1.0.10",
                        "com.github.jnr:jffi:1.2.16",
                        "com.github.jnr:jffi:jar:native:1.2.16",
                        "com.github.jnr:jnr-constants:0.9.9",
                        "com.github.jnr:jnr-ffi:2.1.7",
                        "com.github.jnr:jnr-posix:3.0.53",
                        "com.github.pcj:google-options:1.0.0",
                        "com.github.serceman:jnr-fuse:0.5.5",
                        "com.github.luben:zstd-jni:1.5.2-1",
                        "com.github.oshi:oshi-core:6.4.0",
                        "com.google.auth:google-auth-library-credentials:0.9.1",
                        "com.google.auth:google-auth-library-oauth2-http:0.9.1",
                        "com.google.code.findbugs:jsr305:3.0.1",
                        "com.google.code.gson:gson:2.9.0",
                        "com.google.errorprone:error_prone_annotations:2.9.0",
                        "com.google.errorprone:error_prone_core:0.92",
                        "com.google.guava:failureaccess:1.0.1",
                        "com.google.guava:guava:32.1.1-jre",
                        "com.google.j2objc:j2objc-annotations:1.1",
                        "com.google.jimfs:jimfs:1.1",
                        "com.google.protobuf:protobuf-java-util:3.19.1",
                        "com.google.protobuf:protobuf-java:3.19.1",
                        "com.google.truth:truth:0.44",
                        "org.slf4j:slf4j-simple:1.7.35",
                        "com.googlecode.json-simple:json-simple:1.1.1",
                        "com.jayway.jsonpath:json-path:2.4.0",
                        "org.bouncycastle:bcprov-jdk15on:1.70",
                        "net.jcip:jcip-annotations:1.0",
                    ] + ["io.netty:netty-%s:4.1.94.Final" % module for module in IO_NETTY_MODULES] +
                    ["io.grpc:grpc-%s:1.56.1" % module for module in IO_GRPC_MODULES] +
                    [
                        "io.prometheus:simpleclient:0.10.0",
                        "io.prometheus:simpleclient_hotspot:0.10.0",
                        "io.prometheus:simpleclient_httpserver:0.10.0",
                        "junit:junit:4.13.1",
                        "javax.annotation:javax.annotation-api:1.3.2",
                        "net.javacrumbs.future-converter:future-converter-java8-guava:1.2.0",
                        "org.apache.commons:commons-compress:1.21",
                        "org.apache.commons:commons-pool2:2.9.0",
                        "org.apache.commons:commons-lang3:3.12.0",
                        "commons-io:commons-io:2.11.0",
                        "me.dinowernli:java-grpc-prometheus:0.5.0",
                        "org.apache.tomcat:annotations-api:6.0.53",
                        "org.checkerframework:checker-qual:2.5.2",
                        "org.mockito:mockito-core:2.25.0",
                        "org.openjdk.jmh:jmh-core:1.23",
                        "org.openjdk.jmh:jmh-generator-annprocess:1.23",
                        "org.redisson:redisson:3.13.1",
                        "org.threeten:threetenbp:1.3.3",
                        "org.xerial:sqlite-jdbc:3.34.0",
                        "org.jetbrains:annotations:16.0.2",
                        "org.yaml:snakeyaml:2.0",
                        "org.projectlombok:lombok:1.18.24",
                    ],
        generate_compat_repositories = True,
        override_targets = IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS,
        repositories = [
            "https://repo1.maven.org/maven2",
            "https://mirrors.ibiblio.org/pub/mirrors/maven2",
        ],
    )

    switched_rules_by_language(
        name = "bazel_remote_apis_imports",
        java = True,
    )

    container_repositories()

    protobuf_deps()

    grpc_java_repositories()

    k8s_repositories()

    native.bind(
        name = "jar/redis/clients/jedis",
        actual = "@jedis//jar",
    )

    llvm_toolchain(
        name = "llvm_toolchain",
        llvm_version = "10.0.0",
    )
