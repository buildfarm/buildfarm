"""
buildfarm definitions and configurations around choosing JVM flags for java images.
"""

SERVER_TELEMETRY_JVM_FLAGS = [
    "-javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar",
    "-Dotel.resource.attributes=service.name=server",
    "-Dotel.exporter.otlp.traces.endpoint=http://otel-collector:4317",
    "-Dotel.instrumentation.http.capture-headers.client.request",
    "-Dotel.instrumentation.http.capture-headers.client.response",
    "-Dotel.instrumentation.http.capture-headers.server.request",
    "-Dotel.instrumentation.http.capture-headers.server.response",
]

WORKER_TELEMETRY_JVM_FLAGS = [
    "-javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar",
    "-Dotel.resource.attributes=service.name=worker",
    "-Dotel.exporter.otlp.traces.endpoint=http://otel-collector:4317",
    "-Dotel.instrumentation.http.capture-headers.client.request",
    "-Dotel.instrumentation.http.capture-headers.client.response",
    "-Dotel.instrumentation.http.capture-headers.server.request",
    "-Dotel.instrumentation.http.capture-headers.server.response",
]

RECOMMENDED_JVM_FLAGS = [
    # Enables the JVM to detect if it is running inside a container and automatically adjusts
    # its behavior to optimize performance. This flag can help ensure that the JVM is
    # configured optimally for containerized environments.
    "-XX:+UseContainerSupport",

    # By default, the JVM sets the maximum heap size to 25% of the available memory.
    # It’s quite conservative.  Let's give the heap more space:
    "-XX:MaxRAMPercentage=80.0",

    # Enables the deduplication of identical strings in the JVM's string pool,
    # which can help reduce memory usage.
    "-XX:+UseStringDeduplication",

    # This flag enables compressed object pointers in the JVM,
    # which can reduce the memory footprint of objects on system.
    "-XX:+UseCompressedOops",

    # Create a heap dump when the JVM runs out of memory.
    # This can be useful for debugging memory-related issues.
    # At the very least, seeing a heap dump presented will be a clear indication of OOM.
    "-XX:+HeapDumpOnOutOfMemoryError",
]

DEFAULT_LOGGING_CONFIG = ["-Dlogging.config=file:/app/build_buildfarm/src/main/java/build/buildfarm/logging.properties"]

def ensure_accurate_metadata():
    return select({
        "//conditions:default": [],
        "//config:windows": ["-Dsun.nio.fs.ensureAccurateMetadata=true"],
    })

def server_telemetry():
    return select({
        "//config:open_telemetry": SERVER_TELEMETRY_JVM_FLAGS,
        "//conditions:default": [],
    })

def worker_telemetry():
    return select({
        "//config:open_telemetry": WORKER_TELEMETRY_JVM_FLAGS,
        "//conditions:default": [],
    })

def server_jvm_flags():
    return RECOMMENDED_JVM_FLAGS + DEFAULT_LOGGING_CONFIG + ensure_accurate_metadata() + server_telemetry()

def worker_jvm_flags():
    return RECOMMENDED_JVM_FLAGS + DEFAULT_LOGGING_CONFIG + ensure_accurate_metadata() + worker_telemetry()
