---
layout: default
title: OpenTelemetry
parent: Configuration
nav_order: 2
---

# OpenTelemetry

Buildfarm uses the [OpenTelemetry Java SDK](https://opentelemetry.io/docs/languages/java/) for its manual worker spans and supports the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/) for automatic instrumentation. OpenTelemetry configuration is not part of the Buildfarm YAML configuration.

Configure OpenTelemetry with standard Java system properties, `OTEL_*` environment variables, or a Java-agent configuration file. System properties take precedence over environment variables.

## Java Agent

The Buildfarm server and worker container images include the OpenTelemetry Java agent at:

```
/app/build_buildfarm/opentelemetry-javaagent.jar
```

Enable it by adding the following JVM option:

```shell
-javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar
```

For a local process, download an agent release and use the path where it was downloaded:

```shell
java -javaagent:/path/to/opentelemetry-javaagent.jar ...
```

The agent configures a global OpenTelemetry SDK. Buildfarm's worker spans use that same global SDK, so agent-created and Buildfarm-created spans use the same resource attributes, sampler, propagation, and exporters.

## Configuration

The following example exports traces to an OTLP/gRPC collector:

```shell
export OTEL_SERVICE_NAME=buildfarm-shard-worker
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

The equivalent Java system properties are:

```shell
-Dotel.service.name=buildfarm-shard-worker
-Dotel.exporter.otlp.endpoint=http://otel-collector:4317
-Dotel.exporter.otlp.protocol=grpc
```

Set authentication headers with `OTEL_EXPORTER_OTLP_HEADERS` or `otel.exporter.otlp.headers`; use a Kubernetes Secret rather than placing credentials directly in Helm values. Configure traces, metrics, logs, resource attributes, samplers, and batching with the standard [Java agent configuration options](https://opentelemetry.io/docs/zero-code/java/agent/configuration/).

## Helm

The Helm chart exposes `javaToolOptions` and `extraEnv` for `server`, `shardWorker`, and `execWorker`. For example:

```yaml
shardWorker:
  javaToolOptions: >-
    -XX:+UseContainerSupport
    -javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar
  extraEnv:
    - name: JAVABIN
      value: /usr/bin/java
    - name: OTEL_SERVICE_NAME
      value: buildfarm-shard-worker
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: http://otel-collector:4317
    - name: OTEL_EXPORTER_OTLP_PROTOCOL
      value: grpc
```

For sensitive settings, use `valueFrom` in `extraEnv` to read a Secret:

```yaml
    - name: OTEL_EXPORTER_OTLP_HEADERS
      valueFrom:
        secretKeyRef:
          name: otel-exporter-credentials
          key: headers
```

## Agent-Free Operation

When the Java agent is not installed, Buildfarm initializes an auto-configured OpenTelemetry SDK for its manual worker spans. It reads the same `OTEL_*` variables and `otel.*` system properties as the agent.

Configure an exporter explicitly in this mode. Set `OTEL_TRACES_EXPORTER=none` to disable trace export explicitly.

The automatic instrumentation supplied by the Java agent is unavailable in this mode.

## Configuration File

The Java agent can also read a mounted properties file. Set one of the following to its path:

```shell
OTEL_JAVAAGENT_CONFIGURATION_FILE=/etc/otel/agent.properties
```

```shell
-Dotel.javaagent.configuration-file=/etc/otel/agent.properties
```

The file is lower priority than system properties and environment variables. This is useful when a ConfigMap provides shared non-secret agent configuration while environment variables provide service-specific or secret values.
