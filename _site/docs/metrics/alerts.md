---
layout: default
title: Alerts
parent: Metrics
nav_order: 1
---

Using the Prometheus metrics you may want to setup alerts on your buildfarm cluster.  Below are some example alerts:

**High Cpu Queue**
`avg(queue_size{job="your_instance", queue_name="cpu_queue"})`
Critical = Triggered when the value is greater than 1000 for 60m
Warning = Triggered when the value is greater than 1000 for 30m

**High Cluster Utilization**
`avg(execution_slot_usage{job="your_instance", service="buildfarm-worker"})`
Critical = Triggered when the value is greater than 85 for 60m
Warning = Triggered when the value is greater than 85 for 30m

**Multiple Bazel Versions Detected**
`count by (tool_name) (count(dispatched_operations_tools_amount{job="your_instance", service="buildfarm-server"}) without(host_ip, instance))`
Warning = Triggered when the value is greater than 1 for 1m

**No available workers**
`avg(worker_pool_size{job="your_instance", service="buildfarm-server"})`
Critical = Triggered when the value is equal to 0 for 5m
Warning = Triggered when the value is equal to 0 for 2m

**Stuck CPU Workers**
```
(avg(delta(execution_slot_usage{job="your_instance", service="buildfarm-worker"}[5m])) by (host_ip) == 0) and (avg(execution_slot_usage{job="your_instance", service="buildfarm-worker"}) by (host_ip) > 0) and (avg(execution_slot_usage{job="your_instance", service="buildfarm-worker"}) by (host_ip) < 89)
```
Critical = Triggered when the value is greater than or equal to 0 for 30m

**Key Expirations - Builds May Be Slow**
```
avg(increase(expired_key_total{job="your_instance", service="buildfarm-worker"}[5m])) by (host_ip)
```
Warning = Triggered when the value is greater than 10000 for 1h

**High Prequeue**
`pre_queue_size{job="your_instance", service="buildfarm-server"}`
Critical = Triggered when the value is greater than 0 for 15m
Warning = Triggered when the value is greater than 0 for 5m