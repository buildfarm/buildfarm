---
layout: default
title: Home
nav_order: 1
description: "Buildfarm"
permalink: /
---

![Buildfarm]({{site.url}}{{site.baseurl}}/assets/images/buildfarm-logo.png){:style="height: 200px;"}
{: .fs-9 }

Remote Caching and Execution Service
{: .fs-6 .fw-300 }

[Get started now](https://buildfarm.github.io/buildfarm/docs/quick_start/){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 } [View it on GitHub](https://github.com/buildfarm/buildfarm){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## What is buildfarm?
Buildfarm is a service software stack which presents an implementation of the [Remote Execution API](https://github.com/bazelbuild/remote-apis). This means it can be used by any client of that API to retain content in a `ContentAddressableStorage`, cache ActionResults by a key `ActionCache`, and execute actions asynchronously with `Execution`.

Buildfarm supports many platforms and has been heavily tested with bazel as a client.

This documentation is a comprehensive description of the architecture, features, functionality, and operation, as well as a guide to smoothly installing and running the software. Familiarity with the Remote Execution API is helpful, and references to it will be provided as needed.
