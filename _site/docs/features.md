---
layout: default
title: Features
nav_order: 2
---

# General Features

Buildfarm has endeavored to support a wide variety of features implied or mandated by the Remote Execution API, including those currently not in use or worked around by bazel or other clients.

Most notably, buildfarm has universal support for:

* configurable instances with specific instance types
* progressive and flow controlled CAS reads and writes
* pluggable external CAS endpoints
* RequestMetadata behavior attribution

Bazel Client Feature Usage:

## [Fetch API](https://docs.bazel.build/versions/master/command-line-reference.html#flag--experimental_remote_downloader) - [Planned](https://github.com/buildfarm/buildfarm/pull/605)
## [Builds Without The Bytes](https://github.com/bazelbuild/bazel/issues/6862) - [Read This](https://buildfarm.github.io/buildfarm/docs/execution/builds-without-the-bytes/)
