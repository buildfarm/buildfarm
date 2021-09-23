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