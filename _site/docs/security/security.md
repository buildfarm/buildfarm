---
layout: default
title: Security
has_children: false
nav_order: 6
---

# Auditing Buildfarm Artifacts
The complexities of identifying and tracking open-source software (OSS) to comply with license requirements adds friction to the development and integration process.  We solve this problem for buildfarm artifacts by creating an accurate "bill of materials" (BOM) containing OSS and third-party packages used to create our deployment.  

To audit buildfarm artifacts run the following:
```
bazel build :buildfarm-server-audit :buildfarm-shard-worker-audit
```

To see the BOM file:  
```
cat bazel-bin/buildfarm-server.bom.yaml
```

The BOM file contains library names with corresponding license information.  This currently only works for maven dependencies.
