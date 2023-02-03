---
layout: default
title: Authentication
parent: Configuration
nav_order: 1
---

This is an overview of a simple way to create a self signed TLS key pair for client / server authentication.
We recommend you look at the [script](https://github.com/bazelbuild/bazel/blob/master/src/test/testdata/test_tls_certificate/README.md?plain=1#L1) used by bazel for certificate generation.

## Generated TLS self-signed certificates
After running the above script, you will have the following files:
```
ls /tmp/sslcert
ca.crt  ca.key  client.crt  client.csr  client.key  client.pem  server.crt  server.csr  server.key  server.pem
```

## Configuring the buildfarm server
The certificate and private key can be referenced in buildfarm's config as followed:
```
server:
  sslCertificatePath: /tmp/sslcert/server.crt
  sslPrivateKeyPath: /tmp/sslcert/server.pem
```
## Configuring the buildfarm client
When calling the bazel client, pass the certificate via `--tls_certificate`:
```
bazel build
  --remote_executor=grpcs://localhost:8980 \
  --tls_certificate=/tmp/sslcert/ca.crt \
  <target>

```
Don't forget to use `grpcs://` instead of `grpc://`.

## Combined credentials
You may have certificate and private keys in a combined pem file.
For example, if you were to run `cat  /tmp/sslcert/server.crt /tmp/sslcert/server.pem > combined.pem`
you could provide `combined.pem` to both `sslCertificatePath` and `sslPrivateKeyPath` and get the same results.