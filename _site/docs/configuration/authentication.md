---
layout: default
title: Authentication
parent: Configuration
nav_order: 1
---

This is an overview of a simple way to create a self signed TLS key pair. Particularly how to create the TLS files and convert the key file to the PKCS8 format.

## Generate new key and create a self signed certificate:
```
openssl req \
-x509 \
-nodes \
-days 365 \
-newkey rsa:4096 \
-keyout selfsigned.key.pem \
-out selfsigned-x509.crt \
-subj "/C=US/ST=WA/L=Seattle/CN=example.com/emailAddress=someEmail@gmail.com"
```

### Output:
 - `selfsigned.key.pem` - PEM Key
 - `selfsigned-x509.crt` - x509 Certificate

## Convert PEM key to PKCS8 format
```
openssl pkcs8 \
-topk8 \
-inform PEM \
-outform PEM \
-in selfsigned.key.pem \
-out selfsigned-pkcs8.pem
```

### Ouptut:
 - `selfsigned-pkcs8.pem` - PKCS formatted key

## Buildfarm configuration

Provide these keys to configuration values `sslCertificatePath` and `sslPrivateKeyPath`.

## Combined credentials
You may have your certificate and private keys in a combined pem file.
For example, if you were to run `cat selfsigned.key.pem selfsigned-x509.crt > combined.pem`
you could provide `combined.pem` to both `sslCertificatePath` and `sslPrivateKeyPath` and get the same results.

