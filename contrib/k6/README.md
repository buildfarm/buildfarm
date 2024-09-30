# K6 Performance Test Scripts

These go along with [k6](https://k6.io). You can run them from your workstation or via Grafana's cloud offering.

## Env Vars

- `HOSTANDPORT`: the host and port of your RBE endpoint. No scheme. Example: `localhost:8980`
- `SCHEME`: if this is `grpc`, then TLS will be turned off.

## Scripts

### `getcapabilities.js`

Tests the [`GetCapabilities`](https://github.com/bazelbuild/remote-apis/blob/0d21f29acdb90e1b67db5873e227051af0c80cdd/build/bazel/remote/execution/v2/remote_execution.proto#L452) call over and over. Does a few assertions about the response.

### `findmissingblobs.js`

Maks on `GetCapabilities` and then many [`FindMissingBlobs`](https://github.com/bazelbuild/remote-apis/blob/0d21f29acdb90e1b67db5873e227051af0c80cdd/build/bazel/remote/execution/v2/remote_execution.proto#L351) calls. Each call has a random number of digests, between 600 and 1000 digest. Each digest is also random, with a random size. It's not likely that you'll get cache hits.

## Examples

```sh
export HOSTANDPORT=localhost:8980
export SCHEME=grpc
k6 run -i 300 missingblobs.js
k6 run --vus 10 --duration 600s  missingblobs.js
```