import http from 'k6/http';
import { Client, StatusOK } from 'k6/net/grpc';
import { randomIntBetween, randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const client = new Client();

import { check } from 'k6';

function findMissingBlobsBody(digestCount) {
  let digests = []
  for (let i = 0; i < digestCount; i++) {
    digests.push({
      hash: randomString(64, '0123456789abcdef'), // This looks like a sha256
      size_bytes: Math.floor(Math.random() * 2*1024*1024*1024)
    })
  }
  return {
    blob_digests: digests,
    digest_function: 1 // SHA-256
  }
}


function checkCapabilities(c) {

  const response = c.invoke('build.bazel.remote.execution.v2.Capabilities/GetCapabilities', {});
  // console.log(JSON.stringify(response.message));

  check(response, {
    'includes cache capabilities': (r) => r && r.message.cacheCapabilities,

    //https://github.com/bazelbuild/remote-apis/blob/0d21f29acdb90e1b67db5873e227051af0c80cdd/build/bazel/remote/execution/v2/remote_execution.proto#L2081
    'includes SHA-256 digest': (r) => r && r.message.cacheCapabilities.digestFunctions.includes("SHA256"),
    'supported compressors': (r) => r && r.message.cacheCapabilities.supportedCompressors.includes("IDENTITY"),
    //
    // https://github.com/bazelbuild/remote-apis/blob/0d21f29acdb90e1b67db5873e227051af0c80cdd/build/bazel/remote/execution/v2/remote_execution.proto#L2113
    'includes exec capabilities': (r) => r && r.message.executionCapabilities,
  });
}

export const options = {
  thresholds: {
    grpc_req_duration: ['p(95)<800'], // 95% of requests should be below 200ms
  },
};

export default () => {
  var grpcOptions = {
    reflect: true,
  }
  if (__ENV.SCHEME == 'grpc') {
    grpcOptions['plaintext'] = true; // Default to TLS unless you pass `plaintext: true`
  }
  client.connect(__ENV.HOSTANDPORT, grpcOptions);

  checkCapabilities(client);

  const blobCount = randomIntBetween(600, 1000);
  const response = client.invoke('build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs', findMissingBlobsBody(blobCount));

  check(response, {
    'status is OK': (r) => r && r.status === StatusOK,
    'count matches': (r) => r && r.message.missingBlobDigests.length === blobCount,
  });

  client.close();
};
