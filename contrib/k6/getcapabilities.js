import http from 'k6/http';
import { Client } from 'k6/net/grpc';

const client = new Client();

import { check } from 'k6';



function checkCapabilities(c) {

  const response = c.invoke('build.bazel.remote.execution.v2.Capabilities/GetCapabilities', {});

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
    grpc_req_duration: ['p(95)<200'], // 95% of requests should be below 200ms
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

  client.close();
};
