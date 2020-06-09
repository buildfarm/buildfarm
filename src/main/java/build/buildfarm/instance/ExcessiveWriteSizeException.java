// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance;

import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;

public class ExcessiveWriteSizeException extends Exception {
  private static final long serialVersionUID = 2288236781501706181L;

  private final RequestMetadata requestMetadata;
  private final Digest digest;
  private final long maxBlobSize;

  public ExcessiveWriteSizeException(
      RequestMetadata requestMetadata, Digest digest, long maxBlobSize) {
    super(message(requestMetadata, digest, maxBlobSize));
    this.requestMetadata = requestMetadata;
    this.digest = digest;
    this.maxBlobSize = maxBlobSize;
  }

  public static String message(RequestMetadata requestMetadata, Digest digest, long maxBlobSize) {
    return format(
        "%s -> %s -> %s: attempted write of %s exceeds max_blob_size of %d",
        requestMetadata.getCorrelatedInvocationsId(),
        requestMetadata.getToolInvocationId(),
        requestMetadata.getActionId(),
        DigestUtil.toString(digest),
        maxBlobSize);
  }
}
