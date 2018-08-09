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

import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;

/** Utility methods for the instance package. * */
public class Utils {

  private Utils() {}

  public static ByteString getBlob(Instance instance, Digest blobDigest) throws IOException, InterruptedException {
    try (InputStream in = instance.newStreamInput(instance.getBlobName(blobDigest), 0)) {
      return ByteString.readFrom(in);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }
}
