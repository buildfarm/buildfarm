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

package build.buildfarm.worker.shard;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Path;

public interface CasWriter {
  void write(Digest digest, DigestFunction.Value digestFunction, Path file)
      throws IOException, InterruptedException;

  void insertBlob(Digest digest, DigestFunction.Value digestFunction, ByteString content)
      throws IOException, InterruptedException;
}
