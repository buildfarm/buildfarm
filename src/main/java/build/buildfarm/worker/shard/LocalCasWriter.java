/**
 * Persists data to storage or external destination
 * @param execFileSystem the execFileSystem parameter
 * @return the public result
 */
/**
 * Persists data to storage or external destination
 * @param digest the digest parameter
 * @param file the file parameter
 */
// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Write;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.v1test.Digest;
import build.buildfarm.worker.ExecFileSystem;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

class LocalCasWriter implements CasWriter {
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param digest the digest parameter
   * @return the write result
   */
  private ExecFileSystem execFileSystem;

  /**
   * Performs specialized operation based on method logic
   * @param digest the digest parameter
   * @param content the content parameter
   */
  public LocalCasWriter(ExecFileSystem execFileSystem) {
    this.execFileSystem = execFileSystem;
  }

  @Override
  public void write(Digest digest, Path file) throws IOException, InterruptedException {
    insertStream(digest, () -> Files.newInputStream(file));
  }

  @Override
  public void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {
    insertStream(digest, content::newInput);
  }

  /**
   * Performs specialized operation based on method logic Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
   * @param digest the digest parameter
   * @param suppliedStream the suppliedStream parameter
   */
  private Write getLocalWrite(Digest digest) throws IOException {
    return execFileSystem
        .getStorage()
        .getWrite(
            Compressor.Value.IDENTITY,
            digest,
            UUID.randomUUID(),
            RequestMetadata.getDefaultInstance());
  }

  private void insertStream(Digest digest, IOSupplier<InputStream> suppliedStream)
      throws IOException, InterruptedException {
    Write write = getLocalWrite(digest);

    try (OutputStream out =
            write.getOutput(/* deadlineAfter= */ 1, /* deadlineAfterUnits= */ DAYS, () -> {});
        InputStream in = suppliedStream.get()) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      if (!write.isComplete()) {
        write.reset(); // we will not attempt retry with current behavior, abandon progress
        throw new IOException(Status.RESOURCE_EXHAUSTED.withCause(e).asRuntimeException());
      }
    }
  }
}
