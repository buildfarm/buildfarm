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

import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.CASFileCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.server.Instances;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.FilesystemCASConfig;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.devtools.common.options.OptionsParser;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import build.bazel.remote.execution.v2.RequestMetadata;
import java.util.function.Supplier;
import com.google.common.io.ByteStreams;

class LocalCasWriter implements CasWriter {
  
  private ExecFileSystem execFileSystem;
  
  public LocalCasWriter(ExecFileSystem execFileSystem){
    this.execFileSystem = execFileSystem;
  }
  
  public void write(Digest digest, Path file) throws IOException, InterruptedException {

    Write write = getLocalWrite(digest);
    InputStream inputStream = Files.newInputStream(file);
    insertStream(digest, Suppliers.ofInstance(inputStream));
  }

  private Write getLocalWrite(Digest digest) throws IOException, InterruptedException {
    Write write =
        execFileSystem
            .getStorage()
            .getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    return write;
  }

  public void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {

    Write write = getLocalWrite(digest);
    Supplier<InputStream> suppliedStream = () -> content.newInput();
    insertStream(digest, suppliedStream);
  }

  private void insertStream(Digest digest, Supplier<InputStream> suppliedStream)
      throws IOException, InterruptedException {

    Write write = getLocalWrite(digest);

    try (OutputStream out =
            write.getOutput(/* deadlineAfter=*/ 1, /* deadlineAfterUnits=*/ DAYS, () -> {});
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
