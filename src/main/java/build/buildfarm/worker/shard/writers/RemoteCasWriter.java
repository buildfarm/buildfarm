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

import static build.buildfarm.cas.ContentAddressableStorages.createGrpcCAS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

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

class RemoteCasWriter implements CasWriter {
  
  public void write(Digest digest, Path file) throws IOException, InterruptedException {
    insertFileToCasMember(digest, file);
  }

  private void insertFileToCasMember(Digest digest, Path file)
      throws IOException, InterruptedException {

    try (InputStream in = Files.newInputStream(file)) {
      writeToCasMember(digest, in);
    } catch (ExecutionException e) {
      throw new IOException(Status.RESOURCE_EXHAUSTED.withCause(e).asRuntimeException());
    }
  }

  private void writeToCasMember(Digest digest, InputStream in)
      throws IOException, InterruptedException, ExecutionException {

    // create a write for inserting into another CAS member.
    String workerName = getRandomWorker();
    Instance casMember = workerStub(workerName);
    Write write = getCasMemberWrite(digest, workerName);

    streamIntoWriteFuture(in, write, digest).get();
  }

  private Write getCasMemberWrite(Digest digest, String workerName)
      throws IOException, InterruptedException {

    Instance casMember = workerStub(workerName);

    return casMember.getBlobWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
  }

  public void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {
    insertBlobToCasMember(digest, content);
  }

  private void insertBlobToCasMember(Digest digest, ByteString content)
      throws IOException, InterruptedException {

    try (InputStream in = content.newInput()) {
      writeToCasMember(digest, in);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfUnchecked(cause);
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Status status = Status.fromThrowable(cause);
      throw new IOException(status.asException());
    }
  }
  
  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      logger.log(Level.SEVERE, "error getting worker stub for " + worker, e.getCause());
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }
  
  private String getRandomWorker() throws IOException {
    Set<String> workerSet = backplane.getWorkers();
    synchronized (workerSet) {
      if (workerSet.isEmpty()) {
        throw new RuntimeException("no available workers");
      }
      Random rand = new Random();
      int index = rand.nextInt(workerSet.size());
      // best case no allocation average n / 2 selection
      Iterator<String> iter = workerSet.iterator();
      String worker = null;
      while (iter.hasNext() && index-- >= 0) {
        worker = iter.next();
      }
      return worker;
    }
  }
  
  
  private ListenableFuture<Long> streamIntoWriteFuture(InputStream in, Write write, Digest digest)
      throws IOException {

    SettableFuture<Long> writtenFuture = SettableFuture.create();
    int chunkSizeBytes = KBtoBytes(128);

    // The following callback is performed each time the write stream is ready.
    // For each callback we only transfer a small part of the input stream in order to avoid
    // accumulating a large buffer.  When the file is done being transfered,
    // the callback closes the stream and prepares the future.
    FeedbackOutputStream out =
        write.getOutput(
            /* deadlineAfter=*/ 1,
            /* deadlineAfterUnits=*/ DAYS,
            () -> {
              try {

                FeedbackOutputStream outStream = (FeedbackOutputStream) write;
                while (outStream.isReady()) {
                  if (!CopyBytes(in, outStream, chunkSizeBytes)) {
                    return;
                  }
                }

              } catch (IOException e) {
                if (!write.isComplete()) {
                  write.reset();
                  logger.log(Level.SEVERE, "unexpected error transferring file for " + digest, e);
                }
              }
            });

    write
        .getFuture()
        .addListener(
            () -> {
              try {
                try {
                  out.close();
                } catch (IOException e) {
                  // ignore
                }
                long committedSize = write.getCommittedSize();
                if (committedSize != digest.getSizeBytes()) {
                  logger.warning(
                      format(
                          "committed size %d did not match expectation for digestUtil",
                          committedSize));
                }
                writtenFuture.set(digest.getSizeBytes());
              } catch (RuntimeException e) {
                writtenFuture.setException(e);
              }
            },
            directExecutor());

    return writtenFuture;
  }

  private boolean CopyBytes(InputStream in, OutputStream out, int bytesAmount) throws IOException {
    byte[] buf = new byte[bytesAmount];
    int n = in.read(buf);
    if (n > 0) {
      out.write(buf, 0, n);
      return true;
    }
    return false;
  }
}
