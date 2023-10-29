// Copyright 2022 The Bazel Authors. All rights reserved.
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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Size;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.RetryException;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class RemoteCasWriter implements CasWriter {
  private final Set<String> workerSet;
  private final LoadingCache<String, Instance> workerStubs;
  private final Retrier retrier;

  public RemoteCasWriter(
      Set<String> workerSet, LoadingCache<String, Instance> workerStubs, Retrier retrier) {
    this.workerSet = workerSet;
    this.workerStubs = workerStubs;
    this.retrier = retrier;
  }

  public void write(Digest digest, Path file) throws IOException, InterruptedException {
    if (digest.getSizeBytes() > 0) {
      insertFileToCasMember(digest, file);
    }
  }

  private void insertFileToCasMember(Digest digest, Path file)
      throws IOException, InterruptedException {
    try (InputStream in = Files.newInputStream(file)) {
      retrier.execute(() -> writeToCasMember(digest, in));
    } catch (RetryException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfUnchecked(cause);
      throw new IOException(cause);
    }
  }

  private long writeToCasMember(Digest digest, InputStream in)
      throws IOException, InterruptedException {
    // create a write for inserting into another CAS member.
    String workerName = getRandomWorker();
    Write write = getCasMemberWrite(digest, workerName);

    try {
      return streamIntoWriteFuture(in, write, digest).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfInstanceOf(cause, IOException.class);
      // prevent a discard of this frame
      Status status = Status.fromThrowable(cause);
      throw new IOException(status.asException());
    }
  }

  private Write getCasMemberWrite(Digest digest, String workerName) throws IOException {
    Instance casMember = workerStub(workerName);

    return casMember.getBlobWrite(
        Compressor.Value.IDENTITY, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
  }

  public void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {
    try (InputStream in = content.newInput()) {
      retrier.execute(() -> writeToCasMember(digest, in));
    } catch (RetryException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfUnchecked(cause);
      throw new IOException(cause);
    }
  }

  private String getRandomWorker() throws IOException {
    synchronized (workerSet) {
      if (workerSet.isEmpty()) {
        throw new IOException("no available workers");
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

  private Instance workerStub(String worker) {
    try {
      return workerStubs.get(worker);
    } catch (ExecutionException e) {
      log.log(Level.SEVERE, "error getting worker stub for " + worker, e.getCause());
      throw new IllegalStateException("stub instance creation must not fail");
    }
  }

  private ListenableFuture<Long> streamIntoWriteFuture(InputStream in, Write write, Digest digest)
      throws IOException {
    SettableFuture<Long> writtenFuture = SettableFuture.create();
    int chunkSizeBytes = (int) Size.kbToBytes(128);

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
                  if (!copyBytes(in, outStream, chunkSizeBytes)) {
                    return;
                  }
                }

              } catch (IOException e) {
                if (!write.isComplete()) {
                  write.reset();
                  log.log(Level.SEVERE, "unexpected error transferring file for " + digest, e);
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
                  log.log(
                      Level.WARNING,
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

  private boolean copyBytes(InputStream in, OutputStream out, int bytesAmount) throws IOException {
    byte[] buf = new byte[bytesAmount];
    int n = in.read(buf);
    if (n > 0) {
      out.write(buf, 0, n);
      return true;
    }
    return false;
  }
}
