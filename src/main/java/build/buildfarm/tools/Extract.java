// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.v1test.Digest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class Extract {
  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];

    Scanner scanner = new Scanner(System.in);

    ImmutableSet.Builder<Digest> actionDigests = ImmutableSet.builder();
    while (scanner.hasNext()) {
      actionDigests.add(DigestUtil.parseDigest(scanner.nextLine()));
    }

    ManagedChannel channel = createChannel(host);

    Path root = Path.of("blobs");

    downloadActionContents(root, instanceName, actionDigests.build(), channel);

    channel.shutdown();
  }

  static String blobName(String instanceName, Digest digest) {
    return String.format("%s/blobs/%s", instanceName, DigestUtil.toString(digest));
  }

  static InputStream newInput(
      String instanceName,
      Digest digest,
      ByteStreamStub bsStub,
      ListeningScheduledExecutorService retryService)
      throws IOException {
    return ByteStreamHelper.newInput(
        blobName(instanceName, digest),
        0,
        "endpoint",
        () -> bsStub.withDeadlineAfter(10, TimeUnit.SECONDS),
        Retrier.Backoff.exponential(Duration.ofSeconds(0), Duration.ofSeconds(0), 2, 0, 5),
        Retrier.DEFAULT_IS_RETRIABLE,
        retryService);
  }

  static Runnable blobGetter(
      Path root,
      String instanceName,
      Digest digest,
      ByteStreamStub bsStub,
      AtomicLong outstandingOperations,
      ListeningScheduledExecutorService retryService) {
    if (digest.getSize() == 0) {
      return outstandingOperations::getAndDecrement;
    }
    return () -> {
      Path file = root.resolve(digest.getHash());
      try {
        if (!Files.exists(file) || Files.size(file) != digest.getSize()) {
          try (OutputStream out = Files.newOutputStream(file)) {
            try (InputStream in = newInput(instanceName, digest, bsStub, retryService)) {
              ByteStreams.copy(in, out);
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      outstandingOperations.getAndDecrement();
    };
  }

  static ByteString getBlobIntoFile(
      String type, String instanceName, Digest digest, ByteStreamStub bsStub, Path root)
      throws IOException, InterruptedException {
    Path file = root.resolve(digest.getHash());
    if (Files.exists(file) && Files.size(file) == digest.getSize()) {
      try (InputStream in = Files.newInputStream(file)) {
        return ByteString.readFrom(in);
      }
    }
    System.out.println("Getting " + type + " " + digest.getHash() + "/" + digest.getSize());
    ByteString content = getBlob(instanceName, digest, bsStub);
    try (OutputStream out = Files.newOutputStream(file)) {
      content.writeTo(out);
    }
    return content;
  }

  static ByteString getBlob(String instanceName, Digest digest, ByteStreamStub bsStub)
      throws InterruptedException {
    SettableFuture<ByteString> blobFuture = SettableFuture.create();
    bsStub.read(
        ReadRequest.newBuilder().setResourceName(blobName(instanceName, digest)).build(),
        new StreamObserver<ReadResponse>() {
          final ByteString.Output out = ByteString.newOutput((int) digest.getSize());

          @Override
          public void onNext(ReadResponse response) {
            try {
              response.getData().writeTo(out);
            } catch (IOException e) {
              blobFuture.setException(e);
            }
          }

          @Override
          public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() == Code.NOT_FOUND) {
              t = new NoSuchFileException(digest.getHash() + "/" + digest.getSize());
            }
            blobFuture.setException(t);
          }

          @Override
          public void onCompleted() {
            blobFuture.set(out.toByteString());
          }
        });
    try {
      return blobFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw new RuntimeException(e.getCause());
      }
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  static Runnable directoryGetter(
      Path root,
      String instanceName,
      Digest digest,
      Set<Digest> visitedDirectories,
      Set<Digest> visitedDigests,
      ByteStreamStub bsStub,
      Executor executor,
      AtomicLong outstandingOperations,
      ListeningScheduledExecutorService retryService) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          runInterruptibly();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          outstandingOperations.getAndDecrement();
        }
      }

      void runInterruptibly() throws InterruptedException {
        try {
          ByteString content = getBlobIntoFile("directory", instanceName, digest, bsStub, root);
          handleDirectory(Directory.parseFrom(content));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      void handleDirectory(Directory directory) {
        for (FileNode fileNode : directory.getFilesList()) {
          Digest fileDigest =
              DigestUtil.fromDigest(fileNode.getDigest(), digest.getDigestFunction());
          if (!visitedDigests.contains(fileDigest)) {
            visitedDigests.add(fileDigest);
            outstandingOperations.getAndIncrement();
            executor.execute(
                blobGetter(
                    root, instanceName, fileDigest, bsStub, outstandingOperations, retryService));
          }
        }

        for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
          Digest directoryDigest =
              DigestUtil.fromDigest(directoryNode.getDigest(), digest.getDigestFunction());
          // we may have seen this digest, but now we will have seen it as a directory
          if (!visitedDirectories.contains(directoryDigest)) {
            // probably won't collide with other writers, with single thread
            visitedDigests.add(directoryDigest);
            visitedDirectories.add(directoryDigest);
            outstandingOperations.getAndIncrement();
            executor.execute(
                directoryGetter(
                    root,
                    instanceName,
                    directoryDigest,
                    visitedDirectories,
                    visitedDigests,
                    bsStub,
                    executor,
                    outstandingOperations,
                    retryService));
          }
        }
      }
    };
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  static void downloadActionContents(
      Path root, String instanceName, Set<Digest> actionDigests, Channel channel)
      throws IOException, InterruptedException {
    ByteStreamStub bsStub = ByteStreamGrpc.newStub(channel);

    ExecutorService service = newSingleThreadExecutor();
    ListeningScheduledExecutorService retryService =
        listeningDecorator(newSingleThreadScheduledExecutor());

    Set<Digest> visitedDigests = Sets.newHashSet();
    Set<Digest> visitedDirectories = Sets.newHashSet();

    AtomicLong outstandingOperations = new AtomicLong(0);

    for (Digest actionDigest : actionDigests) {
      ByteString content = getBlobIntoFile("action", instanceName, actionDigest, bsStub, root);
      Action action = Action.parseFrom(content);
      Digest commandDigest =
          DigestUtil.fromDigest(action.getCommandDigest(), actionDigest.getDigestFunction());
      if (!visitedDigests.contains(commandDigest)) {
        visitedDigests.add(commandDigest);
        outstandingOperations.getAndIncrement();
        service.execute(
            blobGetter(
                root, instanceName, commandDigest, bsStub, outstandingOperations, retryService));
      }
      Digest inputRootDigest =
          DigestUtil.fromDigest(action.getInputRootDigest(), actionDigest.getDigestFunction());
      if (!visitedDigests.contains(inputRootDigest)) {
        visitedDirectories.add(inputRootDigest);
        visitedDigests.add(inputRootDigest);
        outstandingOperations.getAndIncrement();
        service.execute(
            directoryGetter(
                root,
                instanceName,
                inputRootDigest,
                visitedDirectories,
                visitedDigests,
                bsStub,
                service,
                outstandingOperations,
                retryService));
      }
    }

    while (outstandingOperations.get() > 0) {
      System.out.println("Waiting on " + outstandingOperations.get() + " operations");
      TimeUnit.SECONDS.sleep(5);
    }

    service.shutdown();
    service.awaitTermination(1, TimeUnit.MINUTES);
    retryService.shutdown();
    retryService.awaitTermination(1, TimeUnit.MINUTES);
  }
}
