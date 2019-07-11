// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.common.DigestUtil;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class Extract {
  static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];

    Scanner scanner = new Scanner(System.in);

    ImmutableSet.Builder<Digest> actionDigests = ImmutableSet.builder();
    while (scanner.hasNext()) {
      actionDigests.add(DigestUtil.parseDigest(scanner.nextLine()));
    }

    ManagedChannel channel = createChannel(host);

    Path root = Paths.get("blobs");

    downloadActionContents(root, instanceName, actionDigests.build(), channel);

    channel.shutdown();
  }

  static String blobName(String instanceName, Digest digest) {
    return String.format("%s/blobs/%s/%d", instanceName, digest.getHash(), digest.getSizeBytes());
  }

  static Runnable blobGetter(Path root, String instanceName, Digest digest, ByteStreamStub bsStub, AtomicLong outstandingOperations) {
    if (digest.getSizeBytes() == 0) {
      return () -> outstandingOperations.getAndDecrement();
    }
    return new Runnable() {
      @Override
      public void run() {
        System.out.println("Getting blob " + digest.getHash());
        bsStub.read(
            ReadRequest.newBuilder()
                .setResourceName(blobName(instanceName, digest))
                .build(),
            new StreamObserver<ReadResponse>() {
              OutputStream out = null;

              void initialize() {
                if (out == null) {
                  try {
                    out = Files.newOutputStream(root.resolve(digest.getHash()));
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              }

              void finish() {
                if (out != null) {
                  try {
                    out.close();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
                outstandingOperations.getAndDecrement();
              }

              @Override
              public void onNext(ReadResponse response) {
                initialize();
                try {
                  response.getData().writeTo(out);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
                finish();
              }

              @Override
              public void onCompleted() {
                finish();
              }
            });
      }
    };
  }

  static ByteString getBlob(String instanceName, Digest digest, ByteStreamStub bsStub) throws InterruptedException {
    SettableFuture<ByteString> blobFuture = SettableFuture.create();
    bsStub.read(
        ReadRequest.newBuilder()
            .setResourceName(blobName(instanceName, digest))
            .build(),
        new StreamObserver<ReadResponse>() {
          ByteString.Output out = ByteString.newOutput((int) digest.getSizeBytes());

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
        throw (RuntimeException) e.getCause();
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
      AtomicLong outstandingOperations) {
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
        System.out.println("Getting directory " + digest.getHash());
        ByteString content = getBlob(instanceName, digest, bsStub);
        try {
          runIO(content);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      void runIO(ByteString content) throws IOException {
        Path file = root.resolve(digest.getHash());
        try (OutputStream out = Files.newOutputStream(file)) {
          content.writeTo(out);
        }
        Directory directory = Directory.parseFrom(content);
        for (FileNode fileNode : directory.getFilesList()) {
          Digest fileDigest = fileNode.getDigest();
          if (!visitedDigests.contains(fileDigest)) {
            visitedDigests.add(fileDigest);
            outstandingOperations.getAndIncrement();
            executor.execute(blobGetter(root, instanceName, fileDigest, bsStub, outstandingOperations));
          }
        }

        for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
          Digest directoryDigest = directoryNode.getDigest();
          // we may have seen this digest, but now we will have seen it as a directory
          if (!visitedDirectories.contains(directoryDigest)) {
            // probably won't collide with other writers, with single thread
            visitedDigests.add(directoryDigest);
            visitedDirectories.add(directoryDigest);
            outstandingOperations.getAndIncrement();
            executor.execute(directoryGetter(root, instanceName, directoryDigest, visitedDirectories, visitedDigests, bsStub, executor, outstandingOperations));
          }
        }
      }
    };
  }

  static void downloadActionContents(Path root, String instanceName, Set<Digest> actionDigests, Channel channel) throws IOException, InterruptedException {
    ByteStreamStub bsStub = ByteStreamGrpc.newStub(channel);

    ExecutorService service = newSingleThreadExecutor();

    Set<Digest> visitedDigests = Sets.newHashSet();
    Set<Digest> visitedDirectories = Sets.newHashSet();

    AtomicLong outstandingOperations = new AtomicLong(0);

    for (Digest actionDigest : actionDigests) {
      System.out.println("Getting action " + actionDigest.getHash());
      ByteString content = getBlob(instanceName, actionDigest, bsStub);
      Path file = root.resolve(actionDigest.getHash());
      try (OutputStream out = Files.newOutputStream(file)) {
        content.writeTo(out);
      }
      Action action = Action.parseFrom(content);
      Digest commandDigest = action.getCommandDigest();
      if (!visitedDigests.contains(commandDigest)) {
        visitedDigests.add(commandDigest);
        outstandingOperations.getAndIncrement();
        service.execute(blobGetter(root, instanceName, commandDigest, bsStub, outstandingOperations));
      }
      Digest inputRootDigest = action.getInputRootDigest();
      if (!visitedDigests.contains(inputRootDigest)) {
        visitedDirectories.add(inputRootDigest);
        visitedDigests.add(inputRootDigest);
        outstandingOperations.getAndIncrement();
        service.execute(directoryGetter(root, instanceName, inputRootDigest, visitedDirectories, visitedDigests, bsStub, service, outstandingOperations));
      }
    }

    while (outstandingOperations.get() > 0) {
      System.out.println("Waiting on " + outstandingOperations.get() + " operations");
      TimeUnit.SECONDS.sleep(5);
    }

    service.shutdown();
    service.awaitTermination(1, TimeUnit.MINUTES);
  }
}
