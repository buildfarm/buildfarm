// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.stub;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.ProgressiveBackoff;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamFutureStub;
import com.google.bytestream.ByteStreamProto;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

/**
 * A client implementing the {@code Write} method of the {@code ByteStream} gRPC service.
 *
 * <p>Users must call {@link #shutdown()} before exiting.
 */
@Log
public class ByteStreamUploader {
  private final String instanceName;
  private final Channel channel;
  private final CallCredentials callCredentials;
  private final long callTimeoutSecs;
  private final Retrier retrier;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<HashCode, ListenableFuture<Void>> uploadsInProgress = new HashMap<>();

  @GuardedBy("lock")
  private boolean isShutdown;

  /**
   * Creates a new instance.
   *
   * @param instanceName the instance name to be prepended to resource name of the {@code Write}
   *     call. See the {@code ByteStream} service definition for details
   * @param channel the {@link io.grpc.Channel} to use for calls
   * @param callCredentials the credentials to use for authentication. May be {@code null}, in which
   *     case no authentication is performed
   * @param callTimeoutSecs the timeout in seconds after which a {@code Write} gRPC call must be
   *     complete. The timeout resets between retries
   * @param retrier the {@link Retrier} whose backoff strategy to use for retry timings.
   */
  public ByteStreamUploader(
      @Nullable String instanceName,
      Channel channel,
      @Nullable CallCredentials callCredentials,
      long callTimeoutSecs,
      Retrier retrier) {
    checkArgument(callTimeoutSecs > 0, "callTimeoutSecs must be gt 0.");

    this.instanceName = instanceName;
    this.channel = channel;
    this.callCredentials = callCredentials;
    this.callTimeoutSecs = callTimeoutSecs;
    this.retrier = retrier;
  }

  /**
   * Uploads a BLOB, as provided by the {@link Chunker}, to the remote {@code ByteStream} service.
   * The call blocks until the upload is complete, or throws an {@link Exception} in case of error.
   *
   * <p>Uploads are retried according to the specified {@link Retrier}. Retrying is transparent to
   * the user of this API.
   *
   * <p>Trying to upload the same BLOB multiple times concurrently, results in only one upload being
   * performed. This is transparent to the user of this API.
   *
   * @throws IOException when the upload failed due to content issues
   */
  public void uploadBlob(HashCode hash, Chunker chunker) throws IOException, InterruptedException {
    uploadBlobs(singletonMap(hash, chunker));
  }

  /**
   * Uploads a list of BLOBs concurrently to the remote {@code ByteStream} service. The call blocks
   * until the upload of all BLOBs is complete, or throws an {@link Exception} after the first
   * upload failed. Any other uploads will continue uploading in the background, until they complete
   * or the {@link #shutdown()} method is called. Errors encountered by these uploads are swallowed.
   *
   * <p>Uploads are retried according to the specified {@link Retrier}. Retrying is transparent to
   * the user of this API.
   *
   * <p>Trying to upload the same BLOB multiple times concurrently, results in only one upload being
   * performed. This is transparent to the user of this API.
   *
   * @throws IOException when the upload failed due to content issues
   */
  public void uploadBlobs(Map<HashCode, Chunker> chunkers)
      throws IOException, InterruptedException {
    List<ListenableFuture<Void>> uploads =
        Lists.newArrayListWithCapacity(chunkers.entrySet().size());

    for (Map.Entry<HashCode, Chunker> chunkerEntry : chunkers.entrySet()) {
      uploads.add(uploadBlobAsync(chunkerEntry.getKey(), chunkerEntry.getValue()));
    }

    try {
      for (ListenableFuture<Void> upload : uploads) {
        upload.get();
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      //noinspection deprecation
      propagateIfInstanceOf(cause, IOException.class);
      throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  /**
   * Cancels all running uploads. The method returns immediately and does NOT wait for the uploads
   * to be cancelled.
   *
   * <p>This method must be the last method called.
   */
  public void shutdown() {
    synchronized (lock) {
      if (isShutdown) {
        return;
      }
      isShutdown = true;
      // Before cancelling, copy the futures to a separate list in order to avoid concurrently
      // iterating over and modifying the map (cancel triggers a listener that removes the entry
      // from the map. the listener is executed in the same thread.).
      List<Future<Void>> uploadsToCancel = Lists.newArrayList(uploadsInProgress.values());
      for (Future<Void> upload : uploadsToCancel) {
        upload.cancel(true);
      }
    }
  }

  @VisibleForTesting
  ListenableFuture<Void> uploadBlobAsync(HashCode hash, Chunker chunker) {
    synchronized (lock) {
      checkState(!isShutdown, "Must not call uploadBlobs after shutdown.");

      ListenableFuture<Void> uploadResult = uploadsInProgress.get(hash);
      if (uploadResult == null) {
        uploadResult = startAsyncUpload(hash, chunker);
        uploadsInProgress.put(hash, uploadResult);
        uploadResult.addListener(
            () -> {
              synchronized (lock) {
                uploadsInProgress.remove(hash);
              }
            },
            MoreExecutors.directExecutor());
      }
      return uploadResult;
    }
  }

  @VisibleForTesting
  boolean uploadsInProgress() {
    synchronized (lock) {
      return !uploadsInProgress.isEmpty();
    }
  }

  public static String uploadResourceName(
      String instanceName, UUID uuid, HashCode hash, long size) {
    String resourceName = format("uploads/%s/blobs/%s/%d", uuid, hash, size);
    if (!Strings.isNullOrEmpty(instanceName)) {
      resourceName = instanceName + "/" + resourceName;
    }
    return resourceName;
  }

  private ListenableFuture<Void> startAsyncUpload(HashCode hash, Chunker chunker) {
    try {
      chunker.reset();
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }

    UUID uploadId = UUID.randomUUID();
    String resourceName = uploadResourceName(instanceName, uploadId, hash, chunker.getSize());
    AsyncUpload newUpload =
        new AsyncUpload(channel, callCredentials, callTimeoutSecs, retrier, resourceName, chunker);
    ListenableFuture<Void> currUpload = newUpload.start();
    currUpload.addListener(
        () -> {
          if (currUpload.isCancelled()) {
            newUpload.cancel();
          }
        },
        MoreExecutors.directExecutor());
    return currUpload;
  }

  private static class AsyncUpload {
    private final Channel channel;
    private final CallCredentials callCredentials;
    private final long callTimeoutSecs;
    private final Retrier retrier;
    private final String resourceName;
    private final Chunker chunker;

    private ClientCall<WriteRequest, WriteResponse> call;

    AsyncUpload(
        Channel channel,
        CallCredentials callCredentials,
        long callTimeoutSecs,
        Retrier retrier,
        String resourceName,
        Chunker chunker) {
      this.channel = channel;
      this.callCredentials = callCredentials;
      this.callTimeoutSecs = callTimeoutSecs;
      this.retrier = retrier;
      this.resourceName = resourceName;
      this.chunker = chunker;
    }

    ListenableFuture<Void> start() {
      ProgressiveBackoff progressiveBackoff = new ProgressiveBackoff(retrier::newBackoff);
      AtomicLong committedOffset = new AtomicLong(0);
      return Futures.transformAsync(
          retrier.executeAsync(
              () -> callAndQueryOnFailure(committedOffset, progressiveBackoff), progressiveBackoff),
          (result) -> {
            long committedSize = committedOffset.get();
            long expected = chunker.getSize();
            if (committedSize != expected) {
              String message =
                  format(
                      "write incomplete: committed_size %d for %d total", committedSize, expected);
              return Futures.immediateFailedFuture(new IOException(message));
            }
            return Futures.immediateFuture(null);
          },
          MoreExecutors.directExecutor());
    }

    private ByteStreamFutureStub bsFutureStub() {
      return ByteStreamGrpc.newFutureStub(channel)
          .withCallCredentials(callCredentials)
          .withDeadlineAfter(callTimeoutSecs, SECONDS);
    }

    private ListenableFuture<Void> callAndQueryOnFailure(
        AtomicLong committedOffset, ProgressiveBackoff progressiveBackoff) {
      return Futures.catchingAsync(
          call(committedOffset),
          Exception.class,
          (e) -> guardQueryWithSuppression(e, committedOffset, progressiveBackoff),
          MoreExecutors.directExecutor());
    }

    @SuppressWarnings("ConstantConditions")
    private ListenableFuture<Void> guardQueryWithSuppression(
        Exception e, AtomicLong committedOffset, ProgressiveBackoff progressiveBackoff) {
      // we are destined to return this, avoid recreating it
      ListenableFuture<Void> exceptionFuture = Futures.immediateFailedFuture(e);

      // FIXME we should also return immediately without the query if
      // we were out of retry attempts for the underlying backoff. This
      // is meant to be an only in-between-retries query request.
      if (!retrier.isRetriable(Status.fromThrowable(e))) {
        return exceptionFuture;
      }

      ListenableFuture<Void> suppressedQueryFuture =
          Futures.catchingAsync(
              query(committedOffset, progressiveBackoff),
              Throwable.class,
              (t) -> {
                // if the query threw an exception, add it to the suppressions
                // for the destined exception
                e.addSuppressed(t);
                return exceptionFuture;
              },
              MoreExecutors.directExecutor());
      return Futures.transformAsync(
          suppressedQueryFuture, (result) -> exceptionFuture, MoreExecutors.directExecutor());
    }

    @SuppressWarnings("ConstantConditions")
    private ListenableFuture<Void> query(
        AtomicLong committedOffset, ProgressiveBackoff progressiveBackoff) {
      ListenableFuture<Long> committedSizeFuture =
          Futures.transform(
              bsFutureStub()
                  .queryWriteStatus(
                      QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build()),
              ByteStreamProto.QueryWriteStatusResponse::getCommittedSize,
              MoreExecutors.directExecutor());
      return Futures.transformAsync(
          committedSizeFuture,
          (committedSize) -> {
            if (committedSize > committedOffset.get()) {
              progressiveBackoff.reset();
            }
            committedOffset.set(committedSize);
            return Futures.immediateFuture(null);
          },
          MoreExecutors.directExecutor());
    }

    private ListenableFuture<Void> call(AtomicLong committedOffset) {
      CallOptions callOptions =
          CallOptions.DEFAULT
              .withCallCredentials(callCredentials)
              .withDeadlineAfter(callTimeoutSecs, SECONDS);
      call = channel.newCall(ByteStreamGrpc.getWriteMethod(), callOptions);

      try {
        chunker.seek(committedOffset.get());
      } catch (IOException e) {
        try {
          chunker.reset();
        } catch (IOException resetException) {
          e.addSuppressed(resetException);
        }
        return Futures.immediateFailedFuture(e);
      }

      SettableFuture<Void> uploadResult = SettableFuture.create();
      ClientCall.Listener<WriteResponse> callListener =
          new ClientCall.Listener<WriteResponse>() {
            private final WriteRequest.Builder requestBuilder = WriteRequest.newBuilder();
            private volatile boolean callHalfClosed = false;

            void halfClose() {
              // call.halfClose() may only be called once. Guard against it being called more
              // often.
              // See: https://github.com/grpc/grpc-java/issues/3201
              if (!callHalfClosed) {
                callHalfClosed = true;
                // Every chunk has been written. No more work to do.
                call.halfClose();
              }
            }

            @Override
            public void onMessage(WriteResponse response) {
              // upload was completed either by us or someone else
              committedOffset.set(response.getCommittedSize());
              halfClose();
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              try {
                // If upload was completed by someone else or already present, then chunker does
                // does not close the Filehandle. Do it here to be sure it's always closed.
                chunker.reset();
              } catch (IOException e) {
                // This exception indicates that closing the underlying input stream failed.
                // We don't expect this to ever happen, but don't want to swallow the exception
                // completely.
                log.log(Level.SEVERE, "Chunker failed closing data source", e);
              }

              if (status.isOk() || Code.ALREADY_EXISTS.equals(status.getCode())) {
                uploadResult.set(null);
              } else {
                uploadResult.setException(status.asRuntimeException());
              }
            }

            @Override
            public void onReady() {
              while (call.isReady()) {
                if (chunker.hasNext()) {
                  halfClose();
                  return;
                }

                try {
                  requestBuilder.clear();
                  Chunker.Chunk chunk = chunker.next();

                  if (callHalfClosed) {
                    return;
                  }

                  if (chunk.getOffset() == committedOffset.get()) {
                    // Resource name only needs to be set on the first write for each file.
                    requestBuilder.setResourceName(resourceName);
                  }

                  boolean isLastChunk = chunker.hasNext();
                  WriteRequest request =
                      requestBuilder
                          .setData(chunk.getData())
                          .setWriteOffset(chunk.getOffset())
                          .setFinishWrite(isLastChunk)
                          .build();

                  call.sendMessage(request);
                } catch (IOException e) {
                  try {
                    chunker.reset();
                  } catch (IOException e1) {
                    // This exception indicates that closing the underlying input stream failed.
                    // We don't expect this to ever happen, but don't want to swallow the exception
                    // completely.
                    log.log(Level.SEVERE, format("Chunker failed closing data source: %s", e1));
                  } finally {
                    call.cancel("Failed to read next chunk.", e);
                  }
                }
              }
            }
          };
      call.start(callListener, new Metadata());
      call.request(1);
      return uploadResult;
    }

    void cancel() {
      if (call != null) {
        call.cancel("Cancelled by user.", null);
      }
    }
  }
}
