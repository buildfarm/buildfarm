package build.buildfarm.common.services;

import static build.buildfarm.common.resources.ResourceParser.uploadResourceName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.WriteCompleteException;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.resources.BlobInformation;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class WriteStreamObserverTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void cancelledBeforeGetOutputIsSilent() throws Exception {
    CancellableContext context = Context.current().withCancellation();
    Instance instance = mock(Instance.class);
    StreamObserver<WriteResponse> responseObserver = mock(StreamObserver.class);
    ByteString cancelled = ByteString.copyFromUtf8("cancelled data");
    Digest cancelledDigest = DIGEST_UTIL.compute(cancelled);
    UUID uuid = UUID.randomUUID();
    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(cancelledDigest))
            .setUuid(uuid.toString())
            .build();
    SettableFuture<Long> future = SettableFuture.create();
    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    FeedbackOutputStream out = mock(FeedbackOutputStream.class);
    doAnswer(
            (Answer<FeedbackOutputStream>)
                invocation -> {
                  context.cancel(new RuntimeException("Cancelled by test"));
                  return out;
                })
        .when(write)
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);

    WriteStreamObserver observer =
        context.call(
            () -> new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver));
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(cancelled)
            .setFinishWrite(true)
            .build());
    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verify(write, times(1))
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(out, times(1)).close();
    verifyNoInteractions(responseObserver);
  }

  @Test
  public void noErrorWhenContextCancelled() throws Exception {
    CancellableContext context = Context.current().withCancellation();
    Instance instance = mock(Instance.class);
    StreamObserver<WriteResponse> responseObserver = mock(StreamObserver.class);
    ByteString cancelled = ByteString.copyFromUtf8("cancelled data");
    Digest cancelledDigest = DIGEST_UTIL.compute(cancelled);
    UUID uuid = UUID.randomUUID();
    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(cancelledDigest))
            .setUuid(uuid.toString())
            .build();
    SettableFuture<Long> future = SettableFuture.create();
    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenReturn(Boolean.TRUE);
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);

    WriteStreamObserver observer =
        context.call(
            () -> new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver));
    context.run(
        () ->
            observer.onNext(
                WriteRequest.newBuilder()
                    .setResourceName(uploadResourceName(uploadBlobRequest))
                    .setData(cancelled)
                    .build()));
    context.cancel(new RuntimeException("Cancelled by test"));
    future.setException(new IOException("test cancel"));

    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verifyNoInteractions(responseObserver);
  }

  @Test
  public void noWriteOnAlreadyCompleted() throws Exception {
    ByteString completed = ByteString.copyFromUtf8("Write already completed");
    Digest completedDigest = DIGEST_UTIL.compute(completed);
    UUID uuid = UUID.randomUUID();
    Instance instance = mock(Instance.class);
    Write write = mock(Write.class);
    SettableFuture<Long> future = SettableFuture.create();
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenAnswer((Answer<Boolean>) invocation -> future.isDone());
    when(instance.getBlobWrite(
            eq(Compressor.Value.ZSTD), eq(completedDigest), eq(uuid), any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);
    StreamObserver<WriteResponse> responseObserver = mock(StreamObserver.class);

    // Mark write complete on getCommittedSize() call.
    doAnswer(
            invocation -> {
              long committed = Write.COMPRESSED_EXPECTED_SIZE;
              future.set(committed);
              return committed;
            })
        .when(write)
        .getCommittedSize();

    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(
                BlobInformation.newBuilder()
                    .setCompressor(Compressor.Value.ZSTD)
                    .setDigest(completedDigest))
            .setUuid(uuid.toString())
            .build();
    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(completed)
            .setFinishWrite(true)
            .build());
    observer.onCompleted();

    // verify that write is not called on already completed write
    verify(outputStream, never()).write(completed.toByteArray());
    verify(responseObserver, times(1)).onNext(any(WriteResponse.class));
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
  }

  @Test
  public void waitForFutureOnComplete() throws Exception {
    ByteString completed = ByteString.copyFromUtf8("Write already completed");
    Digest completedDigest = DIGEST_UTIL.compute(completed);
    SettableFuture<Long> future = SettableFuture.create();

    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenAnswer((Answer<Boolean>) invocation -> future.isDone());
    doAnswer(
            invocation -> {
              future.set(completedDigest.getSize());
              throw new WriteCompleteException();
            })
        .when(write)
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    UUID uuid = UUID.randomUUID();
    Instance instance = mock(Instance.class);
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(completedDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);

    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(completedDigest))
            .setUuid(uuid.toString())
            .build();
    StreamObserver<WriteResponse> responseObserver = mock(StreamObserver.class);
    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(completed)
            .setFinishWrite(true)
            .build());

    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(completedDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verify(write, atLeastOnce()).getFuture();
    verify(write, times(1))
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(responseObserver, times(1)).onNext(any(WriteResponse.class));
    verify(responseObserver, times(1)).onCompleted();
    verifyNoMoreInteractions(responseObserver);
  }
}
