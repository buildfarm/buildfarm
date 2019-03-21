package build.buildfarm.server;

import static build.buildfarm.common.DigestUtil.HashFunction.SHA256;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class ByteStreamServiceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(SHA256);

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

  private Server fakeServer;
  private String fakeServerName;

  @Mock
  private Instances instances;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    fakeServerName = "fake server for " + getClass();
    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .addService(new ByteStreamService(instances))
            .directExecutor()
            .build()
            .start();
  }

  @After
  public void tearDown() throws Exception {
    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  @Test
  public void uploadsCanResetInLine() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(content);
    UUID uuid = UUID.randomUUID();

    SettableFuture<ByteString> writtenFuture = SettableFuture.create();
    ByteString.Output output = ByteString.newOutput((int) digest.getSizeBytes());
    OutputStream out = new OutputStream() {
      @Override
      public void close() {
        if (output.size() == digest.getSizeBytes()) {
          writtenFuture.set(output.toByteString());
        }
      }

      @Override
      public void flush() throws IOException {
        output.flush();
      }

      @Override
      public void write(byte[] b) throws IOException {
        output.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
      }

      @Override
      public void write(int b) throws IOException {
        output.write(b);
      }
    };

    Write write = mock(Write.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        output.reset();
        return null;
      }
    }).when(write).reset();
    when(write.getOutput()).thenReturn(out);
    doAnswer(invocation -> (long) output.size()).when(write).getCommittedSize();
    doAnswer(answerVoid((Runnable listener, Executor executor) -> writtenFuture.addListener(listener, executor)))
        .when(write).addListener(any(Runnable.class), any(Executor.class));

    Instance instance = mock(Instance.class);
    when(instance.getBlobWrite(digest, uuid)).thenReturn(write);

    String resourceName = ByteStreamUploader.getResourceName(uuid, /* instanceName=*/ null, digest);
    when(instances.getFromUploadBlob(eq(resourceName))).thenReturn(instance);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);
    FutureWriteResponseObserver futureResponder = new FutureWriteResponseObserver();
    StreamObserver<WriteRequest> requestObserver = service.write(futureResponder);

    ByteString shortContent = content.substring(0, 6);
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(0)
        .setResourceName(resourceName)
        .setData(shortContent)
        .build());
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(0)
        .setData(content)
        .setFinishWrite(true)
        .build());
    assertThat(futureResponder.get()).isEqualTo(WriteResponse.newBuilder()
        .setCommittedSize(content.size())
        .build());
    requestObserver.onCompleted();
    verify(write, atLeastOnce()).getCommittedSize();
    verify(write, atLeastOnce()).getOutput();
    verify(write, times(1)).reset();
    verify(write, times(1)).addListener(any(Runnable.class), any(Executor.class));
  }

  @Test
  public void uploadsCanProgressAfterCancellation() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(content);
    UUID uuid = UUID.randomUUID();

    SettableFuture<ByteString> writtenFuture = SettableFuture.create();
    ByteString.Output output = ByteString.newOutput((int) digest.getSizeBytes());
    OutputStream out = new OutputStream() {
      @Override
      public void close() {
        if (output.size() == digest.getSizeBytes()) {
          writtenFuture.set(output.toByteString());
        }
      }

      @Override
      public void write(byte[] b, int off, int len) {
        output.write(b, off, len);
      }

      @Override
      public void write(int b) {
        output.write(b);
      }
    };

    Write write = mock(Write.class);
    when(write.getOutput()).thenReturn(out);
    doAnswer(invocation -> (long) output.size()).when(write).getCommittedSize();
    doAnswer(answerVoid((Runnable listener, Executor executor) -> writtenFuture.addListener(listener, executor)))
        .when(write).addListener(any(Runnable.class), any(Executor.class));

    Instance instance = mock(Instance.class);
    when(instance.getBlobWrite(digest, uuid)).thenReturn(write);

    String resourceName = ByteStreamUploader.getResourceName(uuid, /* instanceName=*/ null, digest);
    when(instances.getFromUploadBlob(eq(resourceName))).thenReturn(instance);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);

    FutureWriteResponseObserver futureResponder = new FutureWriteResponseObserver();
    StreamObserver<WriteRequest> requestObserver = service.write(futureResponder);
    ByteString shortContent = content.substring(0, 6);
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(0)
        .setResourceName(resourceName)
        .setData(shortContent)
        .build());
    requestObserver.onError(Status.CANCELLED.asException());
    assertThat(futureResponder.isDone()).isTrue(); // should be done

    futureResponder = new FutureWriteResponseObserver();
    requestObserver = service.write(futureResponder);
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(6)
        .setResourceName(resourceName)
        .setData(content.substring(6))
        .setFinishWrite(true)
        .build());
    assertThat(futureResponder.get()).isEqualTo(WriteResponse.newBuilder()
        .setCommittedSize(content.size())
        .build());
    requestObserver.onCompleted();
    verify(write, atLeastOnce()).getCommittedSize();
    verify(write, atLeastOnce()).getOutput();
    verify(write, times(2)).addListener(any(Runnable.class), any(Executor.class));
  }
}
