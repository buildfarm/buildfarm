package build.buildfarm.server;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.ChunkObserver;
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
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ByteStreamServiceTest {
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

  private static class ResettingChunkObserver implements ChunkObserver {
    private final SettableFuture<Long> committedFuture = SettableFuture.create();
    public long resetCount = 0;
    public long completedCount = 0;

    public ByteString content = ByteString.EMPTY;

    @Override
    public void onNext(ByteString chunk) {
      content = content.concat(chunk);
    }

    @Override
    public long getCommittedSize() {
      return content.size();
    }

    @Override
    public void reset() {
      resetCount++;
      content = ByteString.EMPTY;
    }

    @Override
    public void onCompleted() {
      completedCount++;
      committedFuture.set((long) content.size());
    }

    @Override
    public ListenableFuture<Long> getCommittedFuture() {
      return committedFuture;
    }

    @Override
    public void onError(Throwable t) {
      committedFuture.setException(t);
    }
  }

  @Test
  public void uploadsCanResetInLine() throws ExecutionException, InstanceNotFoundException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = Digest.newBuilder().setHash("my-hash").setSizeBytes(content.size()).build();

    ResettingChunkObserver chunkObserver = new ResettingChunkObserver();

    Instance instance = mock(Instance.class);

    String resourceName = ByteStreamUploader.getResourceName("test-upload", /* instanceName=*/ null, digest);
    when(instances.getFromBlob(eq(resourceName))).thenReturn(instance);
    when(instances.getFromUploadBlob(eq(resourceName))).thenReturn(instance);
    when(instance.getWriteBlobObserver(eq(digest))).thenReturn(chunkObserver);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);
    SettableFuture<WriteResponse> partialFuture = SettableFuture.create();
    StreamObserver<WriteRequest> requestObserver = service.write(new StreamObserver<WriteResponse>() {
      @Override
      public void onNext(WriteResponse response) {
        partialFuture.set(response);
      }

      @Override
      public void onCompleted() {
      }

      @Override
      public void onError(Throwable t) {
        partialFuture.setException(t);
      }
    });

    ByteString shortContent = content.substring(0, 6);
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(0)
        .setResourceName(resourceName)
        .setData(shortContent)
        .build());
    assertThat(chunkObserver.content).isEqualTo(shortContent);
    requestObserver.onNext(WriteRequest.newBuilder()
        .setWriteOffset(0)
        .setData(content)
        .setFinishWrite(true)
        .build());
    requestObserver.onCompleted();
    assertThat(chunkObserver.content).isEqualTo(content);
    assertThat(chunkObserver.resetCount).isEqualTo(1);
    assertThat(chunkObserver.completedCount).isEqualTo(1);
    assertThat(partialFuture.get()).isEqualTo(WriteResponse.newBuilder()
        .setCommittedSize(content.size())
        .build());
  }
}
