package build.buildfarm.server;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.buildfarm.instance.Instance;
import build.buildfarm.server.ExecutionService.KeepaliveWatcher;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecutionServiceTest {
  private Instances instances;

  @Before
  public void setUp() throws Exception {
    instances = mock(Instances.class);
  }

  @Test
  public void keepaliveIsCancelledWithContext() throws Exception {
    ScheduledExecutorService keepaliveScheduler = newSingleThreadScheduledExecutor();
    ExecutionService service = new ExecutionService(
        instances,
        /* keepaliveAfter=*/ 1,
        /* keepaliveUnit=*/ SECONDS, // far enough in the future that we'll get scheduled and cancelled without executing
        keepaliveScheduler);
    Context.CancellableContext withCancellation = Context.current().withCancellation();
    StreamObserver<Operation> response = mock(StreamObserver.class);
    Operation operation = Operation.newBuilder()
        .setName("immediately-cancelled-watch-operation")
        .build();
    ListenableFuture<?> future = withCancellation.call(() -> {
      KeepaliveWatcher watcher = service.createWatcher(response);
      watcher.observe(operation);
      return watcher.getFuture();
    });
    assertThat(future).isNotNull();
    withCancellation.cancel(null);
    assertThat(future.isCancelled()).isTrue();
    assertThat(shutdownAndAwaitTermination(keepaliveScheduler, 1, SECONDS)).isTrue();
    // should only get one call for the real operation
    verify(response, times(1)).onNext(operation);
  }
}
