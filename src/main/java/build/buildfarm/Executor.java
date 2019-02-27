package build.buildfarm;

import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.EXECUTING;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionStub;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Executor {
  static class ExecutionObserver implements StreamObserver<Operation> {
    private final AtomicLong countdown;
    private final AtomicInteger[] statusCounts;
    private final ExecutionStub execStub;
    private final String instanceName;
    private final Digest actionDigest;
    private final Stopwatch stopwatch;
    private final ScheduledExecutorService service;

    private String operationName = null;
    private ScheduledFuture<?> noticeFuture;

    ExecutionObserver(
        AtomicLong countdown,
        AtomicInteger[] statusCounts,
        ExecutionStub execStub,
        String instanceName,
        Digest actionDigest,
        ScheduledExecutorService service) {
      this.countdown = countdown;
      this.statusCounts = statusCounts;
      this.execStub = execStub;
      this.instanceName = instanceName;
      this.actionDigest = actionDigest;
      this.service = service;
      stopwatch = Stopwatch.createStarted();
    }

    void execute() {
      execStub.execute(
          ExecuteRequest.newBuilder()
              .setInstanceName(instanceName)
              .setActionDigest(actionDigest)
              .setSkipCacheLookup(true)
              .build(),
          this);
      noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
    }

    void print(int code, long micros) {
      System.out.println(
          String.format(
              "Action: %s -> %s: %s in %gms",
              DigestUtil.toString(actionDigest),
              operationName,
              Code.forNumber(code),
              micros / 1000.0f));
    }

    void printStillWaiting() {
      noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
      if (operationName == null) {
        System.out.println("StillWaitingFor Operation => " + DigestUtil.toString(actionDigest));
      } else {
        System.out.println("StillWaitingFor Results => " + operationName);
      }
    }

    @Override
    public void onNext(Operation operation) {
      noticeFuture.cancel(false);
      if (operationName == null) {
        operationName = operation.getName();
      }
      long micros = stopwatch.elapsed(MICROSECONDS);
      if (operation.getResponse().is(ExecuteResponse.class)) {
        try {
          ExecuteResponse response = operation.getResponse().unpack(ExecuteResponse.class);
          int code = response.getStatus().getCode();
          print(code, micros);
          statusCounts[code].incrementAndGet();
        } catch (InvalidProtocolBufferException e) {
          System.err.println("An unlikely error has occurred: " + e.getMessage());
        }
      } else {
        try {
          ExecuteOperationMetadata metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
          if (metadata.getStage() == EXECUTING) {
            stopwatch.reset().start();
          }
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
      }
      noticeFuture = service.schedule(this::printStillWaiting, 30, SECONDS);
    }

    @Override
    public void onError(Throwable t) {
      noticeFuture.cancel(false);
      long micros = stopwatch.elapsed(MICROSECONDS);
      int code = io.grpc.Status.fromThrowable(t).getCode().value();
      print(code, micros);
      statusCounts[code].incrementAndGet();
      countdown.decrementAndGet();
    }

    @Override
    public void onCompleted() {
      noticeFuture.cancel(false);
      countdown.decrementAndGet();
    }
  }

  static void executeActions(String instanceName, List<Digest> actionDigests, ExecutionStub execStub) throws InterruptedException {
    ScheduledExecutorService service = newSingleThreadScheduledExecutor();

    AtomicInteger[] statusCounts = new AtomicInteger[18];
    for (int i = 0; i < statusCounts.length; i++) {
      statusCounts[i] = new AtomicInteger(0);
    }
    AtomicLong countdown = new AtomicLong(actionDigests.size());
    for (Digest actionDigest : actionDigests) {
      ExecutionObserver executionObserver =
          new ExecutionObserver(countdown, statusCounts, execStub, instanceName, actionDigest, service);
      executionObserver.execute();
    }
    while (countdown.get() != 0) {
      SECONDS.sleep(1);
    }
    for (int i = 0; i < statusCounts.length; i++) {
      AtomicInteger statusCount = statusCounts[i];
      if (statusCount.get() != 0) {
        System.out.println("Status " + Code.forNumber(i) + " : " + statusCount.get() + " responses");
      }
    }

    shutdownAndAwaitTermination(service, 1, SECONDS);
  }

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  public static void main(String[] args) throws InterruptedException {
    String host = args[0];
    String instanceName = args[1];

    Scanner scanner = new Scanner(System.in);

    ImmutableList.Builder<Digest> actionDigests = ImmutableList.builder();
    while (scanner.hasNext()) {
      actionDigests.add(DigestUtil.parseDigest(scanner.nextLine()));
    }

    ManagedChannel channel = createChannel(host);

    ExecutionStub execStub = ExecutionGrpc.newStub(channel);

    executeActions(instanceName, actionDigests.build(), execStub);

    channel.shutdown();
    channel.awaitTermination(1, SECONDS);
  }
}
