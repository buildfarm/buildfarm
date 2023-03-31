package build.buildfarm.v1test;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: buildfarm.proto")
public class ShutDownWorkerGrpc {

  private ShutDownWorkerGrpc() {}

  public static final String SERVICE_NAME = "build.buildfarm.v1test.ShutDownWorker";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest,
      build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults> METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.ShutDownWorker", "PrepareWorkerForGracefulShutdown"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ShutDownWorkerStub newStub(io.grpc.Channel channel) {
    return new ShutDownWorkerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ShutDownWorkerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ShutDownWorkerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ShutDownWorkerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ShutDownWorkerFutureStub(channel);
  }

  /**
   */
  public static abstract class ShutDownWorkerImplBase implements io.grpc.BindableService {

    /**
     */
    public void prepareWorkerForGracefulShutdown(build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest,
                build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults>(
                  this, METHODID_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN)))
          .build();
    }
  }

  /**
   */
  public static final class ShutDownWorkerStub extends io.grpc.stub.AbstractStub<ShutDownWorkerStub> {
    private ShutDownWorkerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShutDownWorkerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShutDownWorkerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShutDownWorkerStub(channel, callOptions);
    }

    /**
     */
    public void prepareWorkerForGracefulShutdown(build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ShutDownWorkerBlockingStub extends io.grpc.stub.AbstractStub<ShutDownWorkerBlockingStub> {
    private ShutDownWorkerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShutDownWorkerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShutDownWorkerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShutDownWorkerBlockingStub(channel, callOptions);
    }

    /**
     */
    public build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults prepareWorkerForGracefulShutdown(build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ShutDownWorkerFutureStub extends io.grpc.stub.AbstractStub<ShutDownWorkerFutureStub> {
    private ShutDownWorkerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ShutDownWorkerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ShutDownWorkerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ShutDownWorkerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults> prepareWorkerForGracefulShutdown(
        build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN, getCallOptions()), request);
    }
  }

  private static final int METHODID_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ShutDownWorkerImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(ShutDownWorkerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN:
          serviceImpl.prepareWorkerForGracefulShutdown((build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_PREPARE_WORKER_FOR_GRACEFUL_SHUTDOWN);
  }

}
