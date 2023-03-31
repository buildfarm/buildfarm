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
 * <pre>
 * The OperationQueue API is used internally to communicate with Workers
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: buildfarm.proto")
public class OperationQueueGrpc {

  private OperationQueueGrpc() {}

  public static final String SERVICE_NAME = "build.buildfarm.v1test.OperationQueue";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.TakeOperationRequest,
      build.buildfarm.v1test.QueueEntry> METHOD_TAKE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.OperationQueue", "Take"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.TakeOperationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.QueueEntry.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.longrunning.Operation,
      com.google.rpc.Status> METHOD_PUT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.OperationQueue", "Put"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.rpc.Status.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.PollOperationRequest,
      com.google.rpc.Status> METHOD_POLL =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.OperationQueue", "Poll"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.PollOperationRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.rpc.Status.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.OperationsStatusRequest,
      build.buildfarm.v1test.OperationsStatus> METHOD_STATUS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.OperationQueue", "Status"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.OperationsStatusRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.OperationsStatus.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static OperationQueueStub newStub(io.grpc.Channel channel) {
    return new OperationQueueStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static OperationQueueBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new OperationQueueBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static OperationQueueFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new OperationQueueFutureStub(channel);
  }

  /**
   * <pre>
   * The OperationQueue API is used internally to communicate with Workers
   * </pre>
   */
  public static abstract class OperationQueueImplBase implements io.grpc.BindableService {

    /**
     */
    public void take(build.buildfarm.v1test.TakeOperationRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.QueueEntry> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_TAKE, responseObserver);
    }

    /**
     */
    public void put(com.google.longrunning.Operation request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PUT, responseObserver);
    }

    /**
     */
    public void poll(build.buildfarm.v1test.PollOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_POLL, responseObserver);
    }

    /**
     */
    public void status(build.buildfarm.v1test.OperationsStatusRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.OperationsStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_STATUS, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_TAKE,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.TakeOperationRequest,
                build.buildfarm.v1test.QueueEntry>(
                  this, METHODID_TAKE)))
          .addMethod(
            METHOD_PUT,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.longrunning.Operation,
                com.google.rpc.Status>(
                  this, METHODID_PUT)))
          .addMethod(
            METHOD_POLL,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.PollOperationRequest,
                com.google.rpc.Status>(
                  this, METHODID_POLL)))
          .addMethod(
            METHOD_STATUS,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.OperationsStatusRequest,
                build.buildfarm.v1test.OperationsStatus>(
                  this, METHODID_STATUS)))
          .build();
    }
  }

  /**
   * <pre>
   * The OperationQueue API is used internally to communicate with Workers
   * </pre>
   */
  public static final class OperationQueueStub extends io.grpc.stub.AbstractStub<OperationQueueStub> {
    private OperationQueueStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationQueueStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationQueueStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationQueueStub(channel, callOptions);
    }

    /**
     */
    public void take(build.buildfarm.v1test.TakeOperationRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.QueueEntry> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_TAKE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void put(com.google.longrunning.Operation request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void poll(build.buildfarm.v1test.PollOperationRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_POLL, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void status(build.buildfarm.v1test.OperationsStatusRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.OperationsStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_STATUS, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The OperationQueue API is used internally to communicate with Workers
   * </pre>
   */
  public static final class OperationQueueBlockingStub extends io.grpc.stub.AbstractStub<OperationQueueBlockingStub> {
    private OperationQueueBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationQueueBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationQueueBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationQueueBlockingStub(channel, callOptions);
    }

    /**
     */
    public build.buildfarm.v1test.QueueEntry take(build.buildfarm.v1test.TakeOperationRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_TAKE, getCallOptions(), request);
    }

    /**
     */
    public com.google.rpc.Status put(com.google.longrunning.Operation request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT, getCallOptions(), request);
    }

    /**
     */
    public com.google.rpc.Status poll(build.buildfarm.v1test.PollOperationRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_POLL, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.OperationsStatus status(build.buildfarm.v1test.OperationsStatusRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_STATUS, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The OperationQueue API is used internally to communicate with Workers
   * </pre>
   */
  public static final class OperationQueueFutureStub extends io.grpc.stub.AbstractStub<OperationQueueFutureStub> {
    private OperationQueueFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private OperationQueueFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OperationQueueFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new OperationQueueFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.QueueEntry> take(
        build.buildfarm.v1test.TakeOperationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_TAKE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.rpc.Status> put(
        com.google.longrunning.Operation request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.rpc.Status> poll(
        build.buildfarm.v1test.PollOperationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_POLL, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.OperationsStatus> status(
        build.buildfarm.v1test.OperationsStatusRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_STATUS, getCallOptions()), request);
    }
  }

  private static final int METHODID_TAKE = 0;
  private static final int METHODID_PUT = 1;
  private static final int METHODID_POLL = 2;
  private static final int METHODID_STATUS = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final OperationQueueImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(OperationQueueImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_TAKE:
          serviceImpl.take((build.buildfarm.v1test.TakeOperationRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.QueueEntry>) responseObserver);
          break;
        case METHODID_PUT:
          serviceImpl.put((com.google.longrunning.Operation) request,
              (io.grpc.stub.StreamObserver<com.google.rpc.Status>) responseObserver);
          break;
        case METHODID_POLL:
          serviceImpl.poll((build.buildfarm.v1test.PollOperationRequest) request,
              (io.grpc.stub.StreamObserver<com.google.rpc.Status>) responseObserver);
          break;
        case METHODID_STATUS:
          serviceImpl.status((build.buildfarm.v1test.OperationsStatusRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.OperationsStatus>) responseObserver);
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
        METHOD_TAKE,
        METHOD_PUT,
        METHOD_POLL,
        METHOD_STATUS);
  }

}
