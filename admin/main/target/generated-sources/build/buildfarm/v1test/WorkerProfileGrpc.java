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
public class WorkerProfileGrpc {

  private WorkerProfileGrpc() {}

  public static final String SERVICE_NAME = "build.buildfarm.v1test.WorkerProfile";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.WorkerProfileRequest,
      build.buildfarm.v1test.WorkerProfileMessage> METHOD_GET_WORKER_PROFILE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.WorkerProfile", "GetWorkerProfile"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.WorkerProfileRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.WorkerProfileMessage.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.WorkerListRequest,
      build.buildfarm.v1test.WorkerListMessage> METHOD_GET_WORKER_LIST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.WorkerProfile", "GetWorkerList"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.WorkerListRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.WorkerListMessage.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WorkerProfileStub newStub(io.grpc.Channel channel) {
    return new WorkerProfileStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WorkerProfileBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new WorkerProfileBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static WorkerProfileFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new WorkerProfileFutureStub(channel);
  }

  /**
   */
  public static abstract class WorkerProfileImplBase implements io.grpc.BindableService {

    /**
     */
    public void getWorkerProfile(build.buildfarm.v1test.WorkerProfileRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerProfileMessage> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_WORKER_PROFILE, responseObserver);
    }

    /**
     */
    public void getWorkerList(build.buildfarm.v1test.WorkerListRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerListMessage> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_WORKER_LIST, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_WORKER_PROFILE,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.WorkerProfileRequest,
                build.buildfarm.v1test.WorkerProfileMessage>(
                  this, METHODID_GET_WORKER_PROFILE)))
          .addMethod(
            METHOD_GET_WORKER_LIST,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.WorkerListRequest,
                build.buildfarm.v1test.WorkerListMessage>(
                  this, METHODID_GET_WORKER_LIST)))
          .build();
    }
  }

  /**
   */
  public static final class WorkerProfileStub extends io.grpc.stub.AbstractStub<WorkerProfileStub> {
    private WorkerProfileStub(io.grpc.Channel channel) {
      super(channel);
    }

    private WorkerProfileStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerProfileStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new WorkerProfileStub(channel, callOptions);
    }

    /**
     */
    public void getWorkerProfile(build.buildfarm.v1test.WorkerProfileRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerProfileMessage> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_PROFILE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getWorkerList(build.buildfarm.v1test.WorkerListRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerListMessage> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_LIST, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class WorkerProfileBlockingStub extends io.grpc.stub.AbstractStub<WorkerProfileBlockingStub> {
    private WorkerProfileBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private WorkerProfileBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerProfileBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new WorkerProfileBlockingStub(channel, callOptions);
    }

    /**
     */
    public build.buildfarm.v1test.WorkerProfileMessage getWorkerProfile(build.buildfarm.v1test.WorkerProfileRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_WORKER_PROFILE, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.WorkerListMessage getWorkerList(build.buildfarm.v1test.WorkerListRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_WORKER_LIST, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class WorkerProfileFutureStub extends io.grpc.stub.AbstractStub<WorkerProfileFutureStub> {
    private WorkerProfileFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private WorkerProfileFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerProfileFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new WorkerProfileFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.WorkerProfileMessage> getWorkerProfile(
        build.buildfarm.v1test.WorkerProfileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_PROFILE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.WorkerListMessage> getWorkerList(
        build.buildfarm.v1test.WorkerListRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_WORKER_LIST, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_WORKER_PROFILE = 0;
  private static final int METHODID_GET_WORKER_LIST = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final WorkerProfileImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(WorkerProfileImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_WORKER_PROFILE:
          serviceImpl.getWorkerProfile((build.buildfarm.v1test.WorkerProfileRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerProfileMessage>) responseObserver);
          break;
        case METHODID_GET_WORKER_LIST:
          serviceImpl.getWorkerList((build.buildfarm.v1test.WorkerListRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.WorkerListMessage>) responseObserver);
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
        METHOD_GET_WORKER_PROFILE,
        METHOD_GET_WORKER_LIST);
  }

}
