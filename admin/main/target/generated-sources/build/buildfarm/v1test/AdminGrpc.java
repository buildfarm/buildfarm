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
 * The Admin API is used to perform administrative functions on Buildfarm
 * infrastructure
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: buildfarm.proto")
public class AdminGrpc {

  private AdminGrpc() {}

  public static final String SERVICE_NAME = "build.buildfarm.v1test.Admin";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.TerminateHostRequest,
      com.google.rpc.Status> METHOD_TERMINATE_HOST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "TerminateHost"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.TerminateHostRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.rpc.Status.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.StopContainerRequest,
      com.google.rpc.Status> METHOD_STOP_CONTAINER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "StopContainer"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.StopContainerRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.rpc.Status.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.GetHostsRequest,
      build.buildfarm.v1test.GetHostsResult> METHOD_GET_HOSTS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "GetHosts"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.GetHostsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.GetHostsResult.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.GetClientStartTimeRequest,
      build.buildfarm.v1test.GetClientStartTimeResult> METHOD_GET_CLIENT_START_TIME =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "GetClientStartTime"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.GetClientStartTimeRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.GetClientStartTimeResult.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.ScaleClusterRequest,
      com.google.rpc.Status> METHOD_SCALE_CLUSTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "ScaleCluster"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.ScaleClusterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.rpc.Status.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.ReindexCasRequest,
      build.buildfarm.v1test.ReindexCasRequestResults> METHOD_REINDEX_CAS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "ReindexCas"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.ReindexCasRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.ReindexCasRequestResults.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.ShutDownWorkerGracefullyRequest,
      build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults> METHOD_SHUT_DOWN_WORKER_GRACEFULLY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "ShutDownWorkerGracefully"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.ShutDownWorkerGracefullyRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.buildfarm.v1test.DisableScaleInProtectionRequest,
      build.buildfarm.v1test.DisableScaleInProtectionRequestResults> METHOD_DISABLE_SCALE_IN_PROTECTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.buildfarm.v1test.Admin", "DisableScaleInProtection"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.DisableScaleInProtectionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.buildfarm.v1test.DisableScaleInProtectionRequestResults.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AdminStub newStub(io.grpc.Channel channel) {
    return new AdminStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AdminBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static AdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AdminFutureStub(channel);
  }

  /**
   * <pre>
   * The Admin API is used to perform administrative functions on Buildfarm
   * infrastructure
   * </pre>
   */
  public static abstract class AdminImplBase implements io.grpc.BindableService {

    /**
     */
    public void terminateHost(build.buildfarm.v1test.TerminateHostRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_TERMINATE_HOST, responseObserver);
    }

    /**
     */
    public void stopContainer(build.buildfarm.v1test.StopContainerRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_STOP_CONTAINER, responseObserver);
    }

    /**
     */
    public void getHosts(build.buildfarm.v1test.GetHostsRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetHostsResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_HOSTS, responseObserver);
    }

    /**
     */
    public void getClientStartTime(build.buildfarm.v1test.GetClientStartTimeRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetClientStartTimeResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CLIENT_START_TIME, responseObserver);
    }

    /**
     */
    public void scaleCluster(build.buildfarm.v1test.ScaleClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCALE_CLUSTER, responseObserver);
    }

    /**
     */
    public void reindexCas(build.buildfarm.v1test.ReindexCasRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.ReindexCasRequestResults> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REINDEX_CAS, responseObserver);
    }

    /**
     */
    public void shutDownWorkerGracefully(build.buildfarm.v1test.ShutDownWorkerGracefullyRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SHUT_DOWN_WORKER_GRACEFULLY, responseObserver);
    }

    /**
     */
    public void disableScaleInProtection(build.buildfarm.v1test.DisableScaleInProtectionRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.DisableScaleInProtectionRequestResults> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DISABLE_SCALE_IN_PROTECTION, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_TERMINATE_HOST,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.TerminateHostRequest,
                com.google.rpc.Status>(
                  this, METHODID_TERMINATE_HOST)))
          .addMethod(
            METHOD_STOP_CONTAINER,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.StopContainerRequest,
                com.google.rpc.Status>(
                  this, METHODID_STOP_CONTAINER)))
          .addMethod(
            METHOD_GET_HOSTS,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.GetHostsRequest,
                build.buildfarm.v1test.GetHostsResult>(
                  this, METHODID_GET_HOSTS)))
          .addMethod(
            METHOD_GET_CLIENT_START_TIME,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.GetClientStartTimeRequest,
                build.buildfarm.v1test.GetClientStartTimeResult>(
                  this, METHODID_GET_CLIENT_START_TIME)))
          .addMethod(
            METHOD_SCALE_CLUSTER,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.ScaleClusterRequest,
                com.google.rpc.Status>(
                  this, METHODID_SCALE_CLUSTER)))
          .addMethod(
            METHOD_REINDEX_CAS,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.ReindexCasRequest,
                build.buildfarm.v1test.ReindexCasRequestResults>(
                  this, METHODID_REINDEX_CAS)))
          .addMethod(
            METHOD_SHUT_DOWN_WORKER_GRACEFULLY,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.ShutDownWorkerGracefullyRequest,
                build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults>(
                  this, METHODID_SHUT_DOWN_WORKER_GRACEFULLY)))
          .addMethod(
            METHOD_DISABLE_SCALE_IN_PROTECTION,
            asyncUnaryCall(
              new MethodHandlers<
                build.buildfarm.v1test.DisableScaleInProtectionRequest,
                build.buildfarm.v1test.DisableScaleInProtectionRequestResults>(
                  this, METHODID_DISABLE_SCALE_IN_PROTECTION)))
          .build();
    }
  }

  /**
   * <pre>
   * The Admin API is used to perform administrative functions on Buildfarm
   * infrastructure
   * </pre>
   */
  public static final class AdminStub extends io.grpc.stub.AbstractStub<AdminStub> {
    private AdminStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminStub(channel, callOptions);
    }

    /**
     */
    public void terminateHost(build.buildfarm.v1test.TerminateHostRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_TERMINATE_HOST, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stopContainer(build.buildfarm.v1test.StopContainerRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_STOP_CONTAINER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getHosts(build.buildfarm.v1test.GetHostsRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetHostsResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_HOSTS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getClientStartTime(build.buildfarm.v1test.GetClientStartTimeRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetClientStartTimeResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CLIENT_START_TIME, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scaleCluster(build.buildfarm.v1test.ScaleClusterRequest request,
        io.grpc.stub.StreamObserver<com.google.rpc.Status> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SCALE_CLUSTER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reindexCas(build.buildfarm.v1test.ReindexCasRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.ReindexCasRequestResults> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REINDEX_CAS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void shutDownWorkerGracefully(build.buildfarm.v1test.ShutDownWorkerGracefullyRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SHUT_DOWN_WORKER_GRACEFULLY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void disableScaleInProtection(build.buildfarm.v1test.DisableScaleInProtectionRequest request,
        io.grpc.stub.StreamObserver<build.buildfarm.v1test.DisableScaleInProtectionRequestResults> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DISABLE_SCALE_IN_PROTECTION, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The Admin API is used to perform administrative functions on Buildfarm
   * infrastructure
   * </pre>
   */
  public static final class AdminBlockingStub extends io.grpc.stub.AbstractStub<AdminBlockingStub> {
    private AdminBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.google.rpc.Status terminateHost(build.buildfarm.v1test.TerminateHostRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_TERMINATE_HOST, getCallOptions(), request);
    }

    /**
     */
    public com.google.rpc.Status stopContainer(build.buildfarm.v1test.StopContainerRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_STOP_CONTAINER, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.GetHostsResult getHosts(build.buildfarm.v1test.GetHostsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_HOSTS, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.GetClientStartTimeResult getClientStartTime(build.buildfarm.v1test.GetClientStartTimeRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CLIENT_START_TIME, getCallOptions(), request);
    }

    /**
     */
    public com.google.rpc.Status scaleCluster(build.buildfarm.v1test.ScaleClusterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SCALE_CLUSTER, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.ReindexCasRequestResults reindexCas(build.buildfarm.v1test.ReindexCasRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REINDEX_CAS, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults shutDownWorkerGracefully(build.buildfarm.v1test.ShutDownWorkerGracefullyRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SHUT_DOWN_WORKER_GRACEFULLY, getCallOptions(), request);
    }

    /**
     */
    public build.buildfarm.v1test.DisableScaleInProtectionRequestResults disableScaleInProtection(build.buildfarm.v1test.DisableScaleInProtectionRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DISABLE_SCALE_IN_PROTECTION, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The Admin API is used to perform administrative functions on Buildfarm
   * infrastructure
   * </pre>
   */
  public static final class AdminFutureStub extends io.grpc.stub.AbstractStub<AdminFutureStub> {
    private AdminFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AdminFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AdminFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.rpc.Status> terminateHost(
        build.buildfarm.v1test.TerminateHostRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_TERMINATE_HOST, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.rpc.Status> stopContainer(
        build.buildfarm.v1test.StopContainerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_STOP_CONTAINER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.GetHostsResult> getHosts(
        build.buildfarm.v1test.GetHostsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_HOSTS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.GetClientStartTimeResult> getClientStartTime(
        build.buildfarm.v1test.GetClientStartTimeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CLIENT_START_TIME, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.rpc.Status> scaleCluster(
        build.buildfarm.v1test.ScaleClusterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SCALE_CLUSTER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.ReindexCasRequestResults> reindexCas(
        build.buildfarm.v1test.ReindexCasRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REINDEX_CAS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults> shutDownWorkerGracefully(
        build.buildfarm.v1test.ShutDownWorkerGracefullyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SHUT_DOWN_WORKER_GRACEFULLY, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<build.buildfarm.v1test.DisableScaleInProtectionRequestResults> disableScaleInProtection(
        build.buildfarm.v1test.DisableScaleInProtectionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DISABLE_SCALE_IN_PROTECTION, getCallOptions()), request);
    }
  }

  private static final int METHODID_TERMINATE_HOST = 0;
  private static final int METHODID_STOP_CONTAINER = 1;
  private static final int METHODID_GET_HOSTS = 2;
  private static final int METHODID_GET_CLIENT_START_TIME = 3;
  private static final int METHODID_SCALE_CLUSTER = 4;
  private static final int METHODID_REINDEX_CAS = 5;
  private static final int METHODID_SHUT_DOWN_WORKER_GRACEFULLY = 6;
  private static final int METHODID_DISABLE_SCALE_IN_PROTECTION = 7;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AdminImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(AdminImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_TERMINATE_HOST:
          serviceImpl.terminateHost((build.buildfarm.v1test.TerminateHostRequest) request,
              (io.grpc.stub.StreamObserver<com.google.rpc.Status>) responseObserver);
          break;
        case METHODID_STOP_CONTAINER:
          serviceImpl.stopContainer((build.buildfarm.v1test.StopContainerRequest) request,
              (io.grpc.stub.StreamObserver<com.google.rpc.Status>) responseObserver);
          break;
        case METHODID_GET_HOSTS:
          serviceImpl.getHosts((build.buildfarm.v1test.GetHostsRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetHostsResult>) responseObserver);
          break;
        case METHODID_GET_CLIENT_START_TIME:
          serviceImpl.getClientStartTime((build.buildfarm.v1test.GetClientStartTimeRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.GetClientStartTimeResult>) responseObserver);
          break;
        case METHODID_SCALE_CLUSTER:
          serviceImpl.scaleCluster((build.buildfarm.v1test.ScaleClusterRequest) request,
              (io.grpc.stub.StreamObserver<com.google.rpc.Status>) responseObserver);
          break;
        case METHODID_REINDEX_CAS:
          serviceImpl.reindexCas((build.buildfarm.v1test.ReindexCasRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.ReindexCasRequestResults>) responseObserver);
          break;
        case METHODID_SHUT_DOWN_WORKER_GRACEFULLY:
          serviceImpl.shutDownWorkerGracefully((build.buildfarm.v1test.ShutDownWorkerGracefullyRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.ShutDownWorkerGracefullyRequestResults>) responseObserver);
          break;
        case METHODID_DISABLE_SCALE_IN_PROTECTION:
          serviceImpl.disableScaleInProtection((build.buildfarm.v1test.DisableScaleInProtectionRequest) request,
              (io.grpc.stub.StreamObserver<build.buildfarm.v1test.DisableScaleInProtectionRequestResults>) responseObserver);
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
        METHOD_TERMINATE_HOST,
        METHOD_STOP_CONTAINER,
        METHOD_GET_HOSTS,
        METHOD_GET_CLIENT_START_TIME,
        METHOD_SCALE_CLUSTER,
        METHOD_REINDEX_CAS,
        METHOD_SHUT_DOWN_WORKER_GRACEFULLY,
        METHOD_DISABLE_SCALE_IN_PROTECTION);
  }

}
