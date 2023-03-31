package build.bazel.remote.execution.v2;

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
 * The Capabilities service may be used by remote execution clients to query
 * various server properties, in order to self-configure or return meaningful
 * error messages.
 * The query may include a particular `instance_name`, in which case the values
 * returned will pertain to that instance.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: remote_execution.proto")
public class CapabilitiesGrpc {

  private CapabilitiesGrpc() {}

  public static final String SERVICE_NAME = "build.bazel.remote.execution.v2.Capabilities";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.bazel.remote.execution.v2.GetCapabilitiesRequest,
      build.bazel.remote.execution.v2.ServerCapabilities> METHOD_GET_CAPABILITIES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.bazel.remote.execution.v2.Capabilities", "GetCapabilities"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.GetCapabilitiesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.ServerCapabilities.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CapabilitiesStub newStub(io.grpc.Channel channel) {
    return new CapabilitiesStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CapabilitiesBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CapabilitiesBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static CapabilitiesFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CapabilitiesFutureStub(channel);
  }

  /**
   * <pre>
   * The Capabilities service may be used by remote execution clients to query
   * various server properties, in order to self-configure or return meaningful
   * error messages.
   * The query may include a particular `instance_name`, in which case the values
   * returned will pertain to that instance.
   * </pre>
   */
  public static abstract class CapabilitiesImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * GetCapabilities returns the server capabilities configuration of the
     * remote endpoint.
     * Only the capabilities of the services supported by the endpoint will
     * be returned:
     * * Execution + CAS + Action Cache endpoints should return both
     *   CacheCapabilities and ExecutionCapabilities.
     * * Execution only endpoints should return ExecutionCapabilities.
     * * CAS + Action Cache only endpoints should return CacheCapabilities.
     * </pre>
     */
    public void getCapabilities(build.bazel.remote.execution.v2.GetCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ServerCapabilities> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CAPABILITIES, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_CAPABILITIES,
            asyncUnaryCall(
              new MethodHandlers<
                build.bazel.remote.execution.v2.GetCapabilitiesRequest,
                build.bazel.remote.execution.v2.ServerCapabilities>(
                  this, METHODID_GET_CAPABILITIES)))
          .build();
    }
  }

  /**
   * <pre>
   * The Capabilities service may be used by remote execution clients to query
   * various server properties, in order to self-configure or return meaningful
   * error messages.
   * The query may include a particular `instance_name`, in which case the values
   * returned will pertain to that instance.
   * </pre>
   */
  public static final class CapabilitiesStub extends io.grpc.stub.AbstractStub<CapabilitiesStub> {
    private CapabilitiesStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CapabilitiesStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CapabilitiesStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CapabilitiesStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetCapabilities returns the server capabilities configuration of the
     * remote endpoint.
     * Only the capabilities of the services supported by the endpoint will
     * be returned:
     * * Execution + CAS + Action Cache endpoints should return both
     *   CacheCapabilities and ExecutionCapabilities.
     * * Execution only endpoints should return ExecutionCapabilities.
     * * CAS + Action Cache only endpoints should return CacheCapabilities.
     * </pre>
     */
    public void getCapabilities(build.bazel.remote.execution.v2.GetCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ServerCapabilities> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CAPABILITIES, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The Capabilities service may be used by remote execution clients to query
   * various server properties, in order to self-configure or return meaningful
   * error messages.
   * The query may include a particular `instance_name`, in which case the values
   * returned will pertain to that instance.
   * </pre>
   */
  public static final class CapabilitiesBlockingStub extends io.grpc.stub.AbstractStub<CapabilitiesBlockingStub> {
    private CapabilitiesBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CapabilitiesBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CapabilitiesBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CapabilitiesBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetCapabilities returns the server capabilities configuration of the
     * remote endpoint.
     * Only the capabilities of the services supported by the endpoint will
     * be returned:
     * * Execution + CAS + Action Cache endpoints should return both
     *   CacheCapabilities and ExecutionCapabilities.
     * * Execution only endpoints should return ExecutionCapabilities.
     * * CAS + Action Cache only endpoints should return CacheCapabilities.
     * </pre>
     */
    public build.bazel.remote.execution.v2.ServerCapabilities getCapabilities(build.bazel.remote.execution.v2.GetCapabilitiesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CAPABILITIES, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The Capabilities service may be used by remote execution clients to query
   * various server properties, in order to self-configure or return meaningful
   * error messages.
   * The query may include a particular `instance_name`, in which case the values
   * returned will pertain to that instance.
   * </pre>
   */
  public static final class CapabilitiesFutureStub extends io.grpc.stub.AbstractStub<CapabilitiesFutureStub> {
    private CapabilitiesFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CapabilitiesFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CapabilitiesFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CapabilitiesFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * GetCapabilities returns the server capabilities configuration of the
     * remote endpoint.
     * Only the capabilities of the services supported by the endpoint will
     * be returned:
     * * Execution + CAS + Action Cache endpoints should return both
     *   CacheCapabilities and ExecutionCapabilities.
     * * Execution only endpoints should return ExecutionCapabilities.
     * * CAS + Action Cache only endpoints should return CacheCapabilities.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<build.bazel.remote.execution.v2.ServerCapabilities> getCapabilities(
        build.bazel.remote.execution.v2.GetCapabilitiesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CAPABILITIES, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_CAPABILITIES = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CapabilitiesImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(CapabilitiesImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_CAPABILITIES:
          serviceImpl.getCapabilities((build.bazel.remote.execution.v2.GetCapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ServerCapabilities>) responseObserver);
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
        METHOD_GET_CAPABILITIES);
  }

}
