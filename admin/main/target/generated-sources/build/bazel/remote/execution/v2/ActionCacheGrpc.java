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
 * The action cache API is used to query whether a given action has already been
 * performed and, if so, retrieve its result. Unlike the
 * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
 * which addresses blobs by their own content, the action cache addresses the
 * [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
 * digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
 * which produced them.
 * The lifetime of entries in the action cache is implementation-specific, but
 * the server SHOULD assume that more recently used entries are more likely to
 * be used again.
 * As with other services in the Remote Execution API, any call may return an
 * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
 * information about when the client should retry the request; clients SHOULD
 * respect the information provided.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: remote_execution.proto")
public class ActionCacheGrpc {

  private ActionCacheGrpc() {}

  public static final String SERVICE_NAME = "build.bazel.remote.execution.v2.ActionCache";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.bazel.remote.execution.v2.GetActionResultRequest,
      build.bazel.remote.execution.v2.ActionResult> METHOD_GET_ACTION_RESULT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.bazel.remote.execution.v2.ActionCache", "GetActionResult"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.GetActionResultRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.ActionResult.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.bazel.remote.execution.v2.UpdateActionResultRequest,
      build.bazel.remote.execution.v2.ActionResult> METHOD_UPDATE_ACTION_RESULT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "build.bazel.remote.execution.v2.ActionCache", "UpdateActionResult"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.UpdateActionResultRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.ActionResult.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ActionCacheStub newStub(io.grpc.Channel channel) {
    return new ActionCacheStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ActionCacheBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ActionCacheBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ActionCacheFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ActionCacheFutureStub(channel);
  }

  /**
   * <pre>
   * The action cache API is used to query whether a given action has already been
   * performed and, if so, retrieve its result. Unlike the
   * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
   * which addresses blobs by their own content, the action cache addresses the
   * [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
   * digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
   * which produced them.
   * The lifetime of entries in the action cache is implementation-specific, but
   * the server SHOULD assume that more recently used entries are more likely to
   * be used again.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static abstract class ActionCacheImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Retrieve a cached execution result.
     * Implementations SHOULD ensure that any blobs referenced from the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
     * are available at the time of returning the
     * [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
     * for some period of time afterwards. The TTLs of the referenced blobs SHOULD
     * be increased if necessary and applicable.
     * Errors:
     * * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
     * </pre>
     */
    public void getActionResult(build.bazel.remote.execution.v2.GetActionResultRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_ACTION_RESULT, responseObserver);
    }

    /**
     * <pre>
     * Upload a new execution result.
     * In order to allow the server to perform access control based on the type of
     * action, and to assist with client debugging, the client MUST first upload
     * the [Action][build.bazel.remote.execution.v2.Execution] that produced the
     * result, along with its
     * [Command][build.bazel.remote.execution.v2.Command], into the
     * `ContentAddressableStorage`.
     * Errors:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in updating the
     *   action result, such as a missing command or action.
     * * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
     *   entry to the cache.
     * </pre>
     */
    public void updateActionResult(build.bazel.remote.execution.v2.UpdateActionResultRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UPDATE_ACTION_RESULT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_ACTION_RESULT,
            asyncUnaryCall(
              new MethodHandlers<
                build.bazel.remote.execution.v2.GetActionResultRequest,
                build.bazel.remote.execution.v2.ActionResult>(
                  this, METHODID_GET_ACTION_RESULT)))
          .addMethod(
            METHOD_UPDATE_ACTION_RESULT,
            asyncUnaryCall(
              new MethodHandlers<
                build.bazel.remote.execution.v2.UpdateActionResultRequest,
                build.bazel.remote.execution.v2.ActionResult>(
                  this, METHODID_UPDATE_ACTION_RESULT)))
          .build();
    }
  }

  /**
   * <pre>
   * The action cache API is used to query whether a given action has already been
   * performed and, if so, retrieve its result. Unlike the
   * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
   * which addresses blobs by their own content, the action cache addresses the
   * [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
   * digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
   * which produced them.
   * The lifetime of entries in the action cache is implementation-specific, but
   * the server SHOULD assume that more recently used entries are more likely to
   * be used again.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ActionCacheStub extends io.grpc.stub.AbstractStub<ActionCacheStub> {
    private ActionCacheStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ActionCacheStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ActionCacheStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ActionCacheStub(channel, callOptions);
    }

    /**
     * <pre>
     * Retrieve a cached execution result.
     * Implementations SHOULD ensure that any blobs referenced from the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
     * are available at the time of returning the
     * [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
     * for some period of time afterwards. The TTLs of the referenced blobs SHOULD
     * be increased if necessary and applicable.
     * Errors:
     * * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
     * </pre>
     */
    public void getActionResult(build.bazel.remote.execution.v2.GetActionResultRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_ACTION_RESULT, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Upload a new execution result.
     * In order to allow the server to perform access control based on the type of
     * action, and to assist with client debugging, the client MUST first upload
     * the [Action][build.bazel.remote.execution.v2.Execution] that produced the
     * result, along with its
     * [Command][build.bazel.remote.execution.v2.Command], into the
     * `ContentAddressableStorage`.
     * Errors:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in updating the
     *   action result, such as a missing command or action.
     * * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
     *   entry to the cache.
     * </pre>
     */
    public void updateActionResult(build.bazel.remote.execution.v2.UpdateActionResultRequest request,
        io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_ACTION_RESULT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The action cache API is used to query whether a given action has already been
   * performed and, if so, retrieve its result. Unlike the
   * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
   * which addresses blobs by their own content, the action cache addresses the
   * [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
   * digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
   * which produced them.
   * The lifetime of entries in the action cache is implementation-specific, but
   * the server SHOULD assume that more recently used entries are more likely to
   * be used again.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ActionCacheBlockingStub extends io.grpc.stub.AbstractStub<ActionCacheBlockingStub> {
    private ActionCacheBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ActionCacheBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ActionCacheBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ActionCacheBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Retrieve a cached execution result.
     * Implementations SHOULD ensure that any blobs referenced from the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
     * are available at the time of returning the
     * [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
     * for some period of time afterwards. The TTLs of the referenced blobs SHOULD
     * be increased if necessary and applicable.
     * Errors:
     * * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
     * </pre>
     */
    public build.bazel.remote.execution.v2.ActionResult getActionResult(build.bazel.remote.execution.v2.GetActionResultRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_ACTION_RESULT, getCallOptions(), request);
    }

    /**
     * <pre>
     * Upload a new execution result.
     * In order to allow the server to perform access control based on the type of
     * action, and to assist with client debugging, the client MUST first upload
     * the [Action][build.bazel.remote.execution.v2.Execution] that produced the
     * result, along with its
     * [Command][build.bazel.remote.execution.v2.Command], into the
     * `ContentAddressableStorage`.
     * Errors:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in updating the
     *   action result, such as a missing command or action.
     * * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
     *   entry to the cache.
     * </pre>
     */
    public build.bazel.remote.execution.v2.ActionResult updateActionResult(build.bazel.remote.execution.v2.UpdateActionResultRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_ACTION_RESULT, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The action cache API is used to query whether a given action has already been
   * performed and, if so, retrieve its result. Unlike the
   * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
   * which addresses blobs by their own content, the action cache addresses the
   * [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
   * digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
   * which produced them.
   * The lifetime of entries in the action cache is implementation-specific, but
   * the server SHOULD assume that more recently used entries are more likely to
   * be used again.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ActionCacheFutureStub extends io.grpc.stub.AbstractStub<ActionCacheFutureStub> {
    private ActionCacheFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ActionCacheFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ActionCacheFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ActionCacheFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Retrieve a cached execution result.
     * Implementations SHOULD ensure that any blobs referenced from the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
     * are available at the time of returning the
     * [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
     * for some period of time afterwards. The TTLs of the referenced blobs SHOULD
     * be increased if necessary and applicable.
     * Errors:
     * * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<build.bazel.remote.execution.v2.ActionResult> getActionResult(
        build.bazel.remote.execution.v2.GetActionResultRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_ACTION_RESULT, getCallOptions()), request);
    }

    /**
     * <pre>
     * Upload a new execution result.
     * In order to allow the server to perform access control based on the type of
     * action, and to assist with client debugging, the client MUST first upload
     * the [Action][build.bazel.remote.execution.v2.Execution] that produced the
     * result, along with its
     * [Command][build.bazel.remote.execution.v2.Command], into the
     * `ContentAddressableStorage`.
     * Errors:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in updating the
     *   action result, such as a missing command or action.
     * * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
     *   entry to the cache.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<build.bazel.remote.execution.v2.ActionResult> updateActionResult(
        build.bazel.remote.execution.v2.UpdateActionResultRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_ACTION_RESULT, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ACTION_RESULT = 0;
  private static final int METHODID_UPDATE_ACTION_RESULT = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ActionCacheImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(ActionCacheImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_ACTION_RESULT:
          serviceImpl.getActionResult((build.bazel.remote.execution.v2.GetActionResultRequest) request,
              (io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult>) responseObserver);
          break;
        case METHODID_UPDATE_ACTION_RESULT:
          serviceImpl.updateActionResult((build.bazel.remote.execution.v2.UpdateActionResultRequest) request,
              (io.grpc.stub.StreamObserver<build.bazel.remote.execution.v2.ActionResult>) responseObserver);
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
        METHOD_GET_ACTION_RESULT,
        METHOD_UPDATE_ACTION_RESULT);
  }

}
