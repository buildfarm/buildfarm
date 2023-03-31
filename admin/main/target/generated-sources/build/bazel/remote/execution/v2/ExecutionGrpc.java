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
 * The Remote Execution API is used to execute an
 * [Action][build.bazel.remote.execution.v2.Action] on the remote
 * workers.
 * As with other services in the Remote Execution API, any call may return an
 * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
 * information about when the client should retry the request; clients SHOULD
 * respect the information provided.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.1)",
    comments = "Source: remote_execution.proto")
public class ExecutionGrpc {

  private ExecutionGrpc() {}

  public static final String SERVICE_NAME = "build.bazel.remote.execution.v2.Execution";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.bazel.remote.execution.v2.ExecuteRequest,
      com.google.longrunning.Operation> METHOD_EXECUTE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "build.bazel.remote.execution.v2.Execution", "Execute"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.ExecuteRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<build.bazel.remote.execution.v2.WaitExecutionRequest,
      com.google.longrunning.Operation> METHOD_WAIT_EXECUTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "build.bazel.remote.execution.v2.Execution", "WaitExecution"),
          io.grpc.protobuf.ProtoUtils.marshaller(build.bazel.remote.execution.v2.WaitExecutionRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.longrunning.Operation.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ExecutionStub newStub(io.grpc.Channel channel) {
    return new ExecutionStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ExecutionBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ExecutionBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ExecutionFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ExecutionFutureStub(channel);
  }

  /**
   * <pre>
   * The Remote Execution API is used to execute an
   * [Action][build.bazel.remote.execution.v2.Action] on the remote
   * workers.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static abstract class ExecutionImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Execute an action remotely.
     * In order to execute an action, the client must first upload all of the
     * inputs, the
     * [Command][build.bazel.remote.execution.v2.Command] to run, and the
     * [Action][build.bazel.remote.execution.v2.Action] into the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
     * It then calls `Execute` with an `action_digest` referring to them. The
     * server will run the action and eventually return the result.
     * The input `Action`'s fields MUST meet the various canonicalization
     * requirements specified in the documentation for their types so that it has
     * the same digest as other logically equivalent `Action`s. The server MAY
     * enforce the requirements and return errors if a non-canonical input is
     * received. It MAY also proceed without verifying some or all of the
     * requirements, such as for performance reasons. If the server does not
     * verify the requirement, then it will treat the `Action` as distinct from
     * another logically equivalent action if they hash differently.
     * Returns a stream of
     * [google.longrunning.Operation][google.longrunning.Operation] messages
     * describing the resulting execution, with eventual `response`
     * [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
     * `metadata` on the operation is of type
     * [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
     * If the client remains connected after the first response is returned after
     * the server, then updates are streamed as if the client had called
     * [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
     * until the execution completes or the request reaches an error. The
     * operation can also be queried using [Operations
     * API][google.longrunning.Operations.GetOperation].
     * The server NEED NOT implement other methods or functionality of the
     * Operations API.
     * Errors discovered during creation of the `Operation` will be reported
     * as gRPC Status errors, while errors that occurred while running the
     * action will be reported in the `status` field of the `ExecuteResponse`. The
     * server MUST NOT set the `error` field of the `Operation` proto.
     * The possible errors include:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
     *   action requested, such as a missing input or command or no worker being
     *   available. The client may be able to fix the errors and retry.
     * * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
     *   the action.
     * * `UNAVAILABLE`: Due to a transient condition, such as all workers being
     *   occupied (and the server does not support a queue), the action could not
     *   be started. The client should retry.
     * * `INTERNAL`: An internal error occurred in the execution engine or the
     *   worker.
     * * `DEADLINE_EXCEEDED`: The execution timed out.
     * * `CANCELLED`: The operation was cancelled by the client. This status is
     *   only possible if the server implements the Operations API CancelOperation
     *   method, and it was called for the current execution.
     * In the case of a missing input or command, the server SHOULD additionally
     * send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
     * where, for each requested blob not present in the CAS, there is a
     * `Violation` with a `type` of `MISSING` and a `subject` of
     * `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
     * </pre>
     */
    public void execute(build.bazel.remote.execution.v2.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXECUTE, responseObserver);
    }

    /**
     * <pre>
     * Wait for an execution operation to complete. When the client initially
     * makes the request, the server immediately responds with the current status
     * of the execution. The server will leave the request stream open until the
     * operation completes, and then respond with the completed operation. The
     * server MAY choose to stream additional updates as execution progresses,
     * such as to provide an update as to the state of the execution.
     * </pre>
     */
    public void waitExecution(build.bazel.remote.execution.v2.WaitExecutionRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_WAIT_EXECUTION, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_EXECUTE,
            asyncServerStreamingCall(
              new MethodHandlers<
                build.bazel.remote.execution.v2.ExecuteRequest,
                com.google.longrunning.Operation>(
                  this, METHODID_EXECUTE)))
          .addMethod(
            METHOD_WAIT_EXECUTION,
            asyncServerStreamingCall(
              new MethodHandlers<
                build.bazel.remote.execution.v2.WaitExecutionRequest,
                com.google.longrunning.Operation>(
                  this, METHODID_WAIT_EXECUTION)))
          .build();
    }
  }

  /**
   * <pre>
   * The Remote Execution API is used to execute an
   * [Action][build.bazel.remote.execution.v2.Action] on the remote
   * workers.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ExecutionStub extends io.grpc.stub.AbstractStub<ExecutionStub> {
    private ExecutionStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ExecutionStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExecutionStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ExecutionStub(channel, callOptions);
    }

    /**
     * <pre>
     * Execute an action remotely.
     * In order to execute an action, the client must first upload all of the
     * inputs, the
     * [Command][build.bazel.remote.execution.v2.Command] to run, and the
     * [Action][build.bazel.remote.execution.v2.Action] into the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
     * It then calls `Execute` with an `action_digest` referring to them. The
     * server will run the action and eventually return the result.
     * The input `Action`'s fields MUST meet the various canonicalization
     * requirements specified in the documentation for their types so that it has
     * the same digest as other logically equivalent `Action`s. The server MAY
     * enforce the requirements and return errors if a non-canonical input is
     * received. It MAY also proceed without verifying some or all of the
     * requirements, such as for performance reasons. If the server does not
     * verify the requirement, then it will treat the `Action` as distinct from
     * another logically equivalent action if they hash differently.
     * Returns a stream of
     * [google.longrunning.Operation][google.longrunning.Operation] messages
     * describing the resulting execution, with eventual `response`
     * [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
     * `metadata` on the operation is of type
     * [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
     * If the client remains connected after the first response is returned after
     * the server, then updates are streamed as if the client had called
     * [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
     * until the execution completes or the request reaches an error. The
     * operation can also be queried using [Operations
     * API][google.longrunning.Operations.GetOperation].
     * The server NEED NOT implement other methods or functionality of the
     * Operations API.
     * Errors discovered during creation of the `Operation` will be reported
     * as gRPC Status errors, while errors that occurred while running the
     * action will be reported in the `status` field of the `ExecuteResponse`. The
     * server MUST NOT set the `error` field of the `Operation` proto.
     * The possible errors include:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
     *   action requested, such as a missing input or command or no worker being
     *   available. The client may be able to fix the errors and retry.
     * * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
     *   the action.
     * * `UNAVAILABLE`: Due to a transient condition, such as all workers being
     *   occupied (and the server does not support a queue), the action could not
     *   be started. The client should retry.
     * * `INTERNAL`: An internal error occurred in the execution engine or the
     *   worker.
     * * `DEADLINE_EXCEEDED`: The execution timed out.
     * * `CANCELLED`: The operation was cancelled by the client. This status is
     *   only possible if the server implements the Operations API CancelOperation
     *   method, and it was called for the current execution.
     * In the case of a missing input or command, the server SHOULD additionally
     * send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
     * where, for each requested blob not present in the CAS, there is a
     * `Violation` with a `type` of `MISSING` and a `subject` of
     * `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
     * </pre>
     */
    public void execute(build.bazel.remote.execution.v2.ExecuteRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_EXECUTE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Wait for an execution operation to complete. When the client initially
     * makes the request, the server immediately responds with the current status
     * of the execution. The server will leave the request stream open until the
     * operation completes, and then respond with the completed operation. The
     * server MAY choose to stream additional updates as execution progresses,
     * such as to provide an update as to the state of the execution.
     * </pre>
     */
    public void waitExecution(build.bazel.remote.execution.v2.WaitExecutionRequest request,
        io.grpc.stub.StreamObserver<com.google.longrunning.Operation> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_WAIT_EXECUTION, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The Remote Execution API is used to execute an
   * [Action][build.bazel.remote.execution.v2.Action] on the remote
   * workers.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ExecutionBlockingStub extends io.grpc.stub.AbstractStub<ExecutionBlockingStub> {
    private ExecutionBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ExecutionBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExecutionBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ExecutionBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Execute an action remotely.
     * In order to execute an action, the client must first upload all of the
     * inputs, the
     * [Command][build.bazel.remote.execution.v2.Command] to run, and the
     * [Action][build.bazel.remote.execution.v2.Action] into the
     * [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
     * It then calls `Execute` with an `action_digest` referring to them. The
     * server will run the action and eventually return the result.
     * The input `Action`'s fields MUST meet the various canonicalization
     * requirements specified in the documentation for their types so that it has
     * the same digest as other logically equivalent `Action`s. The server MAY
     * enforce the requirements and return errors if a non-canonical input is
     * received. It MAY also proceed without verifying some or all of the
     * requirements, such as for performance reasons. If the server does not
     * verify the requirement, then it will treat the `Action` as distinct from
     * another logically equivalent action if they hash differently.
     * Returns a stream of
     * [google.longrunning.Operation][google.longrunning.Operation] messages
     * describing the resulting execution, with eventual `response`
     * [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
     * `metadata` on the operation is of type
     * [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
     * If the client remains connected after the first response is returned after
     * the server, then updates are streamed as if the client had called
     * [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
     * until the execution completes or the request reaches an error. The
     * operation can also be queried using [Operations
     * API][google.longrunning.Operations.GetOperation].
     * The server NEED NOT implement other methods or functionality of the
     * Operations API.
     * Errors discovered during creation of the `Operation` will be reported
     * as gRPC Status errors, while errors that occurred while running the
     * action will be reported in the `status` field of the `ExecuteResponse`. The
     * server MUST NOT set the `error` field of the `Operation` proto.
     * The possible errors include:
     * * `INVALID_ARGUMENT`: One or more arguments are invalid.
     * * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
     *   action requested, such as a missing input or command or no worker being
     *   available. The client may be able to fix the errors and retry.
     * * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
     *   the action.
     * * `UNAVAILABLE`: Due to a transient condition, such as all workers being
     *   occupied (and the server does not support a queue), the action could not
     *   be started. The client should retry.
     * * `INTERNAL`: An internal error occurred in the execution engine or the
     *   worker.
     * * `DEADLINE_EXCEEDED`: The execution timed out.
     * * `CANCELLED`: The operation was cancelled by the client. This status is
     *   only possible if the server implements the Operations API CancelOperation
     *   method, and it was called for the current execution.
     * In the case of a missing input or command, the server SHOULD additionally
     * send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
     * where, for each requested blob not present in the CAS, there is a
     * `Violation` with a `type` of `MISSING` and a `subject` of
     * `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
     * </pre>
     */
    public java.util.Iterator<com.google.longrunning.Operation> execute(
        build.bazel.remote.execution.v2.ExecuteRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_EXECUTE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Wait for an execution operation to complete. When the client initially
     * makes the request, the server immediately responds with the current status
     * of the execution. The server will leave the request stream open until the
     * operation completes, and then respond with the completed operation. The
     * server MAY choose to stream additional updates as execution progresses,
     * such as to provide an update as to the state of the execution.
     * </pre>
     */
    public java.util.Iterator<com.google.longrunning.Operation> waitExecution(
        build.bazel.remote.execution.v2.WaitExecutionRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_WAIT_EXECUTION, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The Remote Execution API is used to execute an
   * [Action][build.bazel.remote.execution.v2.Action] on the remote
   * workers.
   * As with other services in the Remote Execution API, any call may return an
   * error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
   * information about when the client should retry the request; clients SHOULD
   * respect the information provided.
   * </pre>
   */
  public static final class ExecutionFutureStub extends io.grpc.stub.AbstractStub<ExecutionFutureStub> {
    private ExecutionFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ExecutionFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExecutionFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ExecutionFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_EXECUTE = 0;
  private static final int METHODID_WAIT_EXECUTION = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ExecutionImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(ExecutionImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EXECUTE:
          serviceImpl.execute((build.bazel.remote.execution.v2.ExecuteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
          break;
        case METHODID_WAIT_EXECUTION:
          serviceImpl.waitExecution((build.bazel.remote.execution.v2.WaitExecutionRequest) request,
              (io.grpc.stub.StreamObserver<com.google.longrunning.Operation>) responseObserver);
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
        METHOD_EXECUTE,
        METHOD_WAIT_EXECUTION);
  }

}
