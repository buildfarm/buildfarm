package build.buildfarm.instance;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class DummyInstanceBase extends InstanceBase {
  DummyInstanceBase(String name) {
    super(name);
  }

  @Override
  public void start(String publicName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(
      Iterable<Digest> digests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsBlob(
      build.buildfarm.v1test.Digest digest, Digest.Builder result, RequestMetadata requestMetadata)
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readResourceName(
      Compressor.Value compressor, build.buildfarm.v1test.Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getBlob(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest blobDigest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newBlobInput(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<List<Response>> getAllBlobsFuture(
      Iterable<Digest> digests, DigestFunction.Value digestFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTree(
      build.buildfarm.v1test.Digest rootDigest, int pageSize, String pageToken, Tree.Builder tree) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Write getBlobWrite(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      UUID uuid,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Digest> putAllBlobs(
      Iterable<BatchUpdateBlobsRequest.Request> blobs,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<build.buildfarm.v1test.Digest> fetchBlob(
      Iterable<String> uris,
      Map<String, String> headers,
      build.buildfarm.v1test.Digest expectedDigest,
      RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream newOperationStreamInput(
      String name, long offset, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> execute(
      build.buildfarm.v1test.Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher operationObserver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BackplaneStatus backplaneStatus() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean putOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String listOperations(
      String name, int pageSize, String pageToken, String filter, Consumer<Operation> onOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Operation getOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> watchExecution(UUID executionId, Watcher watcher) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServerCapabilities getCapabilities() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<WorkerProfileMessage> getWorkerProfile(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<BatchWorkerProfilesResponse> batchWorkerProfiles(Iterable<String> names) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CasIndexResults reindexCas() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deregisterWorker(String workerName) {
    throw new UnsupportedOperationException();
  }
}
