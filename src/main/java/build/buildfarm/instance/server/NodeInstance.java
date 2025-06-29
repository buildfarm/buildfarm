// Copyright 2017 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance.server;

import static build.buildfarm.common.Actions.asExecutionStatus;
import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Errors.MISSING_INPUT;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.common.Trees.enumerateTreeFileDigests;
import static build.buildfarm.instance.Utils.putBlob;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheUpdateCapabilities;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.CacheCapabilities;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionCapabilities;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.PriorityCapabilities;
import build.bazel.remote.execution.v2.PriorityCapabilities.PriorityRange;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.SymlinkAbsolutePathStrategy;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.actioncache.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.ProxyDirectoriesIndex;
import build.buildfarm.common.Size;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Write;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.net.URL;
import build.buildfarm.common.resources.BlobInformation;
import build.buildfarm.common.resources.DownloadBlobRequest;
import build.buildfarm.common.resources.ResourceParser;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.InstanceBase;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import com.owteam.engUtils.netrc.Netrc;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.java.Log;
import org.apache.http.auth.Credentials;

@Log
public abstract class NodeInstance extends InstanceBase {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  protected final ContentAddressableStorage contentAddressableStorage;
  protected final ActionCache actionCache;
  protected final OperationsMap outstandingOperations;
  protected final OperationsMap completedOperations;
  protected final Map<Digest, ByteString> activeBlobWrites;
  protected final boolean ensureOutputsPresent;

  public static final String ACTION_INPUT_ROOT_DIRECTORY_PATH = "";

  public static final String DUPLICATE_DIRENT =
      "One of the input `Directory` has multiple entries with the same file name. This will also"
          + " occur if the worker filesystem considers two names to be the same, such as two names"
          + " that vary only by case on a case-insensitive filesystem, or two names with the same"
          + " normalized form on a filesystem that performs Unicode normalization on filenames.";

  public static final String DIRECTORY_NOT_SORTED =
      "The files in an input `Directory` are not correctly sorted by `name`.";

  public static final String DIRECTORY_CYCLE_DETECTED =
      "The input file tree contains a cycle (a `Directory` which, directly or indirectly,"
          + " contains itself).";

  public static final String DUPLICATE_ENVIRONMENT_VARIABLE =
      "The `Command`'s `environment_variables` contain a duplicate entry. On systems where"
          + " environment variables may consider two different names to be the same, such as if"
          + " environment variables are case-insensitive, this may also occur if two equivalent"
          + " environment variables appear.";

  public static final String ENVIRONMENT_VARIABLES_NOT_SORTED =
      "The `Command`'s `environment_variables` are not correctly sorted by `name`.";

  public static final String SYMLINK_TARGET_ABSOLUTE = "A symlink target is absolute.";

  public static final String MISSING_ACTION = "The action was not found in the CAS.";

  public static final String MISSING_COMMAND = "The command was not found in the CAS.";

  public static final String INVALID_DIGEST = "A `Digest` in the input tree is invalid.";

  public static final String INVALID_ACTION = "The `Action` was invalid.";

  public static final String INVALID_COMMAND = "The `Command` of the `Action` was invalid.";

  public static final String INVALID_PLATFORM = "The `Platform` of the `Command` was invalid.";

  private static final String INVALID_FILE_NAME =
      "One of the input `PathNode`s has an invalid name, such as a name containing a `/` character"
          + " or another character which cannot be used in a file's name on the filesystem of the"
          + " worker.";

  private static final String OUTPUT_FILE_DIRECTORY_COLLISION =
      "An output file has the same path as an output directory";

  private static final String OUTPUT_FILE_IS_INPUT_DIRECTORY =
      "An output file has the same path as an input directory";

  private static final String OUTPUT_DIRECTORY_IS_INPUT_FILE =
      "An output directory has the same path as an input file";

  public static final String OUTPUT_FILE_IS_OUTPUT_ANCESTOR =
      "An output file is an ancestor to another output";

  public static final String OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR =
      "An output directory is an ancestor to another output";

  public static final String BLOCK_LIST_ERROR =
      "This request is in block list and is forbidden.  "
          + "To resolve this error, you can tag the rule with 'no-remote'.  "
          + "You can also adjust the action behavior to attempt a different action hash.";

  public static final String NO_REQUEUE_BLOCKED_ERROR =
      "Operation %s not requeued. " + BLOCK_LIST_ERROR;

  public static final String NO_REQUEUE_TOO_MANY_ERROR =
      "Operation %s not requeued.  Operation has been requeued too many times ( %d > %d).";

  public static final String NO_REQUEUE_MISSING_MESSAGE =
      "Operation %s not requeued.  Operation no longer exists.";

  public static final String NO_REQUEUE_COMPLETE_MESSAGE =
      "Operation %s not requeued.  Operation has already completed.";

  public NodeInstance(
      String name,
      ContentAddressableStorage contentAddressableStorage,
      ActionCache actionCache,
      OperationsMap outstandingOperations,
      OperationsMap completedOperations,
      Map<Digest, ByteString> activeBlobWrites,
      boolean ensureOutputsPresent) {
    super(name);
    this.contentAddressableStorage = contentAddressableStorage;
    this.actionCache = actionCache;
    this.outstandingOperations = outstandingOperations;
    this.completedOperations = completedOperations;
    this.activeBlobWrites = activeBlobWrites;
    this.ensureOutputsPresent = ensureOutputsPresent;
  }

  @Override
  public void start(String publicName) throws IOException {}

  @Override
  public void stop() throws InterruptedException {}

  protected ListenableFuture<Iterable<build.bazel.remote.execution.v2.Digest>>
      findMissingActionResultOutputs(
          @Nullable ActionResult result,
          DigestFunction.Value digestFunction,
          Executor executor,
          RequestMetadata requestMetadata) {
    if (result == null) {
      return immediateFuture(ImmutableList.of());
    }
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> digests = ImmutableList.builder();
    digests.addAll(Iterables.transform(result.getOutputFilesList(), OutputFile::getDigest));
    // findMissingBlobs will weed out empties
    digests.add(result.getStdoutDigest());
    digests.add(result.getStderrDigest());
    ListenableFuture<Void> digestsCompleteFuture = immediateFuture(null);

    Executor contextExecutor = Context.current().fixedContextExecutor(executor);
    for (OutputDirectory directory : result.getOutputDirectoriesList()) {
      // TODO make tree cache
      // create an async function here to avoid initiating the calls to expect immediately
      // no synchronization required on digests, since only one request is running at a time
      AsyncFunction<Void, Void> next =
          v ->
              transform(
                  expect(
                      DigestUtil.fromDigest(directory.getTreeDigest(), digestFunction),
                      build.bazel.remote.execution.v2.Tree.parser(),
                      executor,
                      requestMetadata),
                  tree -> {
                    digests.addAll(enumerateTreeFileDigests(tree));
                    return null;
                  },
                  executor);
      digestsCompleteFuture = transformAsync(digestsCompleteFuture, next, contextExecutor);
    }
    return transformAsync(
        digestsCompleteFuture,
        v -> findMissingBlobs(digests.build(), digestFunction, requestMetadata),
        contextExecutor);
  }

  private ListenableFuture<ActionResult> notFoundNullActionResult(
      ListenableFuture<ActionResult> actionResultFuture) {
    return catchingAsync(
        actionResultFuture,
        Exception.class,
        e -> {
          Status status = Status.fromThrowable(e);
          if (status.getCode() == io.grpc.Status.Code.NOT_FOUND) {
            return immediateFuture(null);
          }
          return immediateFailedFuture(e);
        },
        directExecutor());
  }

  @SuppressWarnings("ConstantConditions")
  protected ListenableFuture<ActionResult> ensureOutputsPresent(
      ListenableFuture<ActionResult> resultFuture,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    ListenableFuture<Iterable<build.bazel.remote.execution.v2.Digest>> missingOutputsFuture =
        transformAsync(
            resultFuture,
            result ->
                findMissingActionResultOutputs(
                    result, digestFunction, directExecutor(), requestMetadata),
            directExecutor());
    return notFoundNullActionResult(
        transformAsync(
            missingOutputsFuture,
            missingOutputs -> {
              if (Iterables.isEmpty(missingOutputs)) {
                return resultFuture;
              }
              return immediateFuture(null);
            },
            directExecutor()));
  }

  private static boolean requestFlag(
      RequestMetadata requestMetadata, String name, boolean flagDefault) {
    try {
      URI uri = new URI(requestMetadata.getCorrelatedInvocationsId());
      QueryStringDecoder decoder = new QueryStringDecoder(uri);
      return decoder
          .parameters()
          .getOrDefault(name, ImmutableList.of(flagDefault ? "true" : "false"))
          .getFirst()
          .equals("true");
    } catch (URISyntaxException e) {
      return flagDefault;
    }
  }

  private static boolean shouldEnsureOutputsPresent(
      boolean ensureOutputsPresent, RequestMetadata requestMetadata) {
    // The 'ensure outputs present' setting means that the AC will only return results to the client
    // when all of the action output blobs are present in the CAS.  If any one blob is missing, the
    // system will return a cache miss.  Although this is a more expensive check to perform, some
    // users may want to enable this feature. It may be useful if you cannot rely on requestMetadata
    // of incoming messages (perhaps due to a proxy). Or other build systems may not be reliable
    // without this extra check.

    // The behavior is determined dynamically from optional URI parameters.
    // If unspecified, we perform the outputs present check if the system is globally configured to
    // check for it.
    return requestFlag(requestMetadata, "ENSURE_OUTPUTS_PRESENT", ensureOutputsPresent);
  }

  protected static boolean shouldMergeExecutions(
      boolean mergeExecutions, RequestMetadata requestMetadata) {
    return requestFlag(requestMetadata, "MERGE_EXECUTIONS", mergeExecutions);
  }

  @Override
  public ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    ListenableFuture<ActionResult> result = checkNotNull(actionCache.get(actionKey));
    if (shouldEnsureOutputsPresent(ensureOutputsPresent, requestMetadata)) {
      result =
          checkNotNull(
              ensureOutputsPresent(
                  result, actionKey.getDigest().getDigestFunction(), requestMetadata));
    }
    return result;
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws InterruptedException {
    if (actionResult.getExitCode() == 0) {
      actionCache.put(actionKey, actionResult);
    }
  }

  @Override
  public String readResourceName(Compressor.Value compressor, Digest blobDigest) {
    return ResourceParser.downloadResourceName(
        DownloadBlobRequest.newBuilder()
            .setInstanceName(getName())
            .setBlob(BlobInformation.newBuilder().setCompressor(compressor).setDigest(blobDigest))
            .build());
  }

  @Override
  public InputStream newBlobInput(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    return contentAddressableStorage.newInput(compressor, digest, offset);
  }

  @Override
  public boolean isReadOnly() {
    return contentAddressableStorage.isReadOnly();
  }

  @Override
  public Write getBlobWrite(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      UUID uuid,
      RequestMetadata requestMetadata)
      throws EntryLimitException {
    return contentAddressableStorage.getWrite(compressor, digest, uuid, requestMetadata);
  }

  @Override
  public ListenableFuture<List<Response>> getAllBlobsFuture(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    return contentAddressableStorage.getAllFuture(digests, digestFunction);
  }

  protected ByteString getBlob(Digest blobDigest) throws InterruptedException {
    return getBlob(blobDigest, /* count= */ blobDigest.getSize());
  }

  ByteString getBlob(Digest blobDigest, long count) throws IndexOutOfBoundsException {
    if (blobDigest.getSize() == 0) {
      if (count >= 0) {
        return ByteString.EMPTY;
      } else {
        throw new IndexOutOfBoundsException();
      }
    }

    Blob blob = contentAddressableStorage.get(blobDigest);

    if (blob == null) {
      return null;
    }

    if ((!blob.isEmpty() && 0 >= blob.size()) || count < 0) {
      throw new IndexOutOfBoundsException();
    }

    return blob.getData().substring(0, (int) (Math.min(count, blob.size())));
  }

  protected ListenableFuture<ByteString> getBlobFuture(
      Compressor.Value compressor, Digest blobDigest, RequestMetadata requestMetadata) {
    return getBlobFuture(
        compressor, blobDigest, /* count= */ blobDigest.getSize(), requestMetadata);
  }

  protected ListenableFuture<ByteString> getBlobFuture(
      Compressor.Value compressor, Digest blobDigest, long count, RequestMetadata requestMetadata) {
    SettableFuture<ByteString> future = SettableFuture.create();
    getBlob(
        compressor,
        blobDigest,
        /* offset= */ 0,
        count,
        new ServerCallStreamObserver<ByteString>() {
          ByteString content = ByteString.EMPTY;

          @Override
          public boolean isCancelled() {
            return false;
          }

          @Override
          public void setCompression(String compression) {}

          @Override
          public void setOnCancelHandler(Runnable onCancelHandler) {}

          @Override
          public void disableAutoInboundFlowControl() {}

          @Override
          public boolean isReady() {
            return true;
          }

          @Override
          public void request(int count) {}

          @Override
          public void setMessageCompression(boolean enable) {}

          @Override
          public void setOnReadyHandler(Runnable onReadyHandler) {
            onReadyHandler.run();
          }

          @Override
          public void onNext(ByteString chunk) {
            content = content.concat(chunk);
          }

          @Override
          public void onCompleted() {
            future.set(content);
          }

          @Override
          public void onError(Throwable t) {
            future.setException(t);
          }
        },
        requestMetadata);
    return future;
  }

  @Override
  public void getBlob(
      Compressor.Value compressor,
      Digest blobDigest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    contentAddressableStorage.get(
        compressor, blobDigest, offset, count, blobObserver, requestMetadata);
  }

  @Override
  public boolean containsBlob(
      Digest digest,
      build.bazel.remote.execution.v2.Digest.Builder result,
      RequestMetadata requestMetadata)
      throws InterruptedException {
    return contentAddressableStorage.contains(digest, result);
  }

  @Override
  public Iterable<build.bazel.remote.execution.v2.Digest> putAllBlobs(
      Iterable<BatchUpdateBlobsRequest.Request> requests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException {
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> blobDigestsBuilder =
        new ImmutableList.Builder<>();
    PutAllBlobsException exception = null;
    for (BatchUpdateBlobsRequest.Request request : requests) {
      build.bazel.remote.execution.v2.Digest digest = request.getDigest();
      try {
        Digest responseDigest =
            putBlob(
                this,
                request.getCompressor(),
                DigestUtil.fromDigest(digest, digestFunction),
                request.getData(),
                1,
                SECONDS,
                requestMetadata);
        blobDigestsBuilder.add(DigestUtil.toDigest(responseDigest));
      } catch (StatusException e) {
        if (exception == null) {
          exception = new PutAllBlobsException();
        }
        com.google.rpc.Status status = StatusProto.fromThrowable(e);
        if (status == null) {
          status =
              com.google.rpc.Status.newBuilder()
                  .setCode(Status.fromThrowable(e).getCode().value())
                  .build();
        }
        exception.addFailedResponse(
            BatchUpdateBlobsResponse.Response.newBuilder()
                .setDigest(digest)
                .setStatus(status)
                .build());
      }
    }
    if (exception != null) {
      throw exception;
    }
    return blobDigestsBuilder.build();
  }

  @Override
  public ListenableFuture<Iterable<build.bazel.remote.execution.v2.Digest>> findMissingBlobs(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    Thread findingThread = Thread.currentThread();
    Context.CancellationListener cancellationListener = (context) -> findingThread.interrupt();
    Context.current().addListener(cancellationListener, directExecutor());
    try {
      ListenableFuture<Iterable<build.bazel.remote.execution.v2.Digest>> future =
          immediateFuture(contentAddressableStorage.findMissingBlobs(digests, digestFunction));
      Context.current().removeListener(cancellationListener);
      return future;
    } catch (InterruptedException e) {
      return immediateFailedFuture(e);
    }
  }

  protected abstract int getTreeDefaultPageSize();

  protected abstract int getTreeMaxPageSize();

  protected abstract TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, build.buildfarm.v1test.Digest rootDigest, String pageToken);

  @Override
  public String getTree(
      build.buildfarm.v1test.Digest rootDigest, int pageSize, String pageToken, Tree.Builder tree) {
    tree.setRootDigest(rootDigest);

    if (pageSize == 0) {
      pageSize = getTreeDefaultPageSize();
    }
    if (pageSize >= 0 && pageSize > getTreeMaxPageSize()) {
      pageSize = getTreeMaxPageSize();
    }

    TokenizableIterator<DirectoryEntry> iter = createTreeIterator("getTree", rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      DirectoryEntry entry = iter.next();
      Directory directory = entry.getDirectory();
      // If part of the tree is missing from the CAS, the server will return the
      // portion present and omit the rest.
      if (directory != null) {
        tree.putDirectories(entry.getDigest().getHash(), directory);
        if (pageSize > 0) {
          pageSize--;
        }
      }
    }
    return iter.toNextPageToken();
  }

  private interface ContentWriteFactory {
    Write create(Digest digest) throws IOException;
  }

  private static void assignAuthorization(String host, HttpURLConnection connection) {
    Credentials credentials = Netrc.getInstance().getCredentials(host);
    if (credentials != null
        && !credentials.getUserPrincipal().getName().isEmpty()
        && !credentials.getPassword().isEmpty()) {
      String authorization =
          credentials.getUserPrincipal().getName() + ":" + credentials.getPassword();
      String basic =
          Base64.getEncoder().encodeToString(authorization.getBytes(StandardCharsets.UTF_8));
      connection.setRequestProperty(AUTHORIZATION, "Basic " + basic);
    }
  }

  private static ListenableFuture<Digest> downloadUrl(
      URL url,
      String expectedHash,
      Map<String, String> headers,
      DigestUtil digestUtil,
      ContentWriteFactory getContentWrite)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    // connect timeout?
    // proxy?
    assignAuthorization(url.getHost(), connection);
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }
    connection.setInstanceFollowRedirects(true);
    // request timeout?
    long contentLength = connection.getContentLengthLong();
    int status = connection.getResponseCode();

    if (status != HttpURLConnection.HTTP_OK) {
      String message = connection.getResponseMessage();
      // per docs, returns null if no valid string can be discerned
      // from the responses, i.e. invalid HTTP
      if (message == null) {
        message = "Invalid HTTP Response";
      }
      message = "Download Failed: " + message + " from " + url;
      throw new IOException(message);
    }

    IOSupplier<InputStream> inSupplier;
    if (expectedHash.isEmpty() || contentLength < 0) {
      // not great, plenty risky for large objects
      ByteString data;
      try (InputStream in = connection.getInputStream()) {
        data = ByteString.readFrom(in);
      }
      if (expectedHash.isEmpty()) {
        expectedHash = digestUtil.computeHash(data).toString();
      }
      contentLength = data.size();
      inSupplier = data::newInput;
    } else {
      inSupplier = connection::getInputStream;
    }
    Digest digest = digestUtil.build(expectedHash, contentLength);

    Write write = getContentWrite.create(digest);

    try (InputStream in = inSupplier.get();
        OutputStream out = write.getOutput(1, DAYS, () -> {})) {
      ByteStreams.copy(in, out);
    } catch (Write.WriteCompleteException e) {
      // ignore - completed write transform below delivers early result, future should be done
    }

    return transform(write.getFuture(), committedSize -> digest, directExecutor());
  }

  @Override
  public ListenableFuture<Digest> fetchBlob(
      Iterable<String> uris,
      Map<String, String> headers,
      Digest expectedDigest,
      RequestMetadata requestMetadata) {
    ImmutableList.Builder<URL> urls = ImmutableList.builder();
    for (String uri : uris) {
      try {
        urls.add(new URL(new java.net.URL(uri)));
      } catch (Exception e) {
        return immediateFailedFuture(e);
      }
    }
    return fetchBlobUrls(urls.build(), headers, expectedDigest, requestMetadata);
  }

  /**
   * Some headers in `allHeaders` are intended ONLY for specific URLs indexes, for example, AUTH
   * headers that we wouldn't want exposed to all URL (mirrors).
   *
   * <p>If a header is prefixed with "<int>:", it is intended for the URL at index <int>. If a
   * header is not prefixed, it is intended for all URLs. If a header is indexed, it is higher
   * priority and will override existing headers with the same key.
   *
   * @param allHeaders
   * @param urlIndex
   * @return
   */
  @VisibleForTesting
  static @Nonnull Map<String, String> getHeadersForUrlIndex(
      @Nonnull Map<String, String> allHeaders, int urlIndex) {
    Preconditions.checkArgument(urlIndex >= 0, "urlIndex must be >= 0");
    Map<String, String> mapBuilder = new HashMap<>();
    // Copy over all the "global" headers that apply to all URLs
    Pattern urlIndexPattern = Pattern.compile("^\\d+:.*$");
    allHeaders.forEach(
        (key, value) -> {
          if (!urlIndexPattern.matcher(key).matches()) {
            // it's a global header
            mapBuilder.put(key, value);
          }
        });
    String urlIndexPrefix = urlIndex + ":";
    allHeaders.forEach(
        (key, value) -> {
          if (key.startsWith(urlIndexPrefix)) {
            // it's an indexed header
            mapBuilder.put(key.substring(urlIndexPrefix.length()), value);
          }
        });
    return mapBuilder;
  }

  @VisibleForTesting
  ListenableFuture<Digest> fetchBlobUrls(
      Iterable<URL> urls,
      Map<String, String> headers,
      Digest expectedDigest,
      RequestMetadata requestMetadata) {
    int urlIndex = 0;
    for (URL url : urls) {
      try {
        // some minor abuse here, we want the download to set our built digest size as side effect
        return downloadUrl(
            url,
            expectedDigest.getHash(),
            getHeadersForUrlIndex(headers, urlIndex),
            new DigestUtil(HashFunction.get(expectedDigest.getDigestFunction())),
            actualDigest -> {
              if (!expectedDigest.getHash().isEmpty()
                  && expectedDigest.getSize() >= 0
                  && expectedDigest.getSize() != actualDigest.getSize()) {
                // TODO digestFunction
                throw new DigestMismatchException(actualDigest, expectedDigest);
              }
              return getBlobWrite(
                  Compressor.Value.IDENTITY, actualDigest, UUID.randomUUID(), requestMetadata);
            });
      } catch (Exception e) {
        log.log(Level.WARNING, "download attempt failed", e);
        // ignore?
      }
      urlIndex++;
    }
    return immediateFailedFuture(new NoSuchFileException(expectedDigest.getHash()));
  }

  private static void stringsUniqueAndSortedPrecondition(
      Iterable<String> strings,
      String duplicateViolationMessage,
      String unsortedViolationMessage,
      PreconditionFailure.Builder preconditionFailure) {
    String lastString = "";
    for (String string : strings) {
      int direction = lastString.compareTo(string);
      if (direction == 0) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(string)
            .setDescription(duplicateViolationMessage);
      }
      if (direction > 0) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(lastString + " > " + string)
            .setDescription(unsortedViolationMessage);
      }
    }
  }

  private static void filesUniqueAndSortedPrecondition(
      Iterable<String> files, PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        files, DUPLICATE_DIRENT, DIRECTORY_NOT_SORTED, preconditionFailure);
  }

  private static void environmentVariablesUniqueAndSortedPrecondition(
      Iterable<Command.EnvironmentVariable> environmentVariables,
      PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        Iterables.transform(environmentVariables, Command.EnvironmentVariable::getName),
        DUPLICATE_ENVIRONMENT_VARIABLE,
        ENVIRONMENT_VARIABLES_NOT_SORTED,
        preconditionFailure);
  }

  private static void enumerateActionInputDirectory(
      DigestFunction.Value digestFunction,
      String directoryPath,
      Directory directory,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Consumer<String> onInputFile,
      Consumer<String> onInputDirectory,
      PreconditionFailure.Builder preconditionFailure) {
    Stack<DirectoryNode> directoriesStack = new Stack<>();
    directoriesStack.addAll(directory.getDirectoriesList());

    while (!directoriesStack.isEmpty()) {
      DirectoryNode directoryNode = directoriesStack.pop();
      String directoryName = directoryNode.getName();
      build.bazel.remote.execution.v2.Digest directoryDigest = directoryNode.getDigest();
      String subDirectoryPath =
          directoryPath.isEmpty() ? directoryName : (directoryPath + "/" + directoryName);
      onInputDirectory.accept(subDirectoryPath);

      Directory subDirectory;
      if (directoryDigest.getSizeBytes() == 0) {
        subDirectory = Directory.getDefaultInstance();
      } else {
        subDirectory = directoriesIndex.get(directoryDigest);
      }

      if (subDirectory == null) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_MISSING)
            .setSubject(
                "blobs/"
                    + DigestUtil.toString(DigestUtil.fromDigest(directoryDigest, digestFunction)))
            .setDescription("The directory `/" + subDirectoryPath + "` was not found in the CAS.");
      } else {
        for (FileNode fileNode : subDirectory.getFilesList()) {
          String fileName = fileNode.getName();
          String filePath = subDirectoryPath + "/" + fileName;
          onInputFile.accept(filePath);
        }

        for (DirectoryNode subDirectoryNode : subDirectory.getDirectoriesList()) {
          directoriesStack.push(subDirectoryNode);
        }
      }
    }
  }

  @SuppressWarnings("SameReturnValue")
  private static boolean isValidFilename() {
    // for now, assume all filenames are valid
    return true;
  }

  @VisibleForTesting
  public static void validateActionInputDirectory(
      DigestFunction.Value digestFunction,
      String directoryPath,
      Directory directory,
      Stack<build.bazel.remote.execution.v2.Digest> pathDigests,
      Set<build.bazel.remote.execution.v2.Digest> visited,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      boolean allowSymlinkTargetAbsolute,
      Consumer<String> onInputFile,
      Consumer<String> onInputDirectory,
      Consumer<build.bazel.remote.execution.v2.Digest> onInputDigest,
      PreconditionFailure.Builder preconditionFailure) {
    Set<String> entryNames = new HashSet<>();

    String lastFileName = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String fileName = fileNode.getName();
      if (entryNames.contains(fileName)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + fileName)
            .setDescription(DUPLICATE_DIRENT);
      } else if (lastFileName.compareTo(fileName) > 0) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + lastFileName + " > " + fileName)
            .setDescription(DIRECTORY_NOT_SORTED);
      }
      // FIXME serverside validity check? regex?
      Preconditions.checkState(isValidFilename(), INVALID_FILE_NAME);

      lastFileName = fileName;
      entryNames.add(fileName);

      onInputDigest.accept(fileNode.getDigest());
      String filePath = directoryPath.isEmpty() ? fileName : (directoryPath + "/" + fileName);
      onInputFile.accept(filePath);
    }
    String lastSymlinkName = "";
    for (SymlinkNode symlinkNode : directory.getSymlinksList()) {
      String symlinkName = symlinkNode.getName();
      if (entryNames.contains(symlinkName)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + symlinkName)
            .setDescription(DUPLICATE_DIRENT);
      } else if (lastSymlinkName.compareTo(symlinkName) > 0) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + lastSymlinkName + " > " + symlinkName)
            .setDescription(DIRECTORY_NOT_SORTED);
      }
      String symlinkTarget = symlinkNode.getTarget();
      if (!allowSymlinkTargetAbsolute && symlinkTarget.charAt(0) == '/') {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + symlinkName + " -> " + symlinkTarget)
            .setDescription(SYMLINK_TARGET_ABSOLUTE);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          isValidFilename(symlinkName),
          INVALID_FILE_NAME);
      Preconditions.checkState(
          isValidFilename(symlinkNode.getTarget()),
          INVALID_FILE_NAME);
      // FIXME verify that any relative pathing for the target is within the input root
      */
      lastSymlinkName = symlinkName;
      entryNames.add(symlinkName);
    }
    String lastDirectoryName = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      String directoryName = directoryNode.getName();

      if (entryNames.contains(directoryName)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + directoryName)
            .setDescription(DUPLICATE_DIRENT);
      } else if (lastDirectoryName.compareTo(directoryName) > 0) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject("/" + directoryPath + ": " + lastDirectoryName + " > " + directoryName)
            .setDescription(DIRECTORY_NOT_SORTED);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          isValidFilename(directoryName),
          INVALID_FILE_NAME);
      */
      lastDirectoryName = directoryName;
      entryNames.add(directoryName);

      build.bazel.remote.execution.v2.Digest directoryDigest = directoryNode.getDigest();
      if (pathDigests.contains(directoryDigest)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_CYCLE_DETECTED)
            .setDescription("/" + directoryPath + ": " + directoryName);
      } else {
        String subDirectoryPath =
            directoryPath.isEmpty() ? directoryName : (directoryPath + "/" + directoryName);
        onInputDirectory.accept(subDirectoryPath);
        if (visited.contains(directoryDigest)) {
          Directory subDirectory;
          if (directoryDigest.getSizeBytes() == 0) {
            subDirectory = Directory.getDefaultInstance();
          } else {
            subDirectory = directoriesIndex.get(directoryDigest);
          }
          enumerateActionInputDirectory(
              digestFunction,
              subDirectoryPath,
              subDirectory,
              directoriesIndex,
              onInputFile,
              onInputDirectory,
              preconditionFailure);
        } else {
          validateActionInputDirectoryDigest(
              subDirectoryPath,
              DigestUtil.fromDigest(directoryDigest, digestFunction),
              pathDigests,
              visited,
              directoriesIndex,
              allowSymlinkTargetAbsolute,
              onInputFile,
              onInputDirectory,
              onInputDigest,
              preconditionFailure);
        }
      }
    }
  }

  private static void validateActionInputDirectoryDigest(
      String directoryPath,
      // based on usage might want to make this bazel and pass function
      Digest directoryDigest,
      Stack<build.bazel.remote.execution.v2.Digest> pathDigests,
      Set<build.bazel.remote.execution.v2.Digest> visited,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      boolean allowSymlinkTargetAbsolute,
      Consumer<String> onInputFile,
      Consumer<String> onInputDirectory,
      Consumer<build.bazel.remote.execution.v2.Digest> onInputDigest,
      PreconditionFailure.Builder preconditionFailure) {
    build.bazel.remote.execution.v2.Digest digest = DigestUtil.toDigest(directoryDigest);
    pathDigests.push(digest);
    final Directory directory;
    if (digest.getSizeBytes() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(digest);
    }
    if (directory == null) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(directoryDigest))
          .setDescription("The directory `/" + directoryPath + "` was not found in the CAS.");
    } else {
      validateActionInputDirectory(
          directoryDigest.getDigestFunction(),
          directoryPath,
          directory,
          pathDigests,
          visited,
          directoriesIndex,
          allowSymlinkTargetAbsolute,
          onInputFile,
          onInputDirectory,
          onInputDigest,
          preconditionFailure);
    }
    pathDigests.pop();
    if (directory != null) {
      // missing directories are not visited and will appear in violations list each time
      visited.add(digest);
    }
  }

  protected ListenableFuture<Tree> getTreeFuture(
      String reason, Digest inputRoot, ExecutorService service, RequestMetadata requestMetadata) {
    return listeningDecorator(service)
        .submit(
            () -> {
              Tree.Builder tree = Tree.newBuilder().setRootDigest(inputRoot);

              TokenizableIterator<DirectoryEntry> iterator =
                  createTreeIterator(
                      reason, inputRoot, /* pageToken= */ Instance.SENTINEL_PAGE_TOKEN);
              while (iterator.hasNext()) {
                DirectoryEntry entry = iterator.next();
                Directory directory = entry.getDirectory();
                if (directory != null) {
                  tree.putDirectories(entry.getDigest().getHash(), directory);
                }
              }

              return tree.build();
            });
  }

  private void validateInputs(
      Iterable<build.bazel.remote.execution.v2.Digest> inputDigests,
      DigestFunction.Value digestFunction,
      PreconditionFailure.Builder preconditionFailure,
      RequestMetadata requestMetadata)
      throws StatusException, InterruptedException {
    ListenableFuture<Void> result =
        transform(
            findMissingBlobs(inputDigests, digestFunction, requestMetadata),
            (missingBlobDigests) -> {
              preconditionFailure.addAllViolations(
                  StreamSupport.stream(missingBlobDigests.spliterator(), false)
                      .map(
                          digest ->
                              Violation.newBuilder()
                                  .setType(VIOLATION_TYPE_MISSING)
                                  .setSubject(
                                      "blobs/"
                                          + DigestUtil.toString(
                                              DigestUtil.fromDigest(digest, digestFunction)))
                                  .setDescription(MISSING_INPUT)
                                  .build())
                      .collect(Collectors.toList()));
              return null;
            },
            directExecutor());
    try {
      result.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      com.google.rpc.Status status = StatusProto.fromThrowable(cause);
      if (status == null) {
        getLogger().log(Level.SEVERE, "no rpc status from exception", cause);
        status = asExecutionStatus(cause);
      } else if (Code.forNumber(status.getCode()) == Code.DEADLINE_EXCEEDED) {
        log.log(
            Level.WARNING, "an rpc status was thrown with DEADLINE_EXCEEDED, discarding it", cause);
        status =
            com.google.rpc.Status.newBuilder()
                .setCode(com.google.rpc.Code.UNAVAILABLE.getNumber())
                .setMessage("SUPPRESSED DEADLINE_EXCEEDED: " + cause.getMessage())
                .build();
      }
      throw StatusProto.toStatusException(status);
    }
  }

  public static <V> V getUnchecked(ListenableFuture<V> future) throws InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      return null;
    }
  }

  protected void validateQueuedOperationAndInputs(
      Digest actionDigest,
      QueuedOperation queuedOperation,
      PreconditionFailure.Builder preconditionFailure,
      RequestMetadata requestMetadata)
      throws StatusException, InterruptedException {
    DigestFunction.Value digestFunction =
        queuedOperation.getTree().getRootDigest().getDigestFunction();
    if (!queuedOperation.hasAction()) {
      log.warning("queued operation has no action");
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(actionDigest))
          .setDescription(MISSING_ACTION);
    } else {
      ImmutableSet.Builder<build.bazel.remote.execution.v2.Digest> inputDigestsBuilder =
          ImmutableSet.builder();
      validateAction(
          digestFunction,
          queuedOperation.getAction(),
          queuedOperation.hasCommand() ? queuedOperation.getCommand() : null,
          new ProxyDirectoriesIndex(queuedOperation.getTree().getDirectoriesMap()),
          inputDigestsBuilder::add,
          preconditionFailure);
      validateInputs(
          inputDigestsBuilder.build(), digestFunction, preconditionFailure, requestMetadata);
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());
  }

  private void validateActionDigest(
      String operationName, Digest actionDigest, RequestMetadata requestMetadata)
      throws StatusException, InterruptedException {
    Action action = null;
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    ByteString actionBlob = null;
    if (actionDigest.getSize() != 0) {
      actionBlob = getBlob(actionDigest);
    }
    if (actionBlob == null) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(actionDigest))
          .setDescription(MISSING_ACTION);
    } else {
      try {
        action = Action.parseFrom(actionBlob);
      } catch (InvalidProtocolBufferException e) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(INVALID_ACTION)
            .setDescription("Action " + DigestUtil.toString(actionDigest));
      }
      if (action != null) {
        validateAction(
            operationName,
            actionDigest.getDigestFunction(),
            action,
            preconditionFailure,
            requestMetadata);
      }
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());
  }

  @SuppressWarnings("ConstantConditions")
  protected void validateAction(
      String operationName,
      DigestFunction.Value digestFunction,
      Action action,
      PreconditionFailure.Builder preconditionFailure,
      RequestMetadata requestMetadata)
      throws InterruptedException, StatusException {
    ExecutorService service = newDirectExecutorService();
    ImmutableSet.Builder<build.bazel.remote.execution.v2.Digest> inputDigestsBuilder =
        ImmutableSet.builder();
    Tree tree =
        getUnchecked(
            getTreeFuture(
                operationName,
                DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction),
                service,
                requestMetadata));
    validateAction(
        digestFunction,
        action,
        getUnchecked(
            expect(
                DigestUtil.fromDigest(action.getCommandDigest(), digestFunction),
                Command.parser(),
                service,
                requestMetadata)),
        new ProxyDirectoriesIndex(tree.getDirectoriesMap()),
        inputDigestsBuilder::add,
        preconditionFailure);
    validateInputs(
        inputDigestsBuilder.build(), digestFunction, preconditionFailure, requestMetadata);
  }

  protected void validateQueuedOperation(Digest actionDigest, QueuedOperation queuedOperation)
      throws StatusException {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    validateAction(
        queuedOperation.getTree().getRootDigest().getDigestFunction(),
        queuedOperation.getAction(),
        queuedOperation.hasCommand() ? queuedOperation.getCommand() : null,
        new ProxyDirectoriesIndex(queuedOperation.getTree().getDirectoriesMap()),
        digest -> {},
        preconditionFailure);
    checkPreconditionFailure(actionDigest, preconditionFailure.build());
  }

  protected void validatePlatform(
      Platform platform, PreconditionFailure.Builder preconditionFailure) {
    /* no default platform validation */
  }

  @VisibleForTesting
  void validateCommand(
      Command command,
      build.bazel.remote.execution.v2.Digest inputRootDigest,
      Set<String> inputFiles,
      Set<String> inputDirectories,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      PreconditionFailure.Builder preconditionFailure) {
    validatePlatform(command.getPlatform(), preconditionFailure);

    // FIXME should input/output collisions (through directories) be another
    // invalid action?
    filesUniqueAndSortedPrecondition(command.getOutputFilesList(), preconditionFailure);
    filesUniqueAndSortedPrecondition(command.getOutputDirectoriesList(), preconditionFailure);

    validateOutputs(
        inputFiles,
        inputDirectories,
        Sets.newHashSet(command.getOutputFilesList()),
        Sets.newHashSet(command.getOutputDirectoriesList()),
        preconditionFailure);

    environmentVariablesUniqueAndSortedPrecondition(
        command.getEnvironmentVariablesList(), preconditionFailure);
    if (command.getArgumentsList().isEmpty()) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_COMMAND)
          .setDescription("argument list is empty");
    }

    String workingDirectory = command.getWorkingDirectory();
    if (!workingDirectory.isEmpty()) {
      if (workingDirectory.startsWith("/")) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(INVALID_COMMAND)
            .setDescription("working directory is absolute");
      } else {
        Directory directory = directoriesIndex.get(inputRootDigest);
        for (String segment : workingDirectory.split("/")) {
          if (segment.equals(".")) {
            continue;
          }
          Directory nextDirectory = directory;
          // linear for now
          for (DirectoryNode dirNode : directory.getDirectoriesList()) {
            if (dirNode.getName().equals(segment)) {
              nextDirectory = directoriesIndex.get(dirNode.getDigest());
              break;
            }
          }
          if (nextDirectory == directory) {
            preconditionFailure
                .addViolationsBuilder()
                .setType(VIOLATION_TYPE_INVALID)
                .setSubject(INVALID_COMMAND)
                .setDescription("working directory is not an input directory");
            break;
          }
          directory = nextDirectory;
        }
      }
    }
  }

  protected void validateAction(
      DigestFunction.Value digestFunction,
      Action action,
      @Nullable Command command,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Consumer<build.bazel.remote.execution.v2.Digest> onInputDigest,
      PreconditionFailure.Builder preconditionFailure) {
    ImmutableSet.Builder<String> inputDirectoriesBuilder = ImmutableSet.builder();
    ImmutableSet.Builder<String> inputFilesBuilder = ImmutableSet.builder();

    inputDirectoriesBuilder.add(ACTION_INPUT_ROOT_DIRECTORY_PATH);
    boolean allowSymlinkTargetAbsolute =
        getCacheCapabilities().getSymlinkAbsolutePathStrategy()
            == SymlinkAbsolutePathStrategy.Value.ALLOWED;
    validateActionInputDirectoryDigest(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction),
        new Stack<>(),
        new HashSet<>(),
        directoriesIndex,
        allowSymlinkTargetAbsolute,
        inputFilesBuilder::add,
        inputDirectoriesBuilder::add,
        onInputDigest,
        preconditionFailure);

    if (command == null) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(
              "blobs/"
                  + DigestUtil.toString(
                      DigestUtil.fromDigest(action.getCommandDigest(), digestFunction)))
          .setDescription(MISSING_COMMAND);
    } else {
      validateCommand(
          command,
          action.getInputRootDigest(),
          inputFilesBuilder.build(),
          inputDirectoriesBuilder.build(),
          directoriesIndex,
          preconditionFailure);
    }
  }

  @VisibleForTesting
  static void validateOutputs(
      Set<String> inputFiles,
      Set<String> inputDirectories,
      Set<String> outputFiles,
      Set<String> outputDirectories,
      PreconditionFailure.Builder preconditionFailure) {
    Set<String> outputFilesAndDirectories = Sets.intersection(outputFiles, outputDirectories);
    if (!outputFilesAndDirectories.isEmpty()) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(OUTPUT_FILE_DIRECTORY_COLLISION)
          .setDescription(outputFilesAndDirectories.toString());
    }

    Set<String> parentsOfOutputs = new HashSet<>();

    // An output file cannot be a parent of another output file, be a child of a listed output
    // directory, or have the same path as any of the listed output directories.
    for (String outputFile : outputFiles) {
      if (inputDirectories.contains(outputFile)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(OUTPUT_FILE_IS_INPUT_DIRECTORY)
            .setDescription(outputFile);
      }
      String currentPath = outputFile;
      while (!currentPath.equals("")) {
        final String dirname;
        if (currentPath.contains("/")) {
          dirname = currentPath.substring(0, currentPath.lastIndexOf('/'));
        } else {
          dirname = "";
        }
        parentsOfOutputs.add(dirname);
        currentPath = dirname;
      }
    }

    // An output directory cannot be a parent of another output directory, be a parent of a listed
    // output file, or have the same path as any of the listed output files.
    for (String outputDir : outputDirectories) {
      if (inputFiles.contains(outputDir)) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(outputDir)
            .setDescription(OUTPUT_DIRECTORY_IS_INPUT_FILE);
      }
      String currentPath = outputDir;
      while (!currentPath.equals("")) {
        final String dirname;
        if (currentPath.contains("/")) {
          dirname = currentPath.substring(0, currentPath.lastIndexOf('/'));
        } else {
          dirname = "";
        }
        parentsOfOutputs.add(dirname);
        currentPath = dirname;
      }
    }
    Set<String> outputFileAncestors = Sets.intersection(outputFiles, parentsOfOutputs);
    for (String outputFileAncestor : outputFileAncestors) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputFileAncestor)
          .setDescription(OUTPUT_FILE_IS_OUTPUT_ANCESTOR);
    }
    Set<String> outputDirectoryAncestors = Sets.intersection(outputDirectories, parentsOfOutputs);
    for (String outputDirectoryAncestor : outputDirectoryAncestors) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputDirectoryAncestor)
          .setDescription(OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR);
    }
  }

  protected void logFailedStatus(Digest actionDigest, com.google.rpc.Status status) {
    String message =
        format(
            "%s: %s: %s\n",
            DigestUtil.toString(actionDigest),
            Code.forNumber(status.getCode()),
            status.getMessage());
    for (Any detail : status.getDetailsList()) {
      if (detail.is(PreconditionFailure.class)) {
        message += "  PreconditionFailure:\n";
        PreconditionFailure preconditionFailure;
        try {
          preconditionFailure = detail.unpack(PreconditionFailure.class);
          for (Violation violation : preconditionFailure.getViolationsList()) {
            message +=
                format(
                    "    Violation: %s %s: %s\n",
                    violation.getType(), violation.getSubject(), violation.getDescription());
          }
        } catch (InvalidProtocolBufferException e) {
          message += "  " + e.getMessage();
        }
      } else {
        message += "  Unknown Detail\n";
      }
    }
    getLogger().info(message);
  }

  protected static QueuedOperationMetadata maybeQueuedOperationMetadata(String name, Any metadata) {
    if (metadata.is(QueuedOperationMetadata.class)) {
      try {
        return metadata.unpack(QueuedOperationMetadata.class);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, format("invalid executing operation metadata %s", name), e);
      }
    }
    return null;
  }

  protected static RequestMetadata expectRequestMetadata(Operation operation) {
    String name = operation.getName();
    Any metadata = operation.getMetadata();
    QueuedOperationMetadata queuedOperationMetadata = maybeQueuedOperationMetadata(name, metadata);
    if (queuedOperationMetadata != null) {
      return queuedOperationMetadata.getRequestMetadata();
    }
    return RequestMetadata.getDefaultInstance();
  }

  protected static ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    String name = operation.getName();
    Any metadata = operation.getMetadata();
    QueuedOperationMetadata queuedOperationMetadata = maybeQueuedOperationMetadata(name, metadata);
    if (queuedOperationMetadata != null) {
      return queuedOperationMetadata.getExecuteOperationMetadata();
    }
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      log.log(
          Level.SEVERE, format("invalid execute operation metadata %s", operation.getName()), e);
    }
    return null;
  }

  protected <T> ListenableFuture<T> expect(
      Digest digest, Parser<T> parser, Executor executor, RequestMetadata requestMetadata) {
    // FIXME find a way to make this a transform
    SettableFuture<T> future = SettableFuture.create();
    Futures.addCallback(
        getBlobFuture(Compressor.Value.IDENTITY, digest, requestMetadata),
        new FutureCallback<>() {
          @Override
          public void onSuccess(ByteString blob) {
            try {
              future.set(parser.parseFrom(blob));
            } catch (InvalidProtocolBufferException e) {
              log.log(
                  Level.WARNING,
                  format("expect parse for %s failed", DigestUtil.toString(digest)),
                  e);
              future.setException(e);
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            // NOT_FOUNDs are not notable enough to log independently
            if (status.getCode() != io.grpc.Status.Code.NOT_FOUND) {
              log.log(
                  Level.WARNING, format("expect for %s failed", DigestUtil.toString(digest)), t);
            }
            future.setException(t);
          }
        },
        executor);
    return future;
  }

  @SuppressWarnings("ConstantConditions")
  protected static boolean isErrored(Operation operation) {
    return operation.getDone()
        && operation.getResultCase() == Operation.ResultCase.RESPONSE
        && operation.getResponse().is(ExecuteResponse.class)
        && expectExecuteResponse(operation).getStatus().getCode() != Code.OK.getNumber();
  }

  private static boolean isStage(Operation operation, ExecutionStage.Value stage) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    return metadata != null && metadata.getStage() == stage;
  }

  protected static boolean isUnknown(Operation operation) {
    return isStage(operation, ExecutionStage.Value.UNKNOWN);
  }

  @SuppressWarnings("ConstantConditions")
  protected boolean isCancelled(Operation operation) {
    return operation.getDone()
        && operation.getResultCase() == Operation.ResultCase.RESPONSE
        && operation.getResponse().is(ExecuteResponse.class)
        && expectExecuteResponse(operation).getStatus().getCode() == Code.CANCELLED.getNumber();
  }

  protected static ExecuteResponse getExecuteResponse(Operation operation) {
    if (operation.getDone() && operation.getResultCase() == Operation.ResultCase.RESPONSE) {
      return expectExecuteResponse(operation);
    }
    return null;
  }

  private static ExecuteResponse expectExecuteResponse(Operation operation) {
    try {
      return operation.getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  public static boolean isQueued(Operation operation) {
    return isStage(operation, ExecutionStage.Value.QUEUED);
  }

  protected static boolean isExecuting(Operation operation) {
    return isStage(operation, ExecutionStage.Value.EXECUTING);
  }

  protected static boolean isComplete(Operation operation) {
    return isStage(operation, ExecutionStage.Value.COMPLETED);
  }

  protected boolean wasCompletelyExecuted(Operation operation) {
    ExecuteResponse executeResponse = getExecuteResponse(operation);
    return executeResponse != null && !executeResponse.getCachedResult();
  }

  protected static ActionResult getCacheableActionResult(Operation operation) {
    ExecuteResponse executeResponse = getExecuteResponse(operation);
    if (executeResponse != null
        && !executeResponse.getCachedResult()
        && executeResponse.getStatus().getCode() == Code.OK.getNumber()) {
      ActionResult result = executeResponse.getResult();
      if (result.getExitCode() == 0) {
        return result;
      }
    }
    return null;
  }

  @Override
  public void cancelOperation(String name) throws InterruptedException {
    Operation operation = getOperation(name);
    if (operation == null) {
      operation =
          Operation.newBuilder()
              .setName(name)
              .setMetadata(Any.pack(ExecuteOperationMetadata.getDefaultInstance()))
              .build();
    }
    RequestMetadata requestMetadata = expectRequestMetadata(operation);
    errorOperation(
        operation,
        requestMetadata,
        com.google.rpc.Status.newBuilder().setCode(Code.CANCELLED.getNumber()).build());
  }

  protected void errorOperation(
      Operation operation, RequestMetadata requestMetadata, com.google.rpc.Status status)
      throws InterruptedException {
    if (operation.getDone()) {
      throw new IllegalStateException(
          "Trying to error already completed execution [" + operation.getName() + "]");
    }
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      metadata = ExecuteOperationMetadata.getDefaultInstance();
    }
    QueuedOperationMetadata queuedMetadata =
        maybeQueuedOperationMetadata(operation.getName(), operation.getMetadata());
    if (queuedMetadata == null) {
      queuedMetadata =
          QueuedOperationMetadata.newBuilder().setRequestMetadata(requestMetadata).build();
    }
    queuedMetadata =
        queuedMetadata.toBuilder()
            .setExecuteOperationMetadata(
                metadata.toBuilder().setStage(ExecutionStage.Value.COMPLETED))
            .build();
    putOperation(
        operation.toBuilder()
            .setDone(true)
            .setMetadata(Any.pack(queuedMetadata))
            .setResponse(Any.pack(ExecuteResponse.newBuilder().setStatus(status).build()))
            .build());
  }

  protected void expireOperation(Operation operation) throws InterruptedException {
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setExitCode(-1)
            .setStderrRaw(
                ByteString.copyFromUtf8(
                    "[BUILDFARM]: Action timed out with no response from worker"))
            .build();
    ExecuteResponse executeResponse =
        ExecuteResponse.newBuilder()
            .setResult(actionResult)
            .setStatus(
                com.google.rpc.Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED.getNumber()))
            .build();
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      throw new IllegalStateException(
          "Operation " + operation.getName() + " did not contain valid metadata");
    }
    metadata = metadata.toBuilder().setStage(ExecutionStage.Value.COMPLETED).build();
    putOperation(
        operation.toBuilder()
            .setDone(true)
            .setMetadata(Any.pack(metadata))
            .setResponse(Any.pack(executeResponse))
            .build());
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    if (stage != ExecutionStage.Value.QUEUED && stage != ExecutionStage.Value.EXECUTING) {
      return false;
    }
    Operation operation = getOperation(operationName);
    if (operation == null) {
      return false;
    }
    if (isCancelled(operation)) {
      return false;
    }
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      return false;
    }
    // stage limitation to {QUEUED, EXECUTING} above is required
    return metadata.getStage() == stage;
  }

  private Iterable<DigestFunction.Value> getDigestFunctions() {
    return Stream.of(HashFunction.values())
        .map(HashFunction::getDigestFunction)
        .collect(ImmutableList.toImmutableList());
  }

  protected CacheCapabilities getCacheCapabilities() {
    return CacheCapabilities.newBuilder()
        .addAllDigestFunctions(getDigestFunctions())
        .setActionCacheUpdateCapabilities(
            ActionCacheUpdateCapabilities.newBuilder().setUpdateEnabled(true))
        .setMaxBatchTotalSizeBytes(Size.mbToBytes(4))
        .setSymlinkAbsolutePathStrategy(SymlinkAbsolutePathStrategy.Value.DISALLOWED)
        .setMaxCasBlobSizeBytes(configs.getMaxEntrySizeBytes())

        // Compression support
        .addSupportedCompressors(Compressor.Value.IDENTITY)
        .addSupportedCompressors(Compressor.Value.ZSTD)
        .build();
  }

  protected ExecutionCapabilities getExecutionCapabilities() {
    return ExecutionCapabilities.newBuilder()
        .setDigestFunction(configs.getDigestFunction().getDigestFunction())
        .addAllDigestFunctions(getDigestFunctions())
        .setExecEnabled(true)
        .setExecutionPriorityCapabilities(
            PriorityCapabilities.newBuilder()

                // The priority (relative importance) of this action. Generally, a lower value
                // means that the action should be run sooner than actions having a greater
                // priority value, but the interpretation of a given value is server-
                // dependent. A priority of 0 means the *default* priority. Priorities may be
                // positive or negative, and such actions should run later or sooner than
                // actions having the default priority, respectively. The particular semantics
                // of this field is up to the server. In particular, every server will have
                // their own supported range of priorities, and will decide how these map into
                // scheduling policy.
                .addPriorities(
                    PriorityRange.newBuilder()
                        .setMinPriority(Integer.MIN_VALUE)
                        .setMaxPriority(Integer.MAX_VALUE)))
        .build();
  }

  @Override
  public ServerCapabilities getCapabilities() {
    return ServerCapabilities.newBuilder()
        .setCacheCapabilities(getCacheCapabilities())
        .setExecutionCapabilities(getExecutionCapabilities())
        .build();
  }

  @Override
  public ListenableFuture<WorkerProfileMessage> getWorkerProfile(String name) {
    throw new UnsupportedOperationException(
        "NodeInstance doesn't support getWorkerProfile() method.");
  }

  @Override
  public ListenableFuture<BatchWorkerProfilesResponse> batchWorkerProfiles(Iterable<String> names) {
    throw new UnsupportedOperationException(
        "NodeInstance doesn't support batchWorkerProfiles() method.");
  }

  @Override
  public PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully() {
    throw new UnsupportedOperationException(
        "NodeInstance doesn't support shutDownWorkerGracefully() method.");
  }

  @Override
  public abstract GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request);

  @Override
  public abstract CasIndexResults reindexCas();

  @Override
  public abstract void deregisterWorker(String workerName);

  protected abstract Logger getLogger();
}
