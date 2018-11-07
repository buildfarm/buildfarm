// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class AbstractServerInstance implements Instance {
  private final String name;
  protected final ContentAddressableStorage contentAddressableStorage;
  protected final Map<ActionKey, ActionResult> actionCache;
  protected final Map<String, Operation> outstandingOperations;
  protected final Map<String, Operation> completedOperations;
  protected final Map<Digest, ByteString> activeBlobWrites;
  protected final DigestUtil digestUtil;

  public static final String DUPLICATE_FILE_NODE =
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

  public static final String MISSING_INPUT =
      "A requested input (or the `Command` of the `Action`) was not found in the CAS.";

  public static final String INVALID_DIGEST = "A `Digest` in the input tree is invalid.";

  public static final String INVALID_COMMAND = "The `Command` of the `Action` was invalid.";

  public static final String INVALID_FILE_NAME =
      "One of the input `PathNode`s has an invalid name, such as a name containing a `/` character"
          + " or another character which cannot be used in a file's name on the filesystem of the"
          + " worker.";

  public static final String VIOLATION_TYPE_MISSING = "MISSING";

  public static final String VIOLATION_TYPE_INVALID = "INVALID";

  public AbstractServerInstance(
      String name,
      DigestUtil digestUtil,
      ContentAddressableStorage contentAddressableStorage,
      Map<ActionKey, ActionResult> actionCache,
      Map<String, Operation> outstandingOperations,
      Map<String, Operation> completedOperations,
      Map<Digest, ByteString> activeBlobWrites) {
    this.name = name;
    this.digestUtil = digestUtil;
    this.contentAddressableStorage = contentAddressableStorage;
    this.actionCache = actionCache;
    this.outstandingOperations = outstandingOperations;
    this.completedOperations = completedOperations;
    this.activeBlobWrites = activeBlobWrites;
  }

  @Override
  public void start() { }

  @Override
  public void stop() throws InterruptedException { }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    return actionCache.get(actionKey);
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    if (actionResult.getExitCode() == 0) {
      actionCache.put(actionKey, actionResult);
    }
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return String.format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  public final ByteString getBlob(Digest blobDigest) throws IOException, InterruptedException {
    return getBlob(blobDigest, 0, 0);
  }

  public ByteString getBlob(Digest blobDigest, long offset, long limit)
      throws IOException, IndexOutOfBoundsException, InterruptedException {
    if (blobDigest.getSizeBytes() == 0) {
      if (offset == 0 && limit >= 0) {
        return ByteString.EMPTY;
      } else {
        throw new IndexOutOfBoundsException();
      }
    }

    Blob blob = contentAddressableStorage.get(blobDigest);

    if (blob == null) {
      return null;
    }

    if (offset < 0
        || (blob.isEmpty() && offset > 0)
        || (!blob.isEmpty() && offset >= blob.size())
        || limit < 0) {
      throw new IndexOutOfBoundsException();
    }

    long endIndex = offset + (limit > 0 ? limit : (blob.size() - offset));

    return blob.getData().substring(
        (int) offset, (int) (endIndex > blob.size() ? blob.size() : endIndex));
  }

  protected ListenableFuture<ByteString> getBlobFuture(Digest blobDigest) {
    return getBlobFuture(blobDigest, /* offset=*/ 0, /* limit=*/ 0);
  }

  protected ListenableFuture<ByteString> getBlobFuture(Digest blobDigest, long offset, long limit) {
    SettableFuture<ByteString> future = SettableFuture.create();
    getBlob(blobDigest, offset, limit, new StreamObserver<ByteString>() {
      ByteString content = ByteString.EMPTY;

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
    });
    return future;
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    try {
      ByteString blob = getBlob(blobDigest, offset, limit);

      blobObserver.onNext(blob);
      blobObserver.onCompleted();
    } catch (IOException|InterruptedException e) {
      blobObserver.onError(e);
    }
  }

  @Override
  public ChunkObserver getWriteBlobObserver(Digest blobDigest) {
    // what should the locking semantics be here??
    activeBlobWrites.putIfAbsent(blobDigest, ByteString.EMPTY);
    return new ChunkObserver() {
      SettableFuture<Long> committedFuture = SettableFuture.create();

      @Override
      public long getCommittedSize() {
        return activeBlobWrites.get(blobDigest).size();
      }

      @Override
      public ListenableFuture<Long> getCommittedFuture() {
        return committedFuture;
      }

      @Override
      public void reset() {
        activeBlobWrites.put(blobDigest, ByteString.EMPTY);
      }

      @Override
      public void onNext(ByteString chunk) {
        activeBlobWrites.put(blobDigest, activeBlobWrites.get(blobDigest).concat(chunk));
      }

      @Override
      public void onError(Throwable t) {
        activeBlobWrites.remove(blobDigest);
        committedFuture.setException(t);
      }

      @Override
      public void onCompleted() {
        ByteString content = activeBlobWrites.get(blobDigest);
        // yup, redundant, need to compute this inline
        Preconditions.checkState(digestUtil.compute(content).equals(blobDigest));
        try {
          if (content.size() != 0) {
            Blob blob = new Blob(content, digestUtil);
            contentAddressableStorage.put(blob);
          }
          committedFuture.set((long) content.size());
        } catch (Throwable t) {
          committedFuture.setException(t);
        }
        activeBlobWrites.remove(blobDigest);
      }
    };
  }

  @Override
  public ChunkObserver getWriteOperationStreamObserver(String operationStream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> digests, ExecutorService service) {
    ListeningExecutorService listeningService = listeningDecorator(service);
    ImmutableList.Builder<Digest> builder = new ImmutableList.Builder<>();
    ListenableFuture<List<Void>> missingOrNullBlobs = allAsList(
        Iterables.transform(digests, (digest) -> listeningService.<Void>submit(() -> {
          if (!contentAddressableStorage.contains(digest)) {
            builder.add(digest);
          }
          return null;
        })));
    return transform(missingOrNullBlobs, (results) -> builder.build());
  }

  protected abstract int getTreeDefaultPageSize();
  protected abstract int getTreeMaxPageSize();
  protected abstract TokenizableIterator<DirectoryEntry> createTreeIterator(
      Digest rootDigest, String pageToken) throws IOException, InterruptedException;

  @Override
  public String getTree(
      Digest rootDigest, int pageSize, String pageToken,
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) throws IOException, InterruptedException {
    if (pageSize == 0) {
      pageSize = getTreeDefaultPageSize();
    }
    if (pageSize >= 0 && pageSize > getTreeMaxPageSize()) {
      pageSize = getTreeMaxPageSize();
    }

    TokenizableIterator<DirectoryEntry> iter = createTreeIterator(rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next().getDirectory();
      // If part of the tree is missing from the CAS, the server will return the
      // portion present and omit the rest.
      if (directory != null) {
        directories.add(directory);
        if (pageSize > 0) {
          pageSize--;
        }
      } else if (!acceptMissing) {
        // would be nice to have the digests...
        throw new IOException("directory is missing");
      }
    }
    return iter.toNextPageToken();
  }

  protected String createOperationName(String id) {
    return getName() + "/operations/" + id;
  }

  protected abstract Operation createOperation(ActionKey actionKey);

  // called when an operation will be queued for execution
  protected void onQueue(Operation operation, Action action) throws IOException, InterruptedException, StatusException {
  }

  private void stringsUniqueAndSortedPrecondition(
      Iterable<String> strings,
      String duplicateViolationMessage,
      String unsortedViolationMessage,
      PreconditionFailure.Builder preconditionFailure) {
    String lastString = "";
    for (String string : strings) {
      int direction = lastString.compareTo(string);
      if (direction == 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(duplicateViolationMessage)
            .setDescription(string);
      }
      if (direction < 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(unsortedViolationMessage)
            .setDescription(lastString + " > " + string);
      }
    }
  }

  private void filesUniqueAndSortedPrecondition(
      Iterable<String> files, PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        files,
        DUPLICATE_FILE_NODE,
        DIRECTORY_NOT_SORTED,
        preconditionFailure);
  }

  private void environmentVariablesUniqueAndSortedPrecondition(
      Iterable<Command.EnvironmentVariable> environmentVariables,
      PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        Iterables.transform(
            environmentVariables,
            environmentVariable -> environmentVariable.getName()),
        DUPLICATE_ENVIRONMENT_VARIABLE,
        ENVIRONMENT_VARIABLES_NOT_SORTED,
        preconditionFailure);
  }

  @VisibleForTesting
  public void validateActionInputDirectory(
      Directory directory,
      Stack<Digest> path,
      Set<Digest> visited,
      Map<Digest, Directory> directoriesIndex,
      ImmutableSet.Builder<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure) {
    Set<String> entryNames = new HashSet<>();

    // should construct a directory path string for the description of the failures

    String lastFileName = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String fileName = fileNode.getName();
      if (entryNames.contains(fileName)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DUPLICATE_FILE_NODE)
            .setDescription(fileName);
      } else if (lastFileName.compareTo(fileName) > 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_NOT_SORTED)
            .setDescription(lastFileName + " > " + fileName);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          fileName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      lastFileName = fileName;
      entryNames.add(fileName);

      inputDigests.add(fileNode.getDigest());
    }
    String lastDirectoryName = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      String directoryName = directoryNode.getName();

      if (entryNames.contains(directoryName)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DUPLICATE_FILE_NODE)
            .setDescription(directoryName);
      } else if (lastFileName.compareTo(directoryName) > 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_NOT_SORTED)
            .setDescription(lastDirectoryName + " > " + directoryName);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          directoryName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      lastDirectoryName = directoryName;
      entryNames.add(directoryName);

      if (path.contains(directoryNode.getDigest())) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_CYCLE_DETECTED)
            .setDescription(DigestUtil.toString(directoryNode.getDigest()));
      } else {
        Digest directoryDigest = directoryNode.getDigest();
        if (!visited.contains(directoryDigest)) {
          validateActionInputDirectoryDigest(directoryDigest, path, visited, directoriesIndex, inputDigests, preconditionFailure);
        }
      }
    }
  }

  private void validateActionInputDirectoryDigest(
      Digest directoryDigest,
      Stack<Digest> path,
      Set<Digest> visited,
      Map<Digest, Directory> directoriesIndex,
      ImmutableSet.Builder<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure) {
    path.push(directoryDigest);

    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory == null) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(MISSING_INPUT)
          .setDescription("Directory " + DigestUtil.toString(directoryDigest));
      validateActionInputDirectory(directory, path, visited, directoriesIndex, inputDigests, preconditionFailure);
    }
    path.pop();
    visited.add(directoryDigest);
  }

  protected ListenableFuture<Iterable<Directory>> getTreeDirectories(Digest inputRoot, ListeningExecutorService service) {
    return service.submit(() -> {
      ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();

      TokenizableIterator<DirectoryEntry> iterator = createTreeIterator(inputRoot, /* pageToken=*/ "");
      while (iterator.hasNext()) {
        DirectoryEntry entry = iterator.next();
        Directory directory = entry.getDirectory();
        Preconditions.checkState(directory != null, MISSING_INPUT + " Directory [" + DigestUtil.toString(entry.getDigest()) + "]");
        directories.add(directory);
      }

      return directories.build();
    });
  }

  protected Map<Digest, Directory> createDirectoriesIndex(Iterable<Directory> directories) {
    Set<Digest> directoryDigests = Sets.newHashSet();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = ImmutableMap.builder();
    for (Directory directory : directories) {
      // double compute here...
      Digest directoryDigest = digestUtil.compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return directoriesIndex.build();
  }

  private ListenableFuture<Void> validateInputs(
      Iterable<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure,
      ExecutorService service) {
    return transform(
        findMissingBlobs(inputDigests, service),
        (missingBlobDigests) -> {
          preconditionFailure.addAllViolations(
              Iterables.transform(
                  missingBlobDigests,
                  (digest) -> Violation.newBuilder()
                      .setType(VIOLATION_TYPE_MISSING)
                      .setSubject(MISSING_INPUT)
                      .setDescription(DigestUtil.toString(digest))
                      .build()));
          return null;
        },
        service);
  }

  public static <V> V getUnchecked(ListenableFuture<V> future) throws InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      return null;
    }
  }

  private static void checkViolations(Supplier<Digest> actionDigestSupplier, List<Violation> violations) {
    Preconditions.checkState(violations.isEmpty(), "Action " + DigestUtil.toString(actionDigestSupplier.get()) + " is invalid");
  }

  private void checkViolations(Action action, List<Violation> violations) {
    checkViolations(() -> digestUtil.compute(action), violations);
  }

  protected void validateAction(Action action, PreconditionFailure.Builder preconditionFailure) throws InterruptedException {
    ImmutableSet.Builder<Digest> inputDigestsBuilder = ImmutableSet.builder();
    validateAction(
        action,
        getUnchecked(expectCommand(action.getCommandDigest())),
        getUnchecked(getTreeDirectories(action.getInputRootDigest(), newDirectExecutorService())),
        inputDigestsBuilder,
        preconditionFailure);
    try {
      validateInputs(inputDigestsBuilder.build(), preconditionFailure, newDirectExecutorService()).get();
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e.getCause());
    }
    checkViolations(action, preconditionFailure.getViolationsList());
  }

  protected ListenableFuture<QueuedOperationMetadata> validateQueuedOperationMetadataAndInputs(
      QueuedOperationMetadata metadata, PreconditionFailure.Builder preconditionFailure, ExecutorService service) throws InterruptedException {
    ImmutableSet.Builder<Digest> inputDigestsBuilder = ImmutableSet.builder();
    Action action = metadata.getAction();
    validateAction(
        action,
        metadata.getCommand(),
        metadata.getDirectoriesList(),
        inputDigestsBuilder,
        preconditionFailure);
    return transform(
        validateInputs(inputDigestsBuilder.build(), preconditionFailure, service),
        (result) -> {
          checkViolations(action, preconditionFailure.getViolationsList());
          return metadata;
        },
        service);
  }

  protected void validateQueuedOperationMetadata(
      QueuedOperationMetadata metadata, PreconditionFailure.Builder preconditionFailure) {
    Action action = metadata.getAction();
    validateAction(
        action,
        metadata.getCommand(),
        metadata.getDirectoriesList(),
        ImmutableSet.builder(),
        preconditionFailure);
    checkViolations(action, preconditionFailure.getViolationsList());
  }

  private void validateAction(
      Action action,
      Command command,
      Iterable<Directory> directories,
      ImmutableSet.Builder<Digest> inputDigestsBuilder,
      PreconditionFailure.Builder preconditionFailure) {
    Digest actionDigest = digestUtil.compute(action);
    Digest commandDigest = action.getCommandDigest();
    inputDigestsBuilder.add(commandDigest);

    Map<Digest, Directory> directoriesIndex = createDirectoriesIndex(directories);

    // needs futuring
    validateActionInputDirectoryDigest(
        action.getInputRootDigest(),
        new Stack<>(),
        new HashSet<>(),
        directoriesIndex,
        inputDigestsBuilder,
        preconditionFailure);

    if (command == null) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(MISSING_INPUT)
          .setDescription("Command " + DigestUtil.toString(commandDigest));
    }

    // FIXME should input/output collisions (through directories) be another
    // invalid action?
    filesUniqueAndSortedPrecondition(
        action.getOutputFilesList(), preconditionFailure);
    filesUniqueAndSortedPrecondition(
        action.getOutputDirectoriesList(), preconditionFailure);
    environmentVariablesUniqueAndSortedPrecondition(
        command.getEnvironmentVariablesList(), preconditionFailure);
    if (command.getArgumentsList().isEmpty()) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_COMMAND)
          .setDescription("argument list is empty");
    }
    if (!preconditionFailure.getViolationsList().isEmpty()) {
      throw new IllegalStateException("Action " + DigestUtil.toString(actionDigest) + " was invalid");
    }
  }

  @Override
  public ListenableFuture<Operation> execute(
      Action action,
      boolean skipCacheLookup,
      RequestMetadata metadata) {
    SettableFuture<Operation> executeFuture = SettableFuture.create();
    try {
      execute(action, skipCacheLookup, metadata, executeFuture::set);
    } catch (IllegalStateException e) {
      executeFuture.setException(e);
    } catch (InterruptedException e) {
      executeFuture.setException(e);
    }
    return executeFuture;
  }

  private void execute(
      Action action,
      boolean skipCacheLookup,
      RequestMetadata requestMetadata,
      Consumer<Operation> onOperation) throws InterruptedException {
    ActionKey actionKey = digestUtil.computeActionKey(action);
    Operation operation = createOperation(actionKey);

    System.out.println(System.nanoTime() + ": Operation " + operation.getName() + " was created");

    putOperation(operation);

    onOperation.accept(operation);

    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    try {
      validateAction(action, preconditionFailure);
    } catch (IllegalStateException e) {
      errorOperation(operation.getName(), com.google.rpc.Status.newBuilder()
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(e.getMessage())
          .addDetails(Any.pack(preconditionFailure.build()))
          .build());
      return;
    }

    ExecuteOperationMetadata metadata =
      expectExecuteOperationMetadata(operation);

    Operation.Builder operationBuilder = operation.toBuilder();
    ActionResult actionResult = null;
    if (!skipCacheLookup) {
      metadata = metadata.toBuilder()
          .setStage(Stage.CACHE_CHECK)
          .build();
      putOperation(operationBuilder
          .setMetadata(Any.pack(metadata))
          .build());
      actionResult = getActionResult(actionKey);
    }

    if (actionResult != null) {
      metadata = metadata.toBuilder()
          .setStage(Stage.COMPLETED)
          .build();
      operationBuilder
          .setDone(true)
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(actionResult)
              .setStatus(com.google.rpc.Status.newBuilder()
                  .setCode(Code.OK.value())
                  .build())
              .setCachedResult(true)
              .build()));
    } else {
      try {
        onQueue(operation, action);
      } catch (IOException|StatusException e) {
        deleteOperation(operation.getName());
        throw Status.fromThrowable(e).asRuntimeException();
      } catch (InterruptedException e) {
        deleteOperation(operation.getName());
        Thread.currentThread().interrupt();
        throw Status.fromThrowable(e).asRuntimeException();
      }
      metadata = metadata.toBuilder()
          .setStage(Stage.QUEUED)
          .build();
    }

    operation = operationBuilder
        .setMetadata(Any.pack(metadata))
        .build();
    /* TODO record file count/size for matching purposes? */

    if (!operation.getDone()) {
      updateOperationWatchers(operation); // updates watchers initially for queued stage
    }
    putOperation(operation);
  }

  protected static ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        return null;
      }
    }
    if (operation.getMetadata().is(ExecutingOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(ExecutingOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        return null;
      }
    }
    if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(CompletedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        return null;
      }
    }
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch(InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  protected Action expectAction(Operation operation) throws InterruptedException {
    try {
      ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
      if (metadata == null) {
        return null;
      }
      ByteString actionBlob = getBlob(metadata.getActionDigest());
      if (actionBlob != null) {
        return Action.parseFrom(actionBlob);
      }
    } catch(IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.NOT_FOUND) {
        e.printStackTrace();
      }
    }
    return null;
  }

  private <T> ListenableFuture<T> parseFuture(Digest digest, Parser<T> parser) {
    SettableFuture<T> future = SettableFuture.create();
    Futures.addCallback(getBlobFuture(digest), new FutureCallback<ByteString>() {
      @Override
      public void onSuccess(ByteString blob) {
        try {
          future.set(parser.parseFrom(blob));
        } catch (InvalidProtocolBufferException e) {
          future.setException(e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        future.setException(t);
      }
    });
    return future;
  }

  protected ListenableFuture<Directory> expectDirectory(Digest directoryBlobDigest) {
    return parseFuture(directoryBlobDigest, Directory.parser());
  }

  protected ListenableFuture<Command> expectCommand(Digest commandBlobDigest) {
    return parseFuture(commandBlobDigest, Command.parser());
  }

  protected ListenableFuture<Command> insistCommand(Digest commandBlobDigest) {
    return transform(
        expectCommand(commandBlobDigest),
        (command) -> {
          if (command != null) {
            return command;
          }
          throw new IllegalStateException(MISSING_INPUT + " Command [" + DigestUtil.toString(commandBlobDigest) + "]");
        });
  }

  protected static boolean isCancelled(Operation operation) {
    return operation.getDone() &&
        operation.getResultCase() == Operation.ResultCase.ERROR &&
        operation.getError().getCode() == Code.CANCELLED.value();
  }

  protected static boolean isErrored(Operation operation) {
    return operation.getDone() &&
        operation.getResultCase() == Operation.ResultCase.ERROR;
  }

  private static boolean isStage(Operation operation, Stage stage) {
    ExecuteOperationMetadata metadata
        = expectExecuteOperationMetadata(operation);
    return metadata != null && metadata.getStage() == stage;
  }

  protected static boolean isUnknown(Operation operation) {
    return isStage(operation, Stage.UNKNOWN);
  }

  protected static boolean isQueued(Operation operation) {
    return isStage(operation, Stage.QUEUED);
  }

  protected static boolean isExecuting(Operation operation) {
    return isStage(operation, Stage.EXECUTING);
  }

  protected static boolean isComplete(Operation operation) {
    return isStage(operation, Stage.COMPLETED);
  }

  protected abstract boolean matchOperation(Operation operation) throws InterruptedException;
  protected abstract void enqueueOperation(Operation operation);

  @Override
  public boolean putOperation(Operation operation) throws InterruptedException {
    if (isCancelled(operation)) {
      if (outstandingOperations.remove(operation.getName()) == null) {
        throw new IllegalStateException();
      }
      updateOperationWatchers(operation);
      return true;
    }
    if (isExecuting(operation) &&
        !outstandingOperations.containsKey(operation.getName())) {
      return false;
    }
    if (isQueued(operation)) {
      if (!matchOperation(operation)) {
        enqueueOperation(operation);
      }
    } else {
      updateOperationWatchers(operation);
    }
    return true;
  }

  /**
   * per-operation lock factory/indexer method
   *
   * the lock retrieved for an operation will guard against races
   * during transfers/retrievals/removals
   */
  protected abstract Object operationLock(String operationName);

  protected void updateOperationWatchers(Operation operation) {
    if (operation.getDone()) {
      synchronized (operationLock(operation.getName())) {
        completedOperations.put(operation.getName(), operation);
        outstandingOperations.remove(operation.getName());
      }
    } else {
      outstandingOperations.put(operation.getName(), operation);
    }
  }

  @Override
  public Operation getOperation(String name) {
    synchronized (operationLock(name)) {
      Operation operation = completedOperations.get(name);
      if (operation == null) {
        operation = outstandingOperations.get(name);
      }
      return operation;
    }
  }

  protected abstract int getListOperationsDefaultPageSize();
  protected abstract int getListOperationsMaxPageSize();
  protected abstract TokenizableIterator<Operation> createOperationsIterator(String pageToken);

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter,
      ImmutableList.Builder<Operation> operations) {
    if (pageSize == 0) {
      pageSize = getListOperationsDefaultPageSize();
    } else if (getListOperationsMaxPageSize() > 0 &&
        pageSize > getListOperationsMaxPageSize()) {
      pageSize = getListOperationsMaxPageSize();
    }

    // FIXME filter?
    TokenizableIterator<Operation> iter = createOperationsIterator(pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Operation operation = iter.next();
      operations.add(operation);
      if (pageSize > 0) {
        pageSize--;
      }
    }
    return iter.toNextPageToken();
  }

  @Override
  public void deleteOperation(String name) {
    synchronized (operationLock(name)) {
      Operation deletedOperation = completedOperations.remove(name);
      if (deletedOperation == null &&
          outstandingOperations.containsKey(name)) {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public void cancelOperation(String name) throws InterruptedException {
    errorOperation(name, com.google.rpc.Status.newBuilder()
        .setCode(com.google.rpc.Code.CANCELLED.getNumber())
        .build());
  }

  protected void errorOperation(String name, com.google.rpc.Status status) throws InterruptedException {
    Operation operation = getOperation(name);
    if (operation == null) {
      // throw new IllegalStateException("Trying to error nonexistent operation [" + name + "]");
      System.err.println("Erroring non-existent operation " + name + ", will signal watchers");
      operation = Operation.newBuilder()
          .setName(name)
          .build();
    }
    if (operation.getDone()) {
      throw new IllegalStateException("Trying to error already completed operation [" + name + "]");
    }
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      metadata = ExecuteOperationMetadata.getDefaultInstance();
    }
    putOperation(operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata.toBuilder()
            .setStage(Stage.COMPLETED)
            .build()))
        .setError(status)
        .build());
  }

  protected void expireOperation(Operation operation) throws InterruptedException {
    ActionResult actionResult = ActionResult.newBuilder()
        .setExitCode(-1)
        .setStderrRaw(ByteString.copyFromUtf8(
            "[BUILDFARM]: Action timed out with no response from worker"))
        .build();
    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setResult(actionResult)
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(Code.DEADLINE_EXCEEDED.value())
            .build())
        .build();
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      throw new IllegalStateException("Operation " + operation.getName() + " did not contain valid metadata");
    }
    metadata = metadata.toBuilder()
        .setStage(Stage.COMPLETED)
        .build();
    putOperation(operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata))
        .setResponse(Any.pack(executeResponse))
        .build());
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage) {
    if (stage != Stage.QUEUED
        && stage != Stage.EXECUTING) {
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
    if (metadata.getStage() != stage) {
      return false;
    }
    return true;
  }
}
