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

import build.buildfarm.ac.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ActionCacheUpdateCapabilities;
import build.bazel.remote.execution.v2.CacheCapabilities;
import build.bazel.remote.execution.v2.CacheCapabilities.SymlinkAbsolutePathStrategy;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionCapabilities;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Predicate;

public abstract class AbstractServerInstance implements Instance {
  private final String name;
  protected final ContentAddressableStorage contentAddressableStorage;
  protected final ActionCache actionCache;
  protected final OperationsMap outstandingOperations;
  protected final OperationsMap completedOperations;
  protected final DigestUtil digestUtil;

  private static final String DUPLICATE_FILE_NODE =
      "One of the input `Directory` has multiple entries with the same file name. This will also"
          + " occur if the worker filesystem considers two names to be the same, such as two names"
          + " that vary only by case on a case-insensitive filesystem, or two names with the same"
          + " normalized form on a filesystem that performs Unicode normalization on filenames.";

  private static final String DIRECTORY_NOT_SORTED =
      "The files in an input `Directory` are not correctly sorted by `name`.";

  private static final String DIRECTORY_CYCLE_DETECTED =
      "The input file tree contains a cycle (a `Directory` which, directly or indirectly,"
          + " contains itself).";

  private static final String DUPLICATE_ENVIRONMENT_VARIABLE =
      "The `Command`'s `environment_variables` contain a duplicate entry. On systems where"
          + " environment variables may consider two different names to be the same, such as if"
          + " environment variables are case-insensitive, this may also occur if two equivalent"
          + " environment variables appear.";

  private static final String ENVIRONMENT_VARIABLES_NOT_SORTED =
      "The `Command`'s `environment_variables` are not correctly sorted by `name`.";

  private static final String MISSING_INPUT =
      "A requested input (or the `Command` of the `Action`) was not found in the CAS.";

  private static final String INVALID_DIGEST = "A `Digest` in the input tree is invalid.";

  private static final String INVALID_FILE_NAME =
      "One of the input `PathNode`s has an invalid name, such as a name containing a `/` character"
          + " or another character which cannot be used in a file's name on the filesystem of the"
          + " worker.";

  public AbstractServerInstance(
      String name,
      DigestUtil digestUtil,
      ContentAddressableStorage contentAddressableStorage,
      ActionCache actionCache,
      OperationsMap outstandingOperations,
      OperationsMap completedOperations) {
    this.name = name;
    this.contentAddressableStorage = contentAddressableStorage;
    this.actionCache = actionCache;
    this.outstandingOperations = outstandingOperations;
    this.completedOperations = completedOperations;
    this.digestUtil = digestUtil;
  }

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
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException {
    actionCache.put(actionKey, actionResult);
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return String.format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  @Override
  public final ByteString getBlob(Digest blobDigest) {
    return getBlob(blobDigest, 0, 0);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit)
      throws IndexOutOfBoundsException {
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

  @Override
  public Digest putBlob(ByteString content)
      throws InterruptedException {
    if (content.size() == 0) {
      return digestUtil.empty();
    }
    Blob blob = new Blob(content, digestUtil);
    contentAddressableStorage.put(blob);
    return blob.getDigest();
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws InterruptedException {
    ImmutableList.Builder<Digest> blobDigestsBuilder =
      new ImmutableList.Builder<Digest>();
    for (ByteString blob : blobs) {
      blobDigestsBuilder.add(putBlob(blob));
    }
    return blobDigestsBuilder.build();
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    ImmutableList.Builder<Digest> missingBlobs = new ImmutableList.Builder<>();
    for (Digest digest : digests) {
      if (digest.getSizeBytes() == 0 || contentAddressableStorage.contains(digest)) {
        continue;
      }
      missingBlobs.add(digest);
    }
    return missingBlobs.build();
  }

  protected abstract int getTreeDefaultPageSize();
  protected abstract int getTreeMaxPageSize();
  protected abstract TokenizableIterator<Directory> createTreeIterator(
      Digest rootDigest, String pageToken);

  @Override
  public String getTree(
      Digest rootDigest, int pageSize, String pageToken,
      ImmutableList.Builder<Directory> directories) {
    if (pageSize == 0) {
      pageSize = getTreeDefaultPageSize();
    }
    if (pageSize >= 0 && pageSize > getTreeMaxPageSize()) {
      pageSize = getTreeMaxPageSize();
    }

    TokenizableIterator<Directory> iter =
      createTreeIterator(rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next();
      // If part of the tree is missing from the CAS, the server will return the
      // portion present and omit the rest.
      if (directory != null) {
        directories.add(directory);
        if (pageSize > 0) {
          pageSize--;
        }
      }
    }
    return iter.toNextPageToken();
  }

  protected String createOperationName(String id) {
    return getName() + "/operations/" + id;
  }

  abstract protected Operation createOperation(ActionKey actionKey);

  // called when an operation will be queued for execution
  protected void onQueue(Operation operation, Action action) throws InterruptedException {
  }

  private void stringsUniqueAndSortedPrecondition(
      Iterable<String> strings,
      String duplicateViolationMessage,
      String unsortedViolationMessage) {
    String lastString = "";
    for (String string : strings) {
      int direction = lastString.compareTo(string);
      Preconditions.checkState(direction != 0, duplicateViolationMessage);
      Preconditions.checkState(direction < 0, string + " >= " + lastString, unsortedViolationMessage);
    }
  }

  private void filesUniqueAndSortedPrecondition(Iterable<String> files) {
    stringsUniqueAndSortedPrecondition(
        files,
        DUPLICATE_FILE_NODE,
        DIRECTORY_NOT_SORTED);
  }

  private void environmentVariablesUniqueAndSortedPrecondition(
      Iterable<Command.EnvironmentVariable> environmentVariables) {
    stringsUniqueAndSortedPrecondition(
        Iterables.transform(
            environmentVariables,
            environmentVariable -> environmentVariable.getName()),
        DUPLICATE_ENVIRONMENT_VARIABLE,
        ENVIRONMENT_VARIABLES_NOT_SORTED);
  }

  private void enumerateActionInputDirectory(
      String directoryPath,
      Directory directory,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories) {
    Preconditions.checkState(
        directory != null,
        MISSING_INPUT);

    for (FileNode fileNode : directory.getFilesList()) {
      String fileName = fileNode.getName();
      String filePath = directoryPath.isEmpty() ? fileName : (directoryPath + "/" + fileName);
      inputFiles.add(filePath);
    }
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      String directoryName = directoryNode.getName();

      Digest directoryDigest = directoryNode.getDigest();
      String subDirectoryPath = directoryPath.isEmpty()
          ? directoryName
          : (directoryPath + "/" + directoryName);
      inputDirectories.add(subDirectoryPath);
      enumerateActionInputDirectory(
          subDirectoryPath,
          expectDirectory(directoryDigest),
          inputFiles,
          inputDirectories);
    }
  }

  private void validateActionInputDirectory(
      String directoryPath,
      Directory directory,
      Stack<Digest> pathDigests,
      Set<Digest> visited,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories,
      ImmutableSet.Builder<Digest> inputDigests) {
    Preconditions.checkState(
        directory != null,
        MISSING_INPUT);

    Set<String> entryNames = new HashSet<>();

    String lastFileName = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String fileName = fileNode.getName();
      Preconditions.checkState(
          !entryNames.contains(fileName),
          DUPLICATE_FILE_NODE);
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          fileName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      Preconditions.checkState(
          lastFileName.compareTo(fileName) < 0,
          DIRECTORY_NOT_SORTED);
      lastFileName = fileName;
      entryNames.add(fileName);

      inputDigests.add(fileNode.getDigest());
      String filePath = directoryPath.isEmpty() ? fileName : (directoryPath + "/" + fileName);
      inputFiles.add(filePath);
    }
    String lastDirectoryName = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      String directoryName = directoryNode.getName();

      Preconditions.checkState(
          !entryNames.contains(directoryName),
          DUPLICATE_FILE_NODE);
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          directoryName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      Preconditions.checkState(
          lastDirectoryName.compareTo(directoryName) < 0,
          DIRECTORY_NOT_SORTED);
      lastDirectoryName = directoryName;
      entryNames.add(directoryName);

      Preconditions.checkState(
          !pathDigests.contains(directoryNode.getDigest()),
          DIRECTORY_CYCLE_DETECTED);

      Digest directoryDigest = directoryNode.getDigest();
      String subDirectoryPath = directoryPath.isEmpty()
          ? directoryName
          : (directoryPath + "/" + directoryName);
      inputDirectories.add(subDirectoryPath);
      if (!visited.contains(directoryDigest)) {
        validateActionInputDirectoryDigest(
            subDirectoryPath,
            directoryDigest,
            pathDigests,
            visited,
            inputFiles,
            inputDirectories,
            inputDigests);
      } else {
        enumerateActionInputDirectory(
            subDirectoryPath,
            expectDirectory(directoryDigest),
            inputFiles,
            inputDirectories);
      }
    }
  }

  private void validateActionInputDirectoryDigest(
      String directoryPath,
      Digest directoryDigest,
      Stack<Digest> pathDigests,
      Set<Digest> visited,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories,
      ImmutableSet.Builder<Digest> inputDigests) {
    pathDigests.push(directoryDigest);
    validateActionInputDirectory(
        directoryPath,
        expectDirectory(directoryDigest),
        pathDigests,
        visited,
        inputFiles,
        inputDirectories,
        inputDigests);
    pathDigests.pop();
    visited.add(directoryDigest);
  }

  protected void validateAction(Action action) {
    Digest commandDigest = action.getCommandDigest();
    ImmutableSet.Builder<String> inputDirectoriesBuilder = new ImmutableSet.Builder<>();
    ImmutableSet.Builder<String> inputFilesBuilder = new ImmutableSet.Builder<>();
    ImmutableSet.Builder<Digest> inputDigests = new ImmutableSet.Builder<>();
    inputDigests.add(commandDigest);

    inputDirectoriesBuilder.add("");
    validateActionInputDirectoryDigest(
        "",
        action.getInputRootDigest(),
        new Stack<>(),
        new HashSet<>(),
        inputFilesBuilder,
        inputDirectoriesBuilder,
        inputDigests);

    // A requested input (or the [Command][] of the [Action][]) was not found in
    // the [ContentAddressableStorage][].
    Iterable<Digest> missingBlobDigests = findMissingBlobs(inputDigests.build());
    if (!Iterables.isEmpty(missingBlobDigests)) {
      Preconditions.checkState(
          Iterables.isEmpty(missingBlobDigests),
          MISSING_INPUT);
    }

    Command command;
    try {
      command = Command.parseFrom(getBlob(commandDigest));
    } catch (InvalidProtocolBufferException ex) {
      Preconditions.checkState(
          false,
          INVALID_DIGEST);
      return;
    }

    // FIXME should input/output collisions (through directories) be another
    // invalid action?
    filesUniqueAndSortedPrecondition(command.getOutputFilesList());
    filesUniqueAndSortedPrecondition(command.getOutputDirectoriesList());

    AbstractServerInstance.validateOutputs(
        inputFilesBuilder.build(),
        inputDirectoriesBuilder.build(),
        Sets.newHashSet(command.getOutputFilesList()),
        Sets.newHashSet(command.getOutputDirectoriesList()));

    environmentVariablesUniqueAndSortedPrecondition(
        command.getEnvironmentVariablesList());
    Preconditions.checkState(
        !command.getArgumentsList().isEmpty(),
        INVALID_DIGEST);
  }

  @VisibleForTesting
  public static void validateOutputs(
      Set<String> inputFiles,
      Set<String> inputDirectories,
      Set<String> outputFiles,
      Set<String> outputDirectories) {
    Preconditions.checkState(
        Sets.intersection(outputFiles, outputDirectories).isEmpty(),
        "an output file has the same path as an output directory");

    Set<String> parentsOfOutputs = new HashSet<>();

    // An output file cannot be a parent of another output file, be a child of a listed output directory, or have the same path as any of the listed output directories.
    for (String outputFile : outputFiles) {
      Preconditions.checkState(
        !inputDirectories.contains(outputFile),
        "output file is input directory");
      String currentPath = outputFile;
      while (currentPath != "") {
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

    // An output directory cannot be a parent of another output directory, be a parent of a listed output file, or have the same path as any of the listed output files.
    for (String outputDir : outputDirectories) {
      Preconditions.checkState(
        !inputFiles.contains(outputDir),
        "output directory is input file");
      String currentPath = outputDir;
      while (currentPath != "") {
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
    Preconditions.checkState(Sets.intersection(outputFiles, parentsOfOutputs).isEmpty(), "an output file cannot be a parent of another output");
    Preconditions.checkState(Sets.intersection(outputDirectories, parentsOfOutputs).isEmpty(), "an output directory cannot be a parent of another output");
  }

  @Override
  public void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      Predicate<Operation> watcher) throws InterruptedException {
    ByteString actionBlob = getBlob(actionDigest);
    Preconditions.checkState(actionBlob != null, INVALID_DIGEST);

    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(INVALID_DIGEST);
    }

    validateAction(action);

    ActionKey actionKey = DigestUtil.asActionKey(actionDigest);
    Operation operation = createOperation(actionKey);
    ExecuteOperationMetadata metadata =
      expectExecuteOperationMetadata(operation);

    putOperation(operation);

    watchOperation(operation.getName(), watcher);

    Operation.Builder operationBuilder = operation.toBuilder();
    ActionResult actionResult = null;
    if (!skipCacheLookup) {
      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.CACHE_CHECK)
          .build();
      putOperation(operationBuilder
          .setMetadata(Any.pack(metadata))
          .build());
      actionResult = getActionResult(actionKey);
    }

    if (actionResult != null) {
      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
          .build();
      operationBuilder
          .setDone(true)
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(actionResult)
              .setStatus(com.google.rpc.Status.newBuilder()
                  .setCode(Code.OK.getNumber())
                  .build())
              .setCachedResult(actionResult != null)
              .build()));
    } else {
      onQueue(operation, action);
      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.QUEUED)
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

  protected ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    Preconditions.checkState(
        operation.getMetadata().is(ExecuteOperationMetadata.class));
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch(InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Action expectAction(Operation operation) {
    try {
      return Action.parseFrom(getBlob(
          expectExecuteOperationMetadata(operation).getActionDigest()));
    } catch(InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Command expectCommand(Operation operation) {
    Action action = expectAction(operation);
    if (action == null) {
      return null;
    }
    try {
      return Command.parseFrom(getBlob(action.getCommandDigest()));
    } catch(InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Directory expectDirectory(Digest directoryBlobDigest) {
    try {
      ByteString directoryBlob = getBlob(directoryBlobDigest);
      if (directoryBlob != null) {
        return Directory.parseFrom(directoryBlob);
      }
    } catch(InvalidProtocolBufferException ex) {
    }
    return null;
  }

  protected boolean isCancelled(Operation operation) {
    return operation.getDone() &&
        operation.getResultCase() == Operation.ResultCase.ERROR &&
        operation.getError().getCode() == Status.Code.CANCELLED.value();
  }

  protected boolean isQueued(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        ExecuteOperationMetadata.Stage.QUEUED;
  }

  protected boolean isExecuting(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        ExecuteOperationMetadata.Stage.EXECUTING;
  }

  protected boolean isComplete(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        ExecuteOperationMetadata.Stage.COMPLETED;
  }

  abstract protected boolean matchOperation(Operation operation);
  abstract protected void enqueueOperation(Operation operation);

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
        !outstandingOperations.contains(operation.getName())) {
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

  protected void updateOperationWatchers(Operation operation) throws InterruptedException {
    if (operation.getDone()) {
      synchronized(operationLock(operation.getName())) {
        completedOperations.put(operation.getName(), operation);
        outstandingOperations.remove(operation.getName());
      }
    } else {
      outstandingOperations.put(operation.getName(), operation);
    }
  }

  @Override
  public Operation getOperation(String name) {
    synchronized(operationLock(name)) {
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
    synchronized(operationLock(name)) {
      Operation deletedOperation = completedOperations.remove(name);
      if (deletedOperation == null &&
          outstandingOperations.contains(name)) {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public void cancelOperation(String name) throws InterruptedException {
    Operation operation = getOperation(name);
    putOperation(operation.toBuilder()
        .setDone(true)
        .setError(com.google.rpc.Status.newBuilder()
            .setCode(Code.CANCELLED.getNumber())
            .build())
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
            .setCode(Code.DEADLINE_EXCEEDED.getNumber())
            .build())
        .build();
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      throw new IllegalStateException("Operation " + operation.getName() + " did not contain valid metadata");
    }
    putOperation(operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata))
        .setResponse(Any.pack(executeResponse))
        .build());
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecuteOperationMetadata.Stage stage) {
    if (stage != ExecuteOperationMetadata.Stage.QUEUED
        && stage != ExecuteOperationMetadata.Stage.EXECUTING) {
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

  protected CacheCapabilities getCacheCapabilities() {
    return CacheCapabilities.newBuilder()
        .addDigestFunction(digestUtil.getDigestFunction())
        .setActionCacheUpdateCapabilities(ActionCacheUpdateCapabilities.newBuilder()
            .setUpdateEnabled(true))
        .setMaxBatchTotalSizeBytes(4 * 1024 * 1024)
        .setSymlinkAbsolutePathStrategy(SymlinkAbsolutePathStrategy.DISALLOWED)
        .build();
  }

  protected ExecutionCapabilities getExecutionCapabilities() {
    return ExecutionCapabilities.newBuilder()
        .setDigestFunction(digestUtil.getDigestFunction())
        .setExecEnabled(true)
        .build();
  }

  @Override
  public ServerCapabilities getCapabilities() {
    return ServerCapabilities.newBuilder()
        .setCacheCapabilities(getCacheCapabilities())
        .setExecutionCapabilities(getExecutionCapabilities())
        .build();
  }
}
