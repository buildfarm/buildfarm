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

import build.buildfarm.common.Digests;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecutePreconditionViolationType;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public abstract class AbstractServerInstance implements Instance {
  private final String name;
  protected final Map<Digest, ByteString> contentAddressableStorage;
  protected final Map<Digest, ActionResult> actionCache;
  protected final Map<String, Operation> outstandingOperations;

  public AbstractServerInstance(
      String name,
      Map<Digest, ByteString> contentAddressableStorage,
      Map<Digest, ActionResult> actionCache,
      Map<String, Operation> outstandingOperations) {
    this.name = name;
    this.contentAddressableStorage = contentAddressableStorage;
    this.actionCache = actionCache;
    this.outstandingOperations = outstandingOperations;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ActionResult getActionResult(Digest actionDigest) {
    return actionCache.get(actionDigest);
  }

  @Override
  public void putActionResult(Digest actionDigest, ActionResult actionResult) {
    actionCache.put(actionDigest, actionResult);
  }

  @Override
  public ByteString getBlob(Digest blobDigest) {
    return getBlob(blobDigest, 0, 0);
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit)
      throws IndexOutOfBoundsException {
    ByteString blob = contentAddressableStorage.get(blobDigest);

    if (blob == null) {
      return blob;
    }

    if (offset >= blob.size() || limit < 0) {
      throw new IndexOutOfBoundsException();
    }

    long endIndex = offset + (limit > 0 ? limit : (blob.size() - offset));

    return blob.substring(
        (int) offset, endIndex > blob.size() ? blob.size() : (int) endIndex);
  }

  @Override
  public Digest putBlob(ByteString blob) throws IllegalArgumentException {
    if (blob.size() == 0) {
      throw new IllegalArgumentException();
    }

    Digest blobDigest = Digests.computeDigest(blob);
    contentAddressableStorage.put(blobDigest, blob);
    return blobDigest;
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

    while( iter.hasNext() && pageSize != 0 ) {
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

  abstract protected Operation createOperation(Action action);

  // called when an operation will be queued for execution
  protected void onQueue(Operation operation, Action action) {
  }

  private void stringsUniqueAndSortedPrecondition(
      Iterable<String> strings,
      ExecutePreconditionViolationType duplicateViolationType,
      ExecutePreconditionViolationType unsortedViolationType) {
    String lastString = "";
    for (String string : strings) {
      int direction = lastString.compareTo(string);
      Preconditions.checkState(direction != 0, duplicateViolationType);
      Preconditions.checkState(direction < 0, string + " >= " + lastString, unsortedViolationType);
    }
  }

  private void filesUniqueAndSortedPrecondition(Iterable<String> files) {
    stringsUniqueAndSortedPrecondition(
        files,
        ExecutePreconditionViolationType.DUPLICATE_FILE_NODE,
        ExecutePreconditionViolationType.DIRECTORY_NOT_SORTED);
  }

  private void environmentVariablesUniqueAndSortedPrecondition(
      Iterable<Command.EnvironmentVariable> environmentVariables) {
    stringsUniqueAndSortedPrecondition(
        Iterables.transform(
            environmentVariables,
            environmentVariable -> environmentVariable.getName()),
        ExecutePreconditionViolationType.DUPLICATE_ENVIRONMENT_VARIABLE,
        ExecutePreconditionViolationType.ENVIRONMENT_VARIABLES_NOT_SORTED);
  }

  private void validateAction(Action action) {
    Digest commandDigest = action.getCommandDigest();
    ImmutableList.Builder<Digest> inputDigests = new ImmutableList.Builder<>();
    inputDigests.add(commandDigest);
    Iterator<Directory> directoriesIterator =
      createTreeIterator(action.getInputRootDigest(), "");

    Set<Digest> directoryDigests = new HashSet<Digest>();

    while (directoriesIterator.hasNext()) {
      Directory directory = directoriesIterator.next();

      // FIXME is this INVALID_DIGEST? we get this
      // for both CAS misses and non-directories
      Preconditions.checkState(
          directory != null,
          ExecutePreconditionViolationType.MISSING_INPUT);

      Digest directoryDigest = Digests.computeDigest(directory);
      Preconditions.checkState(
          !directoryDigests.contains(directoryDigest),
          ExecutePreconditionViolationType.DIRECTORY_CYCLE_DETECTED);
      directoryDigests.add(directoryDigest);

      Iterable<String> fileNames = Iterables.transform(
          directory.getFilesList(),
          fileNode -> fileNode.getName());
      Iterable<String> directoryNames = Iterables.transform(
          directory.getDirectoriesList(),
          directoryNode -> directoryNode.getName());

      String lastFileName = "";
      Set<String> entryNames = new HashSet<String>();
      for (String fileName : fileNames) {
        Preconditions.checkState(
            !entryNames.contains(fileName),
            ExecutePreconditionViolationType.DUPLICATE_FILE_NODE);
        /* FIXME serverside validity check? regex?
        Preconditions.checkState(
            fileName.isValidFilename(),
            ExecutePreconditionViolationType.INVALID_FILE_NAME);
        */
        Preconditions.checkState(
            lastFileName.compareTo(fileName) < 0,
            ExecutePreconditionViolationType.DIRECTORY_NOT_SORTED);
        lastFileName = fileName;
        entryNames.add(fileName);
      }
      String lastDirectoryName = "";
      for (String directoryName : directoryNames) {
        Preconditions.checkState(
            !entryNames.contains(directoryName),
            ExecutePreconditionViolationType.DUPLICATE_FILE_NODE);
        /* FIXME serverside validity check? regex?
        Preconditions.checkState(
            directoryName.isValidFilename(),
            ExecutePreconditionViolationType.INVALID_FILE_NAME);
        */
        Preconditions.checkState(
            lastDirectoryName.compareTo(directoryName) < 0,
            ExecutePreconditionViolationType.DIRECTORY_NOT_SORTED);
        lastDirectoryName = directoryName;
        entryNames.add(directoryName);
      }

      inputDigests.addAll(Iterables.transform(directory.getFilesList(), fileNode -> fileNode.getDigest()));
    }
    // A requested input (or the [Command][] of the [Action][]) was not found in
    // the [ContentAddressableStorage][].
    Preconditions.checkState(
        Iterables.isEmpty(findMissingBlobs(inputDigests.build())),
        ExecutePreconditionViolationType.MISSING_INPUT);

    // FIXME should input/output collisions (through directories) be another
    // invalid action?
    filesUniqueAndSortedPrecondition(action.getOutputFilesList());
    filesUniqueAndSortedPrecondition(action.getOutputDirectoriesList());
    Command command;
    try {
      command = Command.parseFrom(getBlob(commandDigest));
    } catch (InvalidProtocolBufferException ex) {
      Preconditions.checkState(
          false,
          ExecutePreconditionViolationType.INVALID_DIGEST);
      return;
    }
    environmentVariablesUniqueAndSortedPrecondition(
        command.getEnvironmentVariablesList());
  }

  @Override
  public void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      boolean waitForCompletion,
      Consumer<Operation> onOperation) {
    validateAction(action);

    Operation operation = createOperation(action);
    ExecuteOperationMetadata metadata =
      expectExecuteOperationMetadata(operation);

    if (!waitForCompletion) {
      onOperation.accept(operation);
    }

    putOperation(operation);

    Operation.Builder operationBuilder = operation.toBuilder();
    ActionResult actionResult = null;
    if (!skipCacheLookup) {
      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.CACHE_CHECK)
          .build();
      putOperation(operationBuilder
          .setMetadata(Any.pack(metadata))
          .build());
      actionResult = getActionResult(Digests.computeDigest(action));
    }

    if (actionResult != null) {
      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
          .build();
      operationBuilder
          .setDone(true)
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(actionResult)
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
      if (waitForCompletion) {
        watchOperation(operation.getName(), /*watchInitialState=*/ false, o -> {
          if (o.getDone()) {
            onOperation.accept(o);
          }
        });
      }
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
  public void putOperation(Operation operation) {
    if (isCancelled(operation)) {
      throw new IllegalStateException();
    }
    if (isQueued(operation)) {
      if (!matchOperation(operation)) {
        enqueueOperation(operation);
      }
    } else {
      updateOperationWatchers(operation);
    }
  }

  protected void updateOperationWatchers(Operation operation) {
    if (operation.getDone()) {
      outstandingOperations.remove(operation.getName());
    } else {
      outstandingOperations.put(operation.getName(), operation);
    }
  }

  @Override
  public Operation getOperation(String name) {
    return outstandingOperations.get(name);
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
  public void cancelOperation(String name) {
    Operation operation = getOperation(name);
    putOperation(operation.toBuilder()
        .setDone(true)
        .setError(com.google.rpc.Status.newBuilder()
            .setCode(com.google.rpc.Code.CANCELLED.getNumber())
            .build())
        .build());
  }
}
