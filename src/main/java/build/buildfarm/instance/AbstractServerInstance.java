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
import static java.util.logging.Level.SEVERE;

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
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionCapabilities;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;

public abstract class AbstractServerInstance implements Instance {
  private static final Logger logger = Logger.getLogger(AbstractServerInstance.class.getName());

  private final String name;
  protected final ContentAddressableStorage contentAddressableStorage;
  protected final ActionCache actionCache;
  protected final OperationsMap outstandingOperations;
  protected final OperationsMap completedOperations;
  protected final Map<Digest, ByteString> activeBlobWrites;
  protected final DigestUtil digestUtil;

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

  public static final String MISSING_INPUT =
      "A requested input (or the `Action` or its `Command`) was not found in the CAS.";

  public static final String INVALID_DIGEST = "A `Digest` in the input tree is invalid.";

  public static final String INVALID_ACTION = "The `Action` was invalid.";

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
      ActionCache actionCache,
      OperationsMap outstandingOperations,
      OperationsMap completedOperations,
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
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException {
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

  private static void stringsUniqueAndSortedPrecondition(
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
      if (direction > 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(unsortedViolationMessage)
            .setDescription(lastString + " > " + string);
      }
    }
  }

  private static void filesUniqueAndSortedPrecondition(
      Iterable<String> files, PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        files,
        DUPLICATE_DIRENT,
        DIRECTORY_NOT_SORTED,
        preconditionFailure);
  }

  private static void environmentVariablesUniqueAndSortedPrecondition(
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

  private static void enumerateActionInputDirectory(
      String directoryPath,
      Directory directory,
      Map<Digest, Directory> directoriesIndex,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories) {
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
          directoriesIndex.get(directoryDigest),
          directoriesIndex,
          inputFiles,
          inputDirectories);
    }
  }

  @VisibleForTesting
  public static void validateActionInputDirectory(
      String directoryPath,
      Directory directory,
      Stack<Digest> pathDigests,
      Set<Digest> visited,
      Map<Digest, Directory> directoriesIndex,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories,
      ImmutableSet.Builder<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure) {
    Set<String> entryNames = new HashSet<>();

    String lastFileName = "";
    for (FileNode fileNode : directory.getFilesList()) {
      String fileName = fileNode.getName();
      if (entryNames.contains(fileName)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DUPLICATE_DIRENT)
            .setDescription("/" + directoryPath + ": " + fileName);
      } else if (lastFileName.compareTo(fileName) > 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_NOT_SORTED)
            .setDescription("/" + directoryPath + ": " + lastFileName + " > " + fileName);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          fileName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      lastFileName = fileName;
      entryNames.add(fileName);

      inputDigests.add(fileNode.getDigest());
      String filePath = directoryPath.isEmpty() ? fileName : (directoryPath + "/" + fileName);
      inputFiles.add(filePath);
    }
    String lastDirectoryName = "";
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      String directoryName = directoryNode.getName();

      if (entryNames.contains(directoryName)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DUPLICATE_DIRENT)
            .setDescription("/" + directoryPath + ": " + directoryName);
      } else if (lastDirectoryName.compareTo(directoryName) > 0) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_NOT_SORTED)
            .setDescription("/" + directoryPath + ": " + lastDirectoryName + " > " + directoryName);
      }
      /* FIXME serverside validity check? regex?
      Preconditions.checkState(
          directoryName.isValidFilename(),
          INVALID_FILE_NAME);
      */
      lastDirectoryName = directoryName;
      entryNames.add(directoryName);

      Digest directoryDigest = directoryNode.getDigest();
      if (pathDigests.contains(directoryDigest)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(DIRECTORY_CYCLE_DETECTED)
            .setDescription("/" + directoryPath + ": " + directoryName);
      } else {
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
              directoriesIndex,
              inputFiles,
              inputDirectories,
              inputDigests,
              preconditionFailure);
        } else {
          Directory subDirectory = directoriesIndex.get(directoryDigest);
          if (subDirectory != null) {
            enumerateActionInputDirectory(
                subDirectoryPath,
                directoriesIndex.get(directoryDigest),
                directoriesIndex,
                inputFiles,
                inputDirectories);
          }
          // null directory case will be handled by input missing in validateActionInputDirectoryDigest
        }
      }
    }
  }

  private static void validateActionInputDirectoryDigest(
      String directoryPath,
      Digest directoryDigest,
      Stack<Digest> pathDigests,
      Set<Digest> visited,
      Map<Digest, Directory> directoriesIndex,
      ImmutableSet.Builder<String> inputFiles,
      ImmutableSet.Builder<String> inputDirectories,
      ImmutableSet.Builder<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure) {
    pathDigests.push(directoryDigest);

    Directory directory = directoriesIndex.get(directoryDigest);
    if (directory == null) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(MISSING_INPUT)
          .setDescription("Directory " + DigestUtil.toString(directoryDigest));
    } else {
      validateActionInputDirectory(
          directoryPath,
          directory,
          pathDigests,
          visited,
          directoriesIndex,
          inputFiles,
          inputDirectories,
          inputDigests,
          preconditionFailure);
    }
    pathDigests.pop();
    visited.add(directoryDigest);
  }

  protected ListenableFuture<Iterable<Directory>> getTreeDirectories(Digest inputRoot, ExecutorService service) {
    return listeningDecorator(service).submit(() -> {
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
    ImmutableSet.Builder<String> inputDirectoriesBuilder = ImmutableSet.builder();
    ImmutableSet.Builder<String> inputFilesBuilder = ImmutableSet.builder();

    // needs futuring
    validateActionInputDirectoryDigest(
        "",
        action.getInputRootDigest(),
        new Stack<>(),
        new HashSet<>(),
        directoriesIndex,
        inputFilesBuilder,
        inputDirectoriesBuilder,
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
        command.getOutputFilesList(), preconditionFailure);
    filesUniqueAndSortedPrecondition(
        command.getOutputDirectoriesList(), preconditionFailure);

    AbstractServerInstance.validateOutputs(
        inputFilesBuilder.build(),
        inputDirectoriesBuilder.build(),
        Sets.newHashSet(command.getOutputFilesList()),
        Sets.newHashSet(command.getOutputDirectoriesList()));

    environmentVariablesUniqueAndSortedPrecondition(
        command.getEnvironmentVariablesList(), preconditionFailure);
    if (command.getArgumentsList().isEmpty()) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_COMMAND)
          .setDescription("argument list is empty");
    }
    checkViolations(() -> actionDigest, preconditionFailure.getViolationsList());
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

  private Action validateActionDigest(Digest actionDigest, PreconditionFailure.Builder preconditionFailure) throws InterruptedException {
    ByteString actionBlob = null;
    try {
      actionBlob = getBlob(actionDigest);
    } catch (IOException e) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(MISSING_INPUT)
          .setDescription(DigestUtil.toString(actionDigest));
    }
    Preconditions.checkState(actionBlob != null, MISSING_INPUT);

    Action action;
    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_ACTION)
          .setDescription(DigestUtil.toString(actionDigest));
      throw new IllegalStateException(INVALID_ACTION);
    }

    validateAction(action, preconditionFailure);
    return action;
  }

  // this deserves a real async execute, but not now
  @Override
  public void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Predicate<Operation> watcher) throws InterruptedException {
    final Action action;
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    try {
      action = validateActionDigest(actionDigest, preconditionFailure);
    } catch (IllegalStateException e) {
      logger.log(SEVERE, "action is invalid " + DigestUtil.toString(actionDigest), e);
      com.google.rpc.Status.Builder status = com.google.rpc.Status.newBuilder()
          .setCode(Status.FAILED_PRECONDITION.getCode().value())
          .setMessage(e.getMessage());
      if (preconditionFailure.getViolationsCount() > 0) {
        status.addDetails(Any.pack(preconditionFailure.build()));
      }
      Operation operation = Operation.newBuilder()
          .setDone(true)
          .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
              .setStage(Stage.COMPLETED)
              .build()))
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setStatus(status)
              .build()))
          .build();
      Preconditions.checkState(!watcher.test(operation), "watcher did not respect completed operation");
      return;
    }

    ActionKey actionKey = DigestUtil.asActionKey(actionDigest);
    Operation operation = createOperation(actionKey);

    logger.info(System.nanoTime() + ": Operation " + operation.getName() + " was created");

    getLogger().info(
        String.format(
            "%s::execute(%s): %s",
            getName(),
            DigestUtil.toString(actionDigest),
            operation.getName()));

    putOperation(operation);

    watchOperation(operation.getName(), watcher);

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
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error unpacking execute operation metadata for " + operation.getName(), e);
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
        logger.log(SEVERE, "error getting action for " + operation.getName(), e);
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

  protected ListenableFuture<Action> expectAction(Digest actionBlobDigest) {
    return parseFuture(actionBlobDigest, Action.parser());
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
        operation.getResultCase() == Operation.ResultCase.RESPONSE &&
        operation.getResponse().is(ExecuteResponse.class) &&
        expectExecuteResponse(operation).getStatus().getCode() != Code.OK.value();
  }

  private static ExecuteResponse expectExecuteResponse(Operation operation) {
    try {
      return operation.getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
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
    String name = operation.getName();
    if (isCancelled(operation)) {
      if (outstandingOperations.remove(name) == null) {
        throw new IllegalStateException(
            String.format("Operation %s was not in outstandingOperations", name));
      }
      updateOperationWatchers(operation);
      return true;
    }
    if (isExecuting(operation) &&
        !outstandingOperations.contains(name)) {
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
   * <p>
   * the lock retrieved for an operation will guard against races
   * during transfers/retrievals/removals
   */
  protected abstract Object operationLock(String operationName);

  protected void updateOperationWatchers(Operation operation) throws InterruptedException {
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
          outstandingOperations.contains(name)) {
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

  public boolean requeueOperation(Operation operation) throws InterruptedException {
    String name = operation.getName();
    if (!isQueued(operation)) {
      throw new IllegalStateException(
          String.format(
              "Operation %s stage is not QUEUED",
              name));
    }

    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      throw new IllegalStateException(
          String.format(
              "Operation %s does not contain ExecuteOperationMetadata",
              name));
    }

    Digest actionDigest = metadata.getActionDigest();

    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    try {
      validateActionDigest(actionDigest, preconditionFailure);
    } catch (IllegalStateException e) {
      errorOperation(name, com.google.rpc.Status.newBuilder()
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .addDetails(Any.pack(preconditionFailure.build()))
          .build());
      return false;
    }

    getLogger().info(
        String.format(
            "%s::requeueOperation(%s): %s",
            getName(),
            DigestUtil.toString(actionDigest),
            name));

    return putOperation(operation);
  }

  protected void errorOperation(String name, com.google.rpc.Status status) throws InterruptedException {
    Operation operation = getOperation(name);
    if (operation == null) {
      // throw new IllegalStateException("Trying to error nonexistent operation [" + name + "]");
      logger.severe("Erroring non-existent operation " + name + ", will signal watchers");
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
            .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
            .build()))
        .setResponse(Any.pack(ExecuteResponse.newBuilder()
            .setStatus(status)
            .build()))
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
            .setCode(Code.DEADLINE_EXCEEDED.value()))
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

  abstract protected Logger getLogger();
}
