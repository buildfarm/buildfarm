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
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.Status;
import java.io.IOException;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public abstract class AbstractServerInstance implements Instance {
  private final String name;
  protected final ContentAddressableStorage contentAddressableStorage;
  protected final ActionCache actionCache;
  protected final OperationsMap outstandingOperations;
  protected final OperationsMap completedOperations;
  protected final DigestUtil digestUtil;

  public static final String ACTION_INPUT_ROOT_DIRECTORY_PATH = "";

  public static final String DUPLICATE_DIRENT =
      "One of the input `Directory` has multiple entries with the same file name. This will also"
          + " occur if the worker filesystem considers two names to be the same, such as two names"
          + " that vary only by case on a case-insensitive filesystem, or two names with the same"
          + " normalized form on a filesystem that performs Unicode normalization on filenames.";

  public static final String DIRECTORY_NOT_SORTED =
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

  public static final String MISSING_INPUT =
      "A requested input (or the `Command` of the `Action`) was not found in the CAS.";

  public static final String INVALID_DIGEST = "A `Digest` in the input tree is invalid.";

  public static final String INVALID_ACTION = "The `Action` was invalid.";

  public static final String INVALID_COMMAND = "The `Command` of the `Action` was invalid.";

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

  public static final String VIOLATION_TYPE_MISSING = "MISSING";

  public static final String VIOLATION_TYPE_INVALID = "INVALID";

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
    return contentAddressableStorage.findMissingBlobs(digests);
  }

  protected abstract int getTreeDefaultPageSize();
  protected abstract int getTreeMaxPageSize();

  protected abstract TokenizableIterator<DirectoryEntry> createTreeIterator(
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

    TokenizableIterator<DirectoryEntry> iter =
        createTreeIterator(rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next().getDirectory();
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

  private void filesUniqueAndSortedPrecondition(
      Iterable<String> files,
      PreconditionFailure.Builder preconditionFailure) {
    stringsUniqueAndSortedPrecondition(
        files,
        DUPLICATE_DIRENT,
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

  private void enumerateActionInputDirectory(
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
  public void validateActionInputDirectory(
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
          enumerateActionInputDirectory(
              subDirectoryPath,
              directoriesIndex.get(directoryDigest),
              directoriesIndex,
              inputFiles,
              inputDirectories);
        }
      }
    }
  }

  private void validateActionInputDirectoryDigest(
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
    final Directory directory;
    if (directoryDigest.getSizeBytes() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(directoryDigest);
    }
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

  protected Iterable<Directory> getTreeDirectories(Digest inputRoot) {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();

    TokenizableIterator<DirectoryEntry> iterator = createTreeIterator(inputRoot, /* pageToken=*/ "");
    while (iterator.hasNext()) {
      DirectoryEntry entry = iterator.next();
      Directory directory = entry.getDirectory();
      if (directory != null) {
        directories.add(directory);
      }
    }

    return directories.build();
  }

  protected Map<Digest, Directory> createDirectoriesIndex(Iterable<Directory> directories) {
    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
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

  private void validateInputs(
			Iterable<Digest> inputDigests,
			PreconditionFailure.Builder preconditionFailure) {
    preconditionFailure.addAllViolations(
        Iterables.transform(
            findMissingBlobs(inputDigests),
            (digest) -> Violation.newBuilder()
                .setType(VIOLATION_TYPE_MISSING)
                .setSubject(MISSING_INPUT)
                .setDescription(DigestUtil.toString(digest))
                .build()));
  }

  @VisibleForTesting
  public static String invalidActionMessage(Digest actionDigest) {
    return String.format("Action %s is invalid", DigestUtil.toString(actionDigest));
  }

  private static void checkPreconditionFailure(
      Digest actionDigest,
      PreconditionFailure preconditionFailure)
      throws StatusException {
    if (preconditionFailure.getViolationsCount() != 0) {
      throw StatusProto.toStatusException(com.google.rpc.Status.newBuilder()
          .setCode(Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionMessage(actionDigest))
          .addDetails(Any.pack(preconditionFailure))
          .build());
    }
  }

  private Action validateActionDigest(Digest actionDigest)
      throws StatusException {
    Action action = null;
    PreconditionFailure.Builder preconditionFailure =
        PreconditionFailure.newBuilder();
    ByteString actionBlob = null;
    if (actionDigest.getSizeBytes() != 0) {
      actionBlob = getBlob(actionDigest);
    }
		if (actionBlob == null) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(MISSING_INPUT)
          .setDescription("Action " + DigestUtil.toString(actionDigest));
    } else {
      try {
        action = Action.parseFrom(actionBlob);
      } catch (InvalidProtocolBufferException e) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(INVALID_ACTION)
            .setDescription("Action " + DigestUtil.toString(actionDigest));
      }
      if (action != null) {
        validateAction(action, preconditionFailure);
      }
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());
    return action;
  }

  protected void validateAction(
      Action action,
      PreconditionFailure.Builder preconditionFailure) {
    ImmutableSet.Builder<Digest> inputDigestsBuilder = ImmutableSet.builder();
    validateAction(
        action,
        expectCommand(action.getCommandDigest()),
        getTreeDirectories(action.getInputRootDigest()),
        inputDigestsBuilder,
        preconditionFailure);
    validateInputs(
        inputDigestsBuilder.build(),
        preconditionFailure);
  }

  private void validateAction(
      Action action,
      @Nullable Command command,
      Iterable<Directory> directories,
      ImmutableSet.Builder<Digest> inputDigests,
      PreconditionFailure.Builder preconditionFailure) {
    Map<Digest, Directory> directoriesIndex = createDirectoriesIndex(directories);
    ImmutableSet.Builder<String> inputDirectoriesBuilder = new ImmutableSet.Builder<>();
    ImmutableSet.Builder<String> inputFilesBuilder = new ImmutableSet.Builder<>();

    inputDirectoriesBuilder.add(ACTION_INPUT_ROOT_DIRECTORY_PATH);
    validateActionInputDirectoryDigest(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        action.getInputRootDigest(),
        new Stack<>(),
        new HashSet<>(),
        directoriesIndex,
        inputFilesBuilder,
        inputDirectoriesBuilder,
        inputDigests,
        preconditionFailure);

    if (command == null) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(MISSING_INPUT)
          .setDescription("Command " + DigestUtil.toString(action.getCommandDigest()));
    } else {
      // FIXME should input/output collisions (through directories) be another
      // invalid action?
      filesUniqueAndSortedPrecondition(
          command.getOutputFilesList(), preconditionFailure);
      filesUniqueAndSortedPrecondition(
          command.getOutputDirectoriesList(), preconditionFailure);

      validateOutputs(
          inputFilesBuilder.build(),
          inputDirectoriesBuilder.build(),
          Sets.newHashSet(command.getOutputFilesList()),
          Sets.newHashSet(command.getOutputDirectoriesList()),
          preconditionFailure);

      environmentVariablesUniqueAndSortedPrecondition(
          command.getEnvironmentVariablesList(), preconditionFailure);
      if (command.getArgumentsList().isEmpty()) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(INVALID_COMMAND)
            .setDescription("argument list is empty");
      }
    }
  }

  @VisibleForTesting
  public static void validateOutputs(
      Set<String> inputFiles,
      Set<String> inputDirectories,
      Set<String> outputFiles,
      Set<String> outputDirectories,
      PreconditionFailure.Builder preconditionFailure) {
    Set<String> outputFilesAndDirectories = Sets.intersection(outputFiles, outputDirectories);
    if (!outputFilesAndDirectories.isEmpty()) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(OUTPUT_FILE_DIRECTORY_COLLISION)
          .setDescription(outputFilesAndDirectories.toString());
    }

    Set<String> parentsOfOutputs = new HashSet<>();

    // An output file cannot be a parent of another output file, be a child of a listed output directory, or have the same path as any of the listed output directories.
    for (String outputFile : outputFiles) {
      if (inputDirectories.contains(outputFile)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(OUTPUT_FILE_IS_INPUT_DIRECTORY)
            .setDescription(outputFile);
      }
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
      if (inputFiles.contains(outputDir)) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(OUTPUT_DIRECTORY_IS_INPUT_FILE)
            .setDescription(outputDir);
      }
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
    Set<String> outputFileAncestors = Sets.intersection(outputFiles, parentsOfOutputs);
    for (String outputFileAncestor : outputFileAncestors) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(OUTPUT_FILE_IS_OUTPUT_ANCESTOR)
          .setDescription(outputFileAncestor);
    }
    Set<String> outputDirectoryAncestors = Sets.intersection(outputDirectories, parentsOfOutputs);
    for (String outputDirectoryAncestor : outputDirectoryAncestors) {
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR)
          .setDescription(outputDirectoryAncestor);
    }
  }

  private void logFailedStatus(Digest actionDigest, com.google.rpc.Status status) {
    String message = String.format(
        "%s: Code %d: %s\n",
        DigestUtil.toString(actionDigest),
        status.getCode(),
        status.getMessage());
    for (Any detail : status.getDetailsList()) {
      if (detail.is(PreconditionFailure.class)) {
        message += "  PreconditionFailure:\n";
        PreconditionFailure preconditionFailure;
        try {
          preconditionFailure = detail.unpack(PreconditionFailure.class);
          for (Violation violation : preconditionFailure.getViolationsList()) {
            message += String.format(
                "    Violation: %s %s: %s\n",
                violation.getType(),
                violation.getSubject(),
                violation.getDescription());
          }
        } catch (InvalidProtocolBufferException e) {
          message += "  " + e.getMessage();
        }
      } else {
        message += "  Unknown Detail\n";
      }
    }
    getLogger().fine(message);
  }

  @Override
  public void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      Predicate<Operation> watcher) throws InterruptedException {
    Action action;
    try {
      action = validateActionDigest(actionDigest);
    } catch (StatusException e) {
      com.google.rpc.Status status = StatusProto.fromThrowable(e);
      if (status == null) {
        getLogger().log(SEVERE, "no rpc status from exception", e);
        status = com.google.rpc.Status.newBuilder()
            .setCode(Status.fromThrowable(e).getCode().value())
            .build();
      }
      logFailedStatus(actionDigest, status);
      Operation operation = Operation.newBuilder()
          .setDone(true)
          .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
              .setStage(Stage.COMPLETED)
              .build()))
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setStatus(status)
              .build()))
          .build();
      if (watcher.test(operation)) {
        getLogger().severe("watcher did not respect completed operation for action " + DigestUtil.toString(actionDigest));
      }
      return;
    }

    ActionKey actionKey = DigestUtil.asActionKey(actionDigest);
    Operation operation = createOperation(actionKey);
    ExecuteOperationMetadata metadata =
        expectExecuteOperationMetadata(operation);

    getLogger().fine(
        String.format(
            "%s::execute(%s): %s",
            getName(),
            DigestUtil.toString(actionDigest),
            operation.getName()));

    putOperation(operation);

    watchOperation(operation.getName(), watcher);

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
                  .setCode(Code.OK.getNumber())
                  .build())
              .setCachedResult(true)
              .build()));
    } else {
      onQueue(operation, action);
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

  protected ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    Preconditions.checkState(
        operation.getMetadata().is(ExecuteOperationMetadata.class));
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Action expectAction(Digest actionDigest) {
    ByteString actionBlob = getBlob(actionDigest);
    if (actionBlob == null) {
      return null;
    }
    try {
      return Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Action expectAction(Operation operation) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      return null;
    }
    return expectAction(metadata.getActionDigest());
  }

  protected Command expectCommand(Operation operation) {
    Action action = expectAction(operation);
    if (action == null) {
      return null;
    }
    return expectCommand(action.getCommandDigest());
  }

  protected Command expectCommand(Digest commandDigest) {
    ByteString commandBlob = getBlob(commandDigest);
    if (commandBlob == null) {
      return null;
    }
    try {
      return Command.parseFrom(commandBlob);
    } catch (InvalidProtocolBufferException ex) {
      return null;
    }
  }

  protected Directory expectDirectory(Digest directoryBlobDigest) {
    try {
      ByteString directoryBlob = getBlob(directoryBlobDigest);
      if (directoryBlob != null) {
        return Directory.parseFrom(directoryBlob);
      }
    } catch (InvalidProtocolBufferException ex) {
    }
    return null;
  }

  protected boolean isCancelled(Operation operation) {
    return operation.getDone() &&
        operation.getResultCase() == Operation.ResultCase.RESPONSE &&
        operation.getResponse().is(ExecuteResponse.class) &&
        expectExecuteResponse(operation).getStatus().getCode() == Code.CANCELLED.getNumber();
  }

  private static ExecuteResponse expectExecuteResponse(Operation operation) {
    try {
      return operation.getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  protected boolean isQueued(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        Stage.QUEUED;
  }

  protected boolean isExecuting(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        Stage.EXECUTING;
  }

  protected boolean isComplete(Operation operation) {
    return expectExecuteOperationMetadata(operation).getStage() ==
        Stage.COMPLETED;
  }

  abstract protected boolean matchOperation(Operation operation) throws InterruptedException;
  abstract protected void enqueueOperation(Operation operation);

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
    Operation operation = getOperation(name);
    if (operation == null) {
      operation = Operation.newBuilder()
          .setName(name)
          .setMetadata(Any.pack(ExecuteOperationMetadata.getDefaultInstance()))
          .build();
    }
    errorOperation(operation, com.google.rpc.Status.newBuilder()
        .setCode(Code.CANCELLED.getNumber())
        .build());
  }

  @Override
  public boolean putAndValidateOperation(Operation operation) throws InterruptedException {
    if (isQueued(operation)) {
      return requeueOperation(operation);
    }
    return putOperation(operation);
  }

  @VisibleForTesting
  public boolean requeueOperation(Operation operation) throws InterruptedException {
    String name = operation.getName();
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      // ensure that watchers are notified
      String message = String.format(
          "Operation %s does not contain ExecuteOperationMetadata",
          name);
      errorOperation(operation, com.google.rpc.Status.newBuilder()
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(message)
          .build());
      return false;
    }

    Digest actionDigest = metadata.getActionDigest();
    try {
      validateActionDigest(actionDigest);
    } catch (StatusException e) {
      com.google.rpc.Status status = StatusProto.fromThrowable(e);
      if (status == null) {
        getLogger().log(SEVERE, "no rpc status from exception", e);
        status = com.google.rpc.Status.newBuilder()
            .setCode(Status.fromThrowable(e).getCode().value())
            .build();
      }
      logFailedStatus(actionDigest, status);
      errorOperation(operation, status);
      return false;
    }

    getLogger().fine(
        String.format(
            "%s::requeueOperation(%s): %s",
            getName(),
            DigestUtil.toString(actionDigest),
            name));

    return putOperation(operation);
  }

  protected void errorOperation(
      Operation operation,
      com.google.rpc.Status status) throws InterruptedException {
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
  public boolean pollOperation(String operationName, Stage stage) {
    if (stage != Stage.QUEUED && stage != Stage.EXECUTING) {
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
