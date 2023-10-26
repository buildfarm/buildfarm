// Copyright 2018 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.instance.server.AbstractServerInstance.ACTION_INPUT_ROOT_DIRECTORY_PATH;
import static build.buildfarm.instance.server.AbstractServerInstance.DIRECTORY_NOT_SORTED;
import static build.buildfarm.instance.server.AbstractServerInstance.DUPLICATE_DIRENT;
import static build.buildfarm.instance.server.AbstractServerInstance.INVALID_COMMAND;
import static build.buildfarm.instance.server.AbstractServerInstance.OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR;
import static build.buildfarm.instance.server.AbstractServerInstance.OUTPUT_FILE_IS_OUTPUT_ANCESTOR;
import static build.buildfarm.instance.server.AbstractServerInstance.SYMLINK_TARGET_ABSOLUTE;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.actioncache.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.WriteCompleteException;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.net.URL;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.WorkerListMessage;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import lombok.extern.java.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
@Log
public class AbstractServerInstanceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  static class DummyServerInstance extends AbstractServerInstance {
    DummyServerInstance(
        ContentAddressableStorage contentAddressableStorage, ActionCache actionCache) {
      super(
          /* name=*/ null,
          /* digestUtil=*/ null,
          contentAddressableStorage,
          actionCache,
          /* outstandingOperations=*/ null,
          /* completedOperations=*/ null,
          /* activeBlobWrites=*/ null,
          false);
    }

    DummyServerInstance() {
      this(/* contentAddressableStorage=*/ null, /* actionCache=*/ null);
    }

    @Override
    protected Logger getLogger() {
      return log;
    }

    @Override
    protected int getTreeDefaultPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int getTreeMaxPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected TokenizableIterator<DirectoryEntry> createTreeIterator(
        String reason, Digest rootDigest, String pageToken) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Operation createOperation(ActionKey actionKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean matchOperation(Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void enqueueOperation(Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object operationLock() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int getListOperationsDefaultPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int getListOperationsMaxPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> watchOperation(String operationName, Watcher watcher) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void match(Platform platform, MatchListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BackplaneStatus backplaneStatus() {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newOperationStreamInput(
        String name, long offset, RequestMetadata requestMetadata) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Write getOperationStreamWrite(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CasIndexResults reindexCas() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FindOperationsResults findEnrichedOperations(String filterPredicate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public EnrichedOperation findEnrichedOperation(String operationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Operation> findOperations(String filterPredicate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> findOperationsByInvocationId(String invocationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Map.Entry<String, String>> getOperations(Set<String> operationIds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterWorker(String workerName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public WorkerProfileMessage getWorkerProfile() {
      throw new UnsupportedOperationException();
    }

    @Override
    public WorkerListMessage getWorkerList() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void duplicateFileInputIsInvalid() {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        Directory.newBuilder()
            .addAllFiles(
                ImmutableList.of(
                    FileNode.newBuilder().setName("foo").build(),
                    FileNode.newBuilder().setName("foo").build()))
            .build(),
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFile=*/ file -> {},
        /* onInputDirectorie=*/ directory -> {},
        /* onInputDigest=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("/: foo");
    assertThat(violation.getDescription()).isEqualTo(DUPLICATE_DIRENT);
  }

  @Test
  public void duplicateEmptyDirectoryCheckPasses() throws StatusException {
    Directory emptyDirectory = Directory.getDefaultInstance();
    Digest emptyDirectoryDigest = DIGEST_UTIL.compute(emptyDirectory);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder()
                        .setName("bar")
                        .setDigest(emptyDirectoryDigest)
                        .build(),
                    DirectoryNode.newBuilder()
                        .setName("foo")
                        .setDigest(emptyDirectoryDigest)
                        .build()))
            .build(),
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ ImmutableMap.of(Digest.getDefaultInstance(), emptyDirectory),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    checkPreconditionFailure(
        Digest.newBuilder().setHash("should not fail").build(), preconditionFailure.build());
  }

  @Test
  public void unsortedFileInputIsInvalid() {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        Directory.newBuilder()
            .addAllFiles(
                ImmutableList.of(
                    FileNode.newBuilder().setName("foo").build(),
                    FileNode.newBuilder().setName("bar").build()))
            .build(),
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("/: foo > bar");
    assertThat(violation.getDescription()).isEqualTo(DIRECTORY_NOT_SORTED);
  }

  @Test
  public void duplicateDirectoryInputIsInvalid() {
    Directory emptyDirectory = Directory.getDefaultInstance();
    Digest emptyDirectoryDigest = DIGEST_UTIL.compute(emptyDirectory);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder()
                        .setName("foo")
                        .setDigest(emptyDirectoryDigest)
                        .build(),
                    DirectoryNode.newBuilder()
                        .setName("foo")
                        .setDigest(emptyDirectoryDigest)
                        .build()))
            .build(),
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ ImmutableMap.of(emptyDirectoryDigest, emptyDirectory),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("/: foo");
    assertThat(violation.getDescription()).isEqualTo(DUPLICATE_DIRENT);
  }

  @Test
  public void unsortedDirectoryInputIsInvalid() {
    Directory emptyDirectory = Directory.getDefaultInstance();
    Digest emptyDirectoryDigest = DIGEST_UTIL.compute(emptyDirectory);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder()
                        .setName("foo")
                        .setDigest(emptyDirectoryDigest)
                        .build(),
                    DirectoryNode.newBuilder()
                        .setName("bar")
                        .setDigest(emptyDirectoryDigest)
                        .build()))
            .build(),
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ ImmutableMap.of(emptyDirectoryDigest, emptyDirectory),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("/: foo > bar");
    assertThat(violation.getDescription()).isEqualTo(DIRECTORY_NOT_SORTED);
  }

  @Test
  public void shouldValidateIfSymlinkTargetAbsolute() {
    // invalid for disallowed
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    Directory absoluteSymlinkDirectory =
        Directory.newBuilder()
            .addSymlinks(SymlinkNode.newBuilder().setName("foo").setTarget("/root/secret").build())
            .build();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        absoluteSymlinkDirectory,
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFile=*/ file -> {},
        /* onInputDirectorie=*/ directory -> {},
        /* onInputDigest=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("/: foo -> /root/secret");
    assertThat(violation.getDescription()).isEqualTo(SYMLINK_TARGET_ABSOLUTE);

    // valid for allowed
    preconditionFailure = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        absoluteSymlinkDirectory,
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* allowSymlinkTargetAbsolute=*/ true,
        /* onInputFile=*/ file -> {},
        /* onInputDirectorie=*/ directory -> {},
        /* onInputDigest=*/ digest -> {},
        preconditionFailure);
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(0);
  }

  @Test
  public void nestedOutputDirectoriesAreInvalid() {
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateOutputs(
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of("foo", "foo/bar"),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("foo");
    assertThat(violation.getDescription()).isEqualTo(OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR);
  }

  @Test
  public void outputDirectoriesContainingOutputFilesAreInvalid() {
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateOutputs(
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of("foo/bar"),
        ImmutableSet.of("foo"),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("foo");
    assertThat(violation.getDescription()).isEqualTo(OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR);
  }

  @Test
  public void outputFilesAsOutputDirectoryAncestorsAreInvalid() {
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    AbstractServerInstance.validateOutputs(
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of("foo"),
        ImmutableSet.of("foo/bar"),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo("foo");
    assertThat(violation.getDescription()).isEqualTo(OUTPUT_FILE_IS_OUTPUT_ANCESTOR);
  }

  @Test
  public void emptyArgumentListIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    instance.validateCommand(
        Command.getDefaultInstance(),
        DIGEST_UTIL.empty(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo(INVALID_COMMAND);
    assertThat(violation.getDescription()).isEqualTo("argument list is empty");
  }

  @Test
  public void absoluteWorkingDirectoryIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    instance.validateCommand(
        Command.newBuilder().addArguments("foo").setWorkingDirectory("/var/lib/db").build(),
        DIGEST_UTIL.empty(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo(INVALID_COMMAND);
    assertThat(violation.getDescription()).isEqualTo("working directory is absolute");
  }

  @Test
  public void undeclaredWorkingDirectoryIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    Digest inputRootDigest = DIGEST_UTIL.compute(Directory.getDefaultInstance());
    PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
    instance.validateCommand(
        Command.newBuilder().addArguments("foo").setWorkingDirectory("not/an/input").build(),
        inputRootDigest,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(inputRootDigest, Directory.getDefaultInstance()),
        preconditionFailureBuilder);
    PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo(INVALID_COMMAND);
    assertThat(violation.getDescription()).isEqualTo("working directory is not an input directory");
  }

  /*-
   * / -> valid dir
   *   bar/ -> missing dir with digest 'missing' and non-zero size
   *   foo/ -> missing dir with digest 'missing' and non-zero size
   */
  @Test
  public void multipleIdenticalDirectoryMissingAreAllPreconditionFailures() {
    Digest missingDirectoryDigest = Digest.newBuilder().setHash("missing").setSizeBytes(1).build();
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    Directory root =
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder()
                        .setName("bar")
                        .setDigest(missingDirectoryDigest)
                        .build(),
                    DirectoryNode.newBuilder()
                        .setName("foo")
                        .setDigest(missingDirectoryDigest)
                        .build()))
            .build();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        root,
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ ImmutableMap.of(),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    String missingSubject = "blobs/" + DigestUtil.toString(missingDirectoryDigest);
    String missingFmt = "The directory `/%s` was not found in the CAS.";
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(2);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_MISSING);
    assertThat(violation.getSubject()).isEqualTo(missingSubject);
    assertThat(violation.getDescription()).isEqualTo(String.format(missingFmt, "bar"));
    violation = preconditionFailure.getViolationsList().get(1);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_MISSING);
    assertThat(violation.getSubject()).isEqualTo(missingSubject);
    assertThat(violation.getDescription()).isEqualTo(String.format(missingFmt, "foo"));
  }

  /*-
   * / -> valid dir
   *   bar/ -> valid dir
   *     baz/ -> missing dir with digest 'missing-empty' and zero size
   *     quux/ -> missing dir with digest 'missing' and non-zero size
   *   foo/ -> valid dir with digest from /bar/, making it a copy of above
   *
   * Only duplicated-bar appears in the index
   * Empty directory needs short circuit in all cases
   * Result should be 2 missing directory paths, no errors
   */
  @Test
  public void validationRevisitReplicatesPreconditionFailures() {
    Digest missingEmptyDirectoryDigest = Digest.newBuilder().setHash("missing-empty").build();
    Digest missingDirectoryDigest = Digest.newBuilder().setHash("missing").setSizeBytes(1).build();
    Directory foo =
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder()
                        .setName("baz")
                        .setDigest(missingEmptyDirectoryDigest)
                        .build(),
                    DirectoryNode.newBuilder()
                        .setName("quux")
                        .setDigest(missingDirectoryDigest)
                        .build()))
            .build();
    Digest fooDigest = DIGEST_UTIL.compute(foo);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    Directory root =
        Directory.newBuilder()
            .addAllDirectories(
                ImmutableList.of(
                    DirectoryNode.newBuilder().setName("bar").setDigest(fooDigest).build(),
                    DirectoryNode.newBuilder().setName("foo").setDigest(fooDigest).build()))
            .build();
    AbstractServerInstance.validateActionInputDirectory(
        ACTION_INPUT_ROOT_DIRECTORY_PATH,
        root,
        /* pathDigests=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ ImmutableMap.of(fooDigest, foo),
        /* allowSymlinkTargetAbsolute=*/ false,
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    String missingSubject = "blobs/" + DigestUtil.toString(missingDirectoryDigest);
    String missingFmt = "The directory `/%s` was not found in the CAS.";
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(2);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_MISSING);
    assertThat(violation.getSubject()).isEqualTo(missingSubject);
    assertThat(violation.getDescription()).isEqualTo(String.format(missingFmt, "bar/quux"));
    violation = preconditionFailure.getViolationsList().get(1);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_MISSING);
    assertThat(violation.getSubject()).isEqualTo(missingSubject);
    assertThat(violation.getDescription()).isEqualTo(String.format(missingFmt, "foo/quux"));
  }

  @SuppressWarnings("unchecked")
  private static void doBlob(
      ContentAddressableStorage contentAddressableStorage,
      Digest digest,
      ByteString content,
      RequestMetadata requestMetadata) {
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  StreamObserver<ByteString> blobObserver =
                      (StreamObserver) invocation.getArguments()[4];
                  blobObserver.onNext(content);
                  blobObserver.onCompleted();
                  return null;
                })
        .when(contentAddressableStorage)
        .get(
            eq(Compressor.Value.IDENTITY),
            eq(digest),
            /* offset=*/ eq(0L),
            eq(digest.getSizeBytes()),
            any(ServerCallStreamObserver.class),
            eq(requestMetadata));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void outputDirectoriesFilesAreEnsuredPresent() throws Exception {
    // our test subjects - these should appear in the findMissingBlobs request
    Digest fileDigest =
        DIGEST_UTIL.compute(ByteString.copyFromUtf8("Output Directory Root File Content"));
    Digest childFileDigest =
        DIGEST_UTIL.compute(ByteString.copyFromUtf8("Output Directory Child File Content"));
    Digest otherFileDigest =
        DIGEST_UTIL.compute(ByteString.copyFromUtf8("Another Output Directory File Content"));

    // setup block and ensureOutputsPresent trigger
    RequestMetadata requestMetadata =
        RequestMetadata.newBuilder()
            .setCorrelatedInvocationsId(
                "https://localhost:12345/test/build?ENSURE_OUTPUTS_PRESENT=true#92af266a-c5bf-48ca-a723-344ae516a786")
            .build();
    ContentAddressableStorage contentAddressableStorage = mock(ContentAddressableStorage.class);
    ActionCache actionCache = mock(ActionCache.class);
    AbstractServerInstance instance =
        new DummyServerInstance(contentAddressableStorage, actionCache);

    Tree tree =
        Tree.newBuilder()
            .setRoot(
                Directory.newBuilder()
                    .addFiles(FileNode.newBuilder().setDigest(fileDigest))
                    .build())
            .addChildren(
                Directory.newBuilder()
                    .addFiles(FileNode.newBuilder().setDigest(childFileDigest))
                    .build())
            .build();
    Tree otherTree =
        Tree.newBuilder()
            .setRoot(
                Directory.newBuilder()
                    .addFiles(FileNode.newBuilder().setDigest(otherFileDigest))
                    .build())
            .build();
    Digest treeDigest = DIGEST_UTIL.compute(tree);
    doBlob(contentAddressableStorage, treeDigest, tree.toByteString(), requestMetadata);
    Digest otherTreeDigest = DIGEST_UTIL.compute(otherTree);
    doBlob(contentAddressableStorage, otherTreeDigest, otherTree.toByteString(), requestMetadata);
    ActionKey actionKey =
        DigestUtil.asActionKey(DIGEST_UTIL.compute(ByteString.copyFromUtf8("action")));
    ActionResult actionResult =
        ActionResult.newBuilder()
            .addOutputDirectories(OutputDirectory.newBuilder().setTreeDigest(treeDigest).build())
            .addOutputDirectories(
                OutputDirectory.newBuilder().setTreeDigest(otherTreeDigest).build())
            .build();
    when(actionCache.get(actionKey)).thenReturn(immediateFuture(actionResult));

    // invocation
    assertThat(instance.getActionResult(actionKey, requestMetadata).get()).isEqualTo(actionResult);

    // validation
    ArgumentCaptor<Iterable<Digest>> findMissingBlobsCaptor =
        ArgumentCaptor.forClass(Iterable.class);
    verify(contentAddressableStorage, times(1))
        .get(
            eq(Compressor.Value.IDENTITY),
            eq(treeDigest),
            /* offset=*/ eq(0L),
            eq(treeDigest.getSizeBytes()),
            any(ServerCallStreamObserver.class),
            eq(requestMetadata));
    verify(contentAddressableStorage, times(1)).findMissingBlobs(findMissingBlobsCaptor.capture());
    assertThat(findMissingBlobsCaptor.getValue())
        .containsAtLeast(fileDigest, childFileDigest, otherFileDigest);
  }

  @Test
  public void fetchBlobWriteCompleteIsSuccess() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Fetch Blob Content");
    Digest contentDigest = DIGEST_UTIL.compute(content);
    Digest expectedDigest = contentDigest.toBuilder().setSizeBytes(-1).build();

    ContentAddressableStorage contentAddressableStorage = mock(ContentAddressableStorage.class);
    AbstractServerInstance instance = new DummyServerInstance(contentAddressableStorage, null);
    RequestMetadata requestMetadata = RequestMetadata.getDefaultInstance();
    Write write = mock(Write.class);

    FeedbackOutputStream writeCompleteOutputStream =
        new FeedbackOutputStream() {
          @Override
          public void write(int n) throws WriteCompleteException {
            throw new WriteCompleteException();
          }

          @Override
          public boolean isReady() {
            return true;
          }
        };
    when(write.getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(writeCompleteOutputStream);
    when(contentAddressableStorage.getWrite(
            eq(Compressor.Value.IDENTITY), eq(contentDigest), any(UUID.class), eq(requestMetadata)))
        .thenReturn(write);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    when(httpURLConnection.getContentLengthLong()).thenReturn(contentDigest.getSizeBytes());
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    when(httpURLConnection.getInputStream()).thenReturn(content.newInput());
    URL url = mock(URL.class);
    when(url.openConnection()).thenReturn(httpURLConnection);

    assertThat(instance.fetchBlobUrls(ImmutableList.of(url), expectedDigest, requestMetadata).get())
        .isEqualTo(contentDigest);
    verify(contentAddressableStorage, times(1))
        .getWrite(
            eq(Compressor.Value.IDENTITY), eq(contentDigest), any(UUID.class), eq(requestMetadata));
    verify(write, times(1)).getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(httpURLConnection, times(1)).getContentLengthLong();
    verify(httpURLConnection, times(1)).getResponseCode();
    verify(url, times(1)).openConnection();
  }
}
