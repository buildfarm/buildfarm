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

import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.instance.server.AbstractServerInstance.ACTION_INPUT_ROOT_DIRECTORY_PATH;
import static build.buildfarm.instance.server.AbstractServerInstance.DIRECTORY_NOT_SORTED;
import static build.buildfarm.instance.server.AbstractServerInstance.DUPLICATE_DIRENT;
import static build.buildfarm.instance.server.AbstractServerInstance.INVALID_COMMAND;
import static build.buildfarm.instance.server.AbstractServerInstance.OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR;
import static build.buildfarm.instance.server.AbstractServerInstance.OUTPUT_FILE_IS_OUTPUT_ANCESTOR;
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
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.instance.MatchListener;
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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.util.Stack;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class AbstractServerInstanceTest {
  private static final Logger logger = Logger.getLogger(AbstractServerInstanceTest.class.getName());

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
          /* activeBlobWrites=*/ null);
    }

    DummyServerInstance() {
      this(/* contentAddressableStorage=*/ null, /* actionCache=*/ null);
    }

    @Override
    protected Logger getLogger() {
      return logger;
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
    public CasIndexResults reindexCas(@Nullable String hostName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FindOperationsResults findOperations(String filterPredicate) {
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
        /* onInputFiles=*/ file -> {},
        /* onInputDirectories=*/ directory -> {},
        /* onInputDigests=*/ digest -> {},
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
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
                      (StreamObserver) invocation.getArguments()[3];
                  blobObserver.onNext(content);
                  blobObserver.onCompleted();
                  return null;
                })
        .when(contentAddressableStorage)
        .get(
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
            eq(treeDigest),
            /* offset=*/ eq(0L),
            eq(treeDigest.getSizeBytes()),
            any(ServerCallStreamObserver.class),
            eq(requestMetadata));
    verify(contentAddressableStorage, times(1)).findMissingBlobs(findMissingBlobsCaptor.capture());
    assertThat(findMissingBlobsCaptor.getValue())
        .containsAtLeast(fileDigest, childFileDigest, otherFileDigest);
  }
}
