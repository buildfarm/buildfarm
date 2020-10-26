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

package build.buildfarm.instance;

import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.instance.AbstractServerInstance.ACTION_INPUT_ROOT_DIRECTORY_PATH;
import static build.buildfarm.instance.AbstractServerInstance.DIRECTORY_NOT_SORTED;
import static build.buildfarm.instance.AbstractServerInstance.DUPLICATE_DIRENT;
import static build.buildfarm.instance.AbstractServerInstance.INVALID_COMMAND;
import static build.buildfarm.instance.AbstractServerInstance.OUTPUT_DIRECTORY_IS_OUTPUT_ANCESTOR;
import static build.buildfarm.instance.AbstractServerInstance.OUTPUT_FILE_IS_OUTPUT_ANCESTOR;
import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.OperationsStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import java.io.InputStream;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractServerInstanceTest {
  private static final Logger logger = Logger.getLogger(AbstractServerInstanceTest.class.getName());

  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  class DummyServerInstance extends AbstractServerInstance {
    DummyServerInstance() {
      super(
          /* name=*/ null,
          /* digestUtil=*/ null,
          /* contentAddressableStorage=*/ null,
          /* actionCache=*/ null,
          /* outstandingOperations=*/ null,
          /* completedOperations=*/ null,
          /* activeBlobWrites=*/ null);
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
    protected Object operationLock(String operationName) {
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
    public GetClientStartTimeResult getClientStartTime(String clientKey) {
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
    public OperationsStatus operationsStatus() {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newOperationStreamInput(
        String name,
        long offset,
        long deadlineAfter,
        TimeUnit deadlineAfterUnits,
        RequestMetadata requestMetadata) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Write getOperationStreamWrite(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CasIndexResults reindexCas(String hostName) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void duplicateFileInputIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
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
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
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
    AbstractServerInstance instance = new DummyServerInstance();

    Directory emptyDirectory = Directory.getDefaultInstance();
    Digest emptyDirectoryDigest = DIGEST_UTIL.compute(emptyDirectory);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
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
    AbstractServerInstance instance = new DummyServerInstance();

    Directory emptyDirectory = Directory.getDefaultInstance();
    Digest emptyDirectoryDigest = DIGEST_UTIL.compute(emptyDirectory);
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
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
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of("foo", "foo/bar"),
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
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of("foo/bar"),
        ImmutableSet.<String>of("foo"),
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
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of(),
        ImmutableSet.<String>of("foo"),
        ImmutableSet.<String>of("foo/bar"),
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
}
