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

import static com.google.common.truth.Truth.assertThat;
import static build.buildfarm.instance.AbstractServerInstance.DIRECTORY_NOT_SORTED;
import static build.buildfarm.instance.AbstractServerInstance.DUPLICATE_FILE_NODE;
import static build.buildfarm.instance.AbstractServerInstance.VIOLATION_TYPE_INVALID;

import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import java.io.InputStream;
import java.util.Stack;
import java.util.function.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractServerInstanceTest {
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
    protected int getTreeDefaultPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int getTreeMaxPageSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected TokenizableIterator<DirectoryEntry> createTreeIterator(
        Digest rootDigest, String pageToken) {
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
    public boolean watchOperation(
        String operationName,
        boolean watchInitialState,
        Predicate<Operation> watcher) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void match(Platform platform, MatchListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newStreamInput(String name, long offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void duplicateFileInputIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailure =
        PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
        Directory.newBuilder()
            .addAllFiles(
                ImmutableList.of(
                    FileNode.newBuilder().setName("foo").build(),
                    FileNode.newBuilder().setName("foo").build()))
            .build(),
        /* path=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* inputDigests=*/ ImmutableSet.builder(),
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo(DUPLICATE_FILE_NODE);
  }

  @Test
  public void unsortedFileInputIsInvalid() {
    AbstractServerInstance instance = new DummyServerInstance();

    PreconditionFailure.Builder preconditionFailure =
        PreconditionFailure.newBuilder();
    instance.validateActionInputDirectory(
        Directory.newBuilder()
            .addAllFiles(
                ImmutableList.of(
                    FileNode.newBuilder().setName("foo").build(),
                    FileNode.newBuilder().setName("bar").build()))
            .build(),
        /* path=*/ new Stack<>(),
        /* visited=*/ Sets.newHashSet(),
        /* directoriesIndex=*/ Maps.newHashMap(),
        /* inputDigests=*/ ImmutableSet.builder(),
        preconditionFailure);

    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getSubject()).isEqualTo(DIRECTORY_NOT_SORTED);
    assertThat(violation.getDescription()).isEqualTo("foo > bar");
  }
}
