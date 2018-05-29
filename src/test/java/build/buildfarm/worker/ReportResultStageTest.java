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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputDirectory;
import com.google.devtools.remoteexecution.v1test.Tree;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.junit.Before;
import org.junit.Test;

public class ReportResultStageTest {
  private final Configuration config;

  private FileSystem fileSystem;
  private Path root;

  @Mock
  private ByteStreamUploader mockUploader;

  private DigestUtil digestUtil;
  private ReportResultStage reportResultStage;

  protected ReportResultStageTest(Configuration config) {
    this.config = config.toBuilder()
        .setAttributeViews("posix")
        .build();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    fileSystem = Jimfs.newFileSystem(config);
    root = Iterables.getFirst(fileSystem.getRootDirectories(), null);

    digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    WorkerContext mockWorkerContext = mock(WorkerContext.class);
    when(mockWorkerContext.getUploader()).thenReturn(mockUploader);
    when(mockWorkerContext.getDigestUtil()).thenReturn(digestUtil);
    PipelineStage error = mock(PipelineStage.class);
    reportResultStage = new ReportResultStage(mockWorkerContext, error);
    fileSystem = Jimfs.newFileSystem(config);
  }

  @Test
  public void uploadOutputsUploadsEmptyOutputDirectories()
      throws IOException, InterruptedException {
    Files.createDirectory(root.resolve("foo"));
    // maybe make some files...
    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    reportResultStage.uploadOutputs(
        resultBuilder,
        root,
        ImmutableList.<String>of(),
        ImmutableList.<String>of("foo"));
    Tree emptyTree = Tree.newBuilder()
        .setRoot(Directory.getDefaultInstance())
        .build();
    verify(mockUploader)
        .uploadBlobs(eq(ImmutableList.<Chunker>of(
            new Chunker(emptyTree.toByteString(), digestUtil.compute(emptyTree)))));
    assertThat(resultBuilder.getOutputDirectoriesList()).containsExactly(
        OutputDirectory.newBuilder()
            .setPath("foo")
            .setTreeDigest(digestUtil.compute(emptyTree))
            .build());
  }

  @Test
  public void uploadOutputsUploadsFiles()
      throws IOException, InterruptedException {
    Path topdir = root.resolve("foo");
    Files.createDirectory(topdir);
    Path file = topdir.resolve("bar");
    Files.createFile(file);
    // maybe make some files...
    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    reportResultStage.uploadOutputs(
        resultBuilder,
        root,
        ImmutableList.<String>of(),
        ImmutableList.<String>of("foo"));
    Tree tree = Tree.newBuilder()
        .setRoot(Directory.newBuilder()
            .addFiles(FileNode.newBuilder()
                .setName("bar")
                .setDigest(digestUtil.empty())
                .setIsExecutable(Files.isExecutable(file))
                .build())
            .build())
        .build();
    verify(mockUploader)
        .uploadBlobs(eq(ImmutableList.<Chunker>of(
            new Chunker(ByteString.EMPTY, digestUtil.empty()),
            new Chunker(tree.toByteString(), digestUtil.compute(tree)))));
    assertThat(resultBuilder.getOutputDirectoriesList()).containsExactly(
        OutputDirectory.newBuilder()
            .setPath("foo")
            .setTreeDigest(digestUtil.compute(tree))
            .build());
  }

  @Test
  public void uploadOutputsUploadsNestedDirectories()
      throws IOException, InterruptedException {
    Path topdir = root.resolve("foo");
    Files.createDirectory(topdir);
    Path subdir = topdir.resolve("bar");
    Files.createDirectory(subdir);
    Path file = subdir.resolve("baz");
    Files.createFile(file);
    // maybe make some files...
    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    reportResultStage.uploadOutputs(
        resultBuilder,
        root,
        ImmutableList.<String>of(),
        ImmutableList.<String>of("foo"));
    Directory subDirectory = Directory.newBuilder()
        .addFiles(FileNode.newBuilder()
            .setName("baz")
            .setDigest(digestUtil.empty())
            .setIsExecutable(Files.isExecutable(file))
            .build())
        .build();
    Tree tree = Tree.newBuilder()
        .setRoot(Directory.newBuilder()
            .addDirectories(DirectoryNode.newBuilder()
                .setName("bar")
                .setDigest(digestUtil.compute(subDirectory))
                .build())
            .build())
        .addChildren(subDirectory)
        .build();
    verify(mockUploader)
        .uploadBlobs(eq(ImmutableList.<Chunker>of(
            new Chunker(ByteString.EMPTY, digestUtil.empty()),
            new Chunker(tree.toByteString(), digestUtil.compute(tree)))));
    assertThat(resultBuilder.getOutputDirectoriesList()).containsExactly(
        OutputDirectory.newBuilder()
            .setPath("foo")
            .setTreeDigest(digestUtil.compute(tree))
            .build());
  }

  @Test
  public void uploadOutputsIgnoresMissingOutputDirectories()
      throws IOException, InterruptedException {
    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    reportResultStage.uploadOutputs(
        resultBuilder,
        root,
        ImmutableList.<String>of(),
        ImmutableList.<String>of("foo"));
    Tree emptyTree = Tree.newBuilder()
        .setRoot(Directory.getDefaultInstance())
        .build();
    verify(mockUploader, never())
        .uploadBlobs(any());
  }
}
