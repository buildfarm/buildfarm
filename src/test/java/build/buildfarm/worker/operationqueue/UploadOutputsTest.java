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

package build.buildfarm.worker.operationqueue;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.CASInsertionPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.naming.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class UploadOutputsTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  private final Configuration config;

  private FileSystem fileSystem;
  private Path root;
  private ActionResult.Builder resultBuilder;

  @Mock private ByteStreamUploader mockUploader;

  protected UploadOutputsTest(Configuration config) {
    this.config = config.toBuilder().setAttributeViews("posix").build();
  }

  @Before
  public void setUp() throws ConfigurationException {
    MockitoAnnotations.initMocks(this);

    fileSystem = Jimfs.newFileSystem(config);
    root = Iterables.getFirst(fileSystem.getRootDirectories(), null);

    resultBuilder = ActionResult.newBuilder();
  }

  @Test
  public void uploadOutputsUploadsEmptyOutputDirectories()
      throws IOException, StatusException, InterruptedException {
    Files.createDirectory(root.resolve("foo"));
    // maybe make some files...
    uploadOutputs(ImmutableList.<String>of(), ImmutableList.<String>of("foo"));
    Tree emptyTree = Tree.newBuilder().setRoot(Directory.getDefaultInstance()).build();
    ByteString emptyTreeBlob = emptyTree.toByteString();
    ArgumentCaptor<Map<HashCode, Chunker>> uploadCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUploader).uploadBlobs(uploadCaptor.capture());
    Map<HashCode, Chunker> upload = uploadCaptor.getValue();
    Chunker chunker = upload.get(DIGEST_UTIL.computeHash(emptyTreeBlob));
    assertThat(chunker.next().getData()).isEqualTo(emptyTreeBlob);
    assertThat(resultBuilder.getOutputDirectoriesList())
        .containsExactly(
            OutputDirectory.newBuilder()
                .setPath("foo")
                .setTreeDigest(DIGEST_UTIL.compute(emptyTree))
                .build());
  }

  @Test
  public void uploadOutputsUploadsFiles()
      throws IOException, StatusException, InterruptedException {
    Path topdir = root.resolve("foo");
    Files.createDirectory(topdir);
    Path file = topdir.resolve("bar");
    Files.createFile(file);
    // maybe make some files...
    uploadOutputs(ImmutableList.<String>of(), ImmutableList.<String>of("foo"));
    Tree tree =
        Tree.newBuilder()
            .setRoot(
                Directory.newBuilder()
                    .addFiles(
                        FileNode.newBuilder()
                            .setName("bar")
                            .setDigest(DIGEST_UTIL.empty())
                            .setIsExecutable(Files.isExecutable(file))
                            .build())
                    .build())
            .build();
    ByteString treeBlob = tree.toByteString();
    ArgumentCaptor<Map<HashCode, Chunker>> uploadCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUploader).uploadBlobs(uploadCaptor.capture());
    Map<HashCode, Chunker> upload = uploadCaptor.getValue();
    Chunker emptyChunker = upload.get(DIGEST_UTIL.computeHash(ByteString.EMPTY));
    assertThat(emptyChunker.next().getData()).isEqualTo(ByteString.EMPTY);
    Chunker treeChunker = upload.get(DIGEST_UTIL.computeHash(treeBlob));
    assertThat(treeChunker.next().getData()).isEqualTo(treeBlob);
    assertThat(resultBuilder.getOutputDirectoriesList())
        .containsExactly(
            OutputDirectory.newBuilder()
                .setPath("foo")
                .setTreeDigest(DIGEST_UTIL.compute(tree))
                .build());
  }

  @Test
  public void uploadOutputsUploadsNestedDirectories()
      throws IOException, StatusException, InterruptedException {
    Path topdir = root.resolve("foo");
    Files.createDirectory(topdir);
    Path subdir = topdir.resolve("bar");
    Files.createDirectory(subdir);
    Path file = subdir.resolve("baz");
    Files.createFile(file);
    // maybe make some files...
    uploadOutputs(ImmutableList.<String>of(), ImmutableList.<String>of("foo"));
    Directory subDirectory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("baz")
                    .setDigest(DIGEST_UTIL.empty())
                    .setIsExecutable(Files.isExecutable(file))
                    .build())
            .build();
    Tree tree =
        Tree.newBuilder()
            .setRoot(
                Directory.newBuilder()
                    .addDirectories(
                        DirectoryNode.newBuilder()
                            .setName("bar")
                            .setDigest(DIGEST_UTIL.compute(subDirectory))
                            .build())
                    .build())
            .addChildren(subDirectory)
            .build();
    ByteString treeBlob = tree.toByteString();
    ArgumentCaptor<Map<HashCode, Chunker>> uploadCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUploader).uploadBlobs(uploadCaptor.capture());
    Map<HashCode, Chunker> upload = uploadCaptor.getValue();
    Chunker emptyChunker = upload.get(DIGEST_UTIL.computeHash(ByteString.EMPTY));
    assertThat(emptyChunker.next().getData()).isEqualTo(ByteString.EMPTY);
    Chunker treeChunker = upload.get(DIGEST_UTIL.computeHash(treeBlob));
    assertThat(treeChunker.next().getData()).isEqualTo(treeBlob);
    assertThat(resultBuilder.getOutputDirectoriesList())
        .containsExactly(
            OutputDirectory.newBuilder()
                .setPath("foo")
                .setTreeDigest(DIGEST_UTIL.compute(tree))
                .build());
  }

  @Test
  public void uploadOutputsIgnoresMissingOutputDirectories()
      throws IOException, StatusException, InterruptedException {
    uploadOutputs(ImmutableList.<String>of(), ImmutableList.<String>of("foo"));
    Tree emptyTree = Tree.newBuilder().setRoot(Directory.getDefaultInstance()).build();
    verify(mockUploader, never()).uploadBlobs(any());
  }

  private void uploadOutputs(Iterable<String> files, Iterable<String> directories)
      throws IOException, InterruptedException {
    OperationQueueWorkerContext.uploadOutputs(
        resultBuilder,
        DIGEST_UTIL,
        root,
        files,
        directories,
        mockUploader,
        /* inlineContentLimit=*/ 0,
        CASInsertionPolicy.ALWAYS_INSERT,
        CASInsertionPolicy.ALWAYS_INSERT,
        CASInsertionPolicy.ALWAYS_INSERT);
  }

  @Test(expected = IllegalStateException.class)
  public void uploadOutputsThrowsIllegalStateExceptionWhenOutputFileIsDirectory()
      throws IOException, InterruptedException {
    Files.createDirectory(root.resolve("foo"));
    uploadOutputs(ImmutableList.<String>of("foo"), ImmutableList.<String>of());
  }

  @Test(expected = IllegalStateException.class)
  public void uploadOutputsThrowsIllegalStateExceptionWhenOutputDirectoryIsFile()
      throws IOException, InterruptedException {
    Files.createFile(root.resolve("foo"));
    uploadOutputs(ImmutableList.<String>of(), ImmutableList.<String>of("foo"));
  }
}
