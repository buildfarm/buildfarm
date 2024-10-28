// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.common.DigestPath;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.function.IOConsumer;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TreeWalkerTest {
  private final Path root = Iterables.getOnlyElement(Jimfs.newFileSystem().getRootDirectories());

  @Test
  public void treeShouldHaveHashedEmptyDirsInChildren() throws IOException {
    // ensure that we have a clean topdir
    Path treeRoot = root.resolve("tree_root");
    Files.createDirectory(treeRoot);
    Files.createDirectories(treeRoot.resolve("empty_subdir"));
    DigestUtil digestUtil = DigestUtil.forHash("BLAKE3");
    TreeWalker treeWalker =
        new TreeWalker(/* createSymlinkOutputs= */ false, digestUtil, digestPath -> {});
    Files.walkFileTree(treeRoot, treeWalker);
    Tree tree = treeWalker.getTree();
    DirectoryNode directoryNode = Iterables.getOnlyElement(tree.getRoot().getDirectoriesList());
    assertThat(directoryNode.getName()).isEqualTo("empty_subdir");
    Digest digest = digestUtil.toDigest(directoryNode.getDigest());
    assertThat(digestUtil.compute(Iterables.getOnlyElement(tree.getChildrenList())))
        .isEqualTo(digest);
    assertThat(digest).isEqualTo(digestUtil.empty());
    assertThat(digest.getHash().isEmpty()).isFalse();
  }

  @Test
  public void visitsSymlinksAsFilesWhenConfigured() throws IOException {
    DigestUtil digestUtil = DigestUtil.forHash("BLAKE3");
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = digestUtil.compute(content);

    IOConsumer<DigestPath> fileObserver = mock(IOConsumer.class);
    TreeWalker treeWalker =
        new TreeWalker(/* createSymlinkOutputs= */ false, digestUtil, fileObserver);
    Path treeRoot = root.resolve("tree_root");
    Files.createDirectory(treeRoot);
    Path filePath = treeRoot.resolve("file");
    try (OutputStream out = Files.newOutputStream(filePath)) {
      content.writeTo(out);
    }
    Path symlinkPath = treeRoot.resolve("symlink_to_file");
    Files.createSymbolicLink(symlinkPath, treeRoot.relativize(filePath));
    Files.walkFileTree(treeRoot, treeWalker);
    Tree tree = treeWalker.getTree();
    Directory rootDirectory = tree.getRoot();
    List<FileNode> files = rootDirectory.getFilesList();
    assertThat(files.size()).isEqualTo(2);
    assertThat(files.get(0).getDigest()).isEqualTo(files.get(1).getDigest());
    assertThat(files.get(0).getName()).isEqualTo("file");
    assertThat(files.get(1).getName()).isEqualTo("symlink_to_file");
    assertThat(rootDirectory.getSymlinksCount()).isEqualTo(0);
    verify(fileObserver, times(1)).accept(new DigestPath(digest, filePath));
    verify(fileObserver, times(1)).accept(new DigestPath(digest, symlinkPath));
  }

  @Test
  public void visitsSymlinksWhenConfigured() throws IOException {
    DigestUtil digestUtil = DigestUtil.forHash("BLAKE3");

    TreeWalker treeWalker =
        new TreeWalker(/* createSymlinkOutputs= */ true, digestUtil, digestPath -> {});
    Path treeRoot = root.resolve("tree_root");
    Files.createDirectory(treeRoot);
    Path filePath = treeRoot.resolve("file");
    Files.createSymbolicLink(treeRoot.resolve("symlink"), treeRoot.relativize(filePath));
    Files.walkFileTree(treeRoot, treeWalker);
    Tree tree = treeWalker.getTree();
    Directory rootDirectory = tree.getRoot();
    SymlinkNode symlink = Iterables.getOnlyElement(rootDirectory.getSymlinksList());
    assertThat(symlink.getName()).isEqualTo("symlink");
    assertThat(symlink.getTarget()).isEqualTo("file");
    assertThat(rootDirectory.getFilesCount()).isEqualTo(0);
  }

  @Test
  public void ignoresDeadSymlinksWhenConfigured() throws IOException {
    DigestUtil digestUtil = DigestUtil.forHash("BLAKE3");

    TreeWalker treeWalker =
        new TreeWalker(/* createSymlinkOutputs= */ false, digestUtil, digestPath -> {});
    Path treeRoot = root.resolve("tree_root");
    Files.createDirectory(treeRoot);
    Path filePath = treeRoot.resolve("file");
    Files.createSymbolicLink(treeRoot.resolve("dead_symlink"), treeRoot.relativize(filePath));
    Files.walkFileTree(treeRoot, treeWalker);
    Tree tree = treeWalker.getTree();
    Directory rootDirectory = tree.getRoot();
    assertThat(rootDirectory.getSymlinksCount()).isEqualTo(0);
    assertThat(rootDirectory.getFilesCount()).isEqualTo(0);
  }
}
