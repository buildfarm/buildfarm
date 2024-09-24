// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.util;

import static build.buildfarm.worker.util.InputsIndexer.BAZEL_TOOL_INPUT_MARKER;
import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.NodeProperties;
import build.bazel.remote.execution.v2.NodeProperty;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.Tree;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

// TODO: use WorkerTestUtils.makeTree
@RunWith(JUnit4.class)
public class InputsIndexerTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  @Test
  public void basicEmptyTree() {
    Tree emptyTree = Tree.newBuilder().build();
    InputsIndexer indexer = new InputsIndexer(emptyTree, Paths.get("."));
    assertThat(indexer.tree).isEqualTo(emptyTree);
  }

  @Test
  public void canGetRootDir() {
    Tree.Builder treeBuilder = Tree.newBuilder();

    Directory rootDir = Directory.getDefaultInstance();
    Digest rootDirDigest = addDirToTree(treeBuilder, "my_root_dir", rootDir);
    treeBuilder.setRootDigest(rootDirDigest);

    Path arbitraryOpRoot = Paths.get(".");

    InputsIndexer indexer = new InputsIndexer(treeBuilder.build(), arbitraryOpRoot);
    assertThat(indexer.proxyDirs.get(DigestUtil.toDigest(rootDirDigest))).isEqualTo(rootDir);
    assertThat(indexer.getAllInputs().size()).isEqualTo(0);
  }

  @Test
  public void rootDirWithFiles() {
    Tree.Builder treeBuilder = Tree.newBuilder();

    FileNode myfile =
        makeFileNode("my_file", "my file contents", NodeProperties.getDefaultInstance());
    Directory rootDir = Directory.newBuilder().addFiles(myfile).build();
    Digest rootDirDigest = addDirToTree(treeBuilder, "my_root_dir", rootDir);
    treeBuilder.setRootDigest(rootDirDigest);

    Path arbitraryOpRoot = Paths.get("asdf");
    InputsIndexer indexer = new InputsIndexer(treeBuilder.build(), arbitraryOpRoot);
    assertThat(indexer.proxyDirs.get(DigestUtil.toDigest(rootDirDigest))).isEqualTo(rootDir);

    Input myfileInput = makeInput(arbitraryOpRoot, myfile);

    ImmutableMap<Path, Input> expectedInputs =
        ImmutableMap.of(Paths.get(myfileInput.getPath()), myfileInput);

    assertThat(indexer.getAllInputs()).isEqualTo(expectedInputs);
  }

  @Test
  public void canRecurseAndDistinguishToolInputs() {
    Tree.Builder treeBuilder = Tree.newBuilder();

    FileNode myfile =
        makeFileNode("my_file", "my file contents", NodeProperties.getDefaultInstance());
    FileNode subdirfile =
        makeFileNode("subdir_file", "my subdir file contents", NodeProperties.getDefaultInstance());
    FileNode toolfile =
        makeFileNode(
            "tool_file",
            "my tool file contents",
            makeNodeProperties(ImmutableMap.of(BAZEL_TOOL_INPUT_MARKER, "value doesn't matter")));

    Directory subDir = Directory.newBuilder().addFiles(subdirfile).build();
    String subDirName = "my_sub_dir";
    Digest subDirDigest = addDirToTree(treeBuilder, subDirName, subDir);

    Directory rootDir =
        Directory.newBuilder()
            .addFiles(myfile)
            .addFiles(toolfile)
            .addDirectories(makeDirNode(subDirName, DigestUtil.toDigest(subDirDigest)))
            .build();

    Digest rootDirDigest = addDirToTree(treeBuilder, "my_root_dir", rootDir);
    treeBuilder.setRootDigest(rootDirDigest);

    Path arbitraryOpRoot = Paths.get("asdf");

    InputsIndexer indexer = new InputsIndexer(treeBuilder.build(), arbitraryOpRoot);
    assertThat(indexer.proxyDirs.get(DigestUtil.toDigest(rootDirDigest))).isEqualTo(rootDir);
    assertThat(indexer.proxyDirs.size()).isEqualTo(2);

    Input myfileInput = makeInput(arbitraryOpRoot, myfile);
    Input subdirfileInput = makeInput(arbitraryOpRoot.resolve(subDirName), subdirfile);
    Input toolfileInput = makeInput(arbitraryOpRoot, toolfile);

    ImmutableMap<Path, Input> nonToolInputs =
        ImmutableMap.of(
            Paths.get(myfileInput.getPath()),
            myfileInput,
            Paths.get(subdirfileInput.getPath()),
            subdirfileInput);
    ImmutableMap<Path, Input> toolInputs =
        ImmutableMap.of(Paths.get(toolfileInput.getPath()), toolfileInput);
    ImmutableMap<Path, Input> allInputs =
        ImmutableMap.<Path, Input>builder().putAll(nonToolInputs).putAll(toolInputs).build();

    assertThat(indexer.getAllInputs()).isEqualTo(allInputs);
    assertThat(indexer.getAllInputs().size()).isEqualTo(3);
    assertThat(indexer.getToolInputs()).isEqualTo(toolInputs);
  }

  Digest addDirToTree(Tree.Builder treeBuilder, String dirname, Directory dir) {
    ByteString dirnameBytes = ByteString.copyFromUtf8(dirname);
    Digest digest = DIGEST_UTIL.compute(dirnameBytes);
    String hash = digest.getHash();
    treeBuilder.putDirectories(hash, dir);
    return digest;
  }

  FileNode makeFileNode(String filename, String content, NodeProperties nodeProperties) {
    return FileNode.newBuilder()
        .setName(filename)
        .setDigest(DigestUtil.toDigest(DIGEST_UTIL.compute(ByteString.copyFromUtf8(content))))
        .setIsExecutable(false)
        .setNodeProperties(nodeProperties)
        .build();
  }

  DirectoryNode makeDirNode(String dirname, build.bazel.remote.execution.v2.Digest dirDigest) {
    // Pretty sure we don't need the actual hash for our testing purposes
    return DirectoryNode.newBuilder().setName(dirname).setDigest(dirDigest).build();
  }

  NodeProperties makeNodeProperties(ImmutableMap<String, String> props) {
    return NodeProperties.newBuilder()
        .addAllProperties(
            props.entrySet().stream()
                .map(
                    kv ->
                        NodeProperty.newBuilder()
                            .setName(kv.getKey())
                            .setValue(kv.getValue())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  Input makeInput(Path fileDir, FileNode file) {
    Path fileNodePath = fileDir.resolve(file.getName());
    return Input.newBuilder()
        .setPath(fileNodePath.toString())
        .setDigest(file.getDigest().getHashBytes())
        .build();
  }
}
