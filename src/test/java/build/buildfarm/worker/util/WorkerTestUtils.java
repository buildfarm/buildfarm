package build.buildfarm.worker.util;

import static build.buildfarm.worker.util.InputsIndexer.BAZEL_TOOL_INPUT_MARKER;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.NodeProperties;
import build.bazel.remote.execution.v2.NodeProperty;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Tree;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkerTestUtils {
  public static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  public static FileNode makeFileNode(
      String filename, String content, NodeProperties nodeProperties) {
    return FileNode.newBuilder()
        .setName(filename)
        .setDigest(DIGEST_UTIL.compute(ByteString.copyFromUtf8(content)))
        .setIsExecutable(false)
        .setNodeProperties(nodeProperties)
        .build();
  }

  public static DirectoryNode makeDirNode(String dirname, Digest dirDigest) {
    // Pretty sure we don't need the actual hash for our testing purposes
    return DirectoryNode.newBuilder().setName(dirname).setDigest(dirDigest).build();
  }

  public static Digest addDirToTree(Tree.Builder treeBuilder, String dirname, Directory dir) {
    ByteString dirnameBytes = ByteString.copyFromUtf8(dirname);
    Digest digest = DIGEST_UTIL.compute(dirnameBytes);
    String hash = digest.getHash();
    treeBuilder.putDirectories(hash, dir);
    return digest;
  }

  public static NodeProperties makeNodeProperties(ImmutableMap<String, String> props) {
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

  public static Input makeInput(Path fileDir, FileNode file) {
    Path fileNodePath = fileDir.resolve(file.getName());
    return Input.newBuilder()
        .setPath(fileNodePath.toString())
        .setDigest(file.getDigest().getHashBytes())
        .build();
  }

  public static Command makeCommand() {
    ImmutableList<String> outputFiles = ImmutableList.of("output_file", "out_subdir/out_subfile");
    ImmutableList<String> outputDirs = ImmutableList.of("out_subdir");
    ImmutableList<String> outputPaths =
        ImmutableList.<String>builder().addAll(outputFiles).addAll(outputDirs).build();

    return Command.newBuilder()
        .addAllOutputFiles(outputFiles)
        .addAllOutputDirectories(outputDirs)
        .addAllOutputPaths(outputPaths)
        .build();
  }

  public static class TreeFile {
    public final String path;
    public final boolean isTool;

    // null means directory
    public final String content;

    public TreeFile(String path) {
      this(path, "", false);
    }

    public TreeFile(String path, String content) {
      this(path, content, false);
    }

    public TreeFile(String path, String content, boolean isTool) {
      this.path = path;
      this.isTool = isTool;
      this.content = content;
    }

    public boolean isDir() {
      return this.content == null;
    }

    public String name() {
      return Paths.get(this.path).getFileName().toString();
    }
  }

  public static Tree makeTree(String rootDirPath, List<TreeFile> files) {
    Tree.Builder treeBuilder = Tree.newBuilder();
    if (files.isEmpty()) {
      return treeBuilder.build();
    }
    Directory.Builder rootDirBuilder = Directory.newBuilder();

    Map<String, Directory.Builder> dirBuilders = new HashMap<>();

    for (TreeFile file : files) {
      if (file.isDir()) {
        dirBuilders.computeIfAbsent(file.path, (filePath) -> Directory.newBuilder());
      } else {
        NodeProperties props = NodeProperties.getDefaultInstance();
        if (file.isTool) {
          props = makeNodeProperties(ImmutableMap.of(BAZEL_TOOL_INPUT_MARKER, ""));
        }
        FileNode fileNode = makeFileNode(file.name(), file.content, props);
        Path parentDirPath = Paths.get(file.path).getParent();
        if (parentDirPath != null) {
          String parentDirPathStr = parentDirPath.normalize().toString();
          Directory.Builder parentDirBuilder =
              dirBuilders.computeIfAbsent(parentDirPathStr, (filePath) -> Directory.newBuilder());
          parentDirBuilder.addFiles(fileNode);
        } else {
          rootDirBuilder.addFiles(fileNode);
        }
      }
    }

    for (Map.Entry<String, Directory.Builder> entry : dirBuilders.entrySet()) {
      String subDirName = entry.getKey();
      Directory subDir = entry.getValue().build();
      Digest subDirDigest = addDirToTree(treeBuilder, subDirName, subDir);
      rootDirBuilder.addDirectories(makeDirNode(subDirName, subDirDigest));
    }

    Digest rootDirDigest = addDirToTree(treeBuilder, rootDirPath, rootDirBuilder.build());
    treeBuilder.setRootDigest(rootDirDigest);

    return treeBuilder.build();
  }

  public static List<Path> listFilesRec(Path root) throws IOException {
    List<Path> filesFound = new ArrayList<>();

    Files.walkFileTree(
        root,
        new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            filesFound.add(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            throw new IOException("visitFileFailed");
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            filesFound.add(dir);
            return FileVisitResult.CONTINUE;
          }
        });

    return filesFound;
  }

  // Check all expected files exist and that only they exist
  public static void assertFilesExistExactly(Path root, List<Path> expectedFiles)
      throws IOException {
    List<Path> listedPaths = listFilesRec(root);
    for (Path filePath : listedPaths) {
      assertWithMessage("Path not match prefix of any expected file: " + filePath)
          .that(expectedFiles.stream().anyMatch(p -> p.startsWith(p)))
          .isTrue();
    }
    assertThat(listedPaths).containsAtLeastElementsIn(expectedFiles);
  }
}
