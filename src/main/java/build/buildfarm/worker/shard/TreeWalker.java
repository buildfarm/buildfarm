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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.common.DigestPath;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.function.IOConsumer;
import build.buildfarm.v1test.Digest;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.logging.Level;
import lombok.extern.java.Log;

/** May be used multiple times, but not threadsafe */
@Log
class TreeWalker extends SimpleFileVisitor<Path> {
  private static final class OutputDirectoryContext {
    private final List<FileNode> files = new ArrayList<>();
    private final List<DirectoryNode> directories = new ArrayList<>();
    private final List<SymlinkNode> symlinks = new ArrayList<>();

    void addFile(FileNode fileNode) {
      files.add(fileNode);
    }

    void addDirectory(DirectoryNode directoryNode) {
      directories.add(directoryNode);
    }

    void addSymlink(SymlinkNode symlinkNode) {
      symlinks.add(symlinkNode);
    }

    Directory toDirectory() {
      files.sort(Comparator.comparing(FileNode::getName));
      directories.sort(Comparator.comparing(DirectoryNode::getName));
      symlinks.sort(Comparator.comparing(SymlinkNode::getName));
      return Directory.newBuilder()
          .addAllFiles(files)
          .addAllDirectories(directories)
          .addAllSymlinks(symlinks)
          .build();
    }
  }

  private final Stack<OutputDirectoryContext> path = new Stack<>();
  private final DigestUtil digestUtil;
  private final IOConsumer<DigestPath> fileObserver;
  private Tree.Builder treeBuilder = null;
  private OutputDirectoryContext currentDirectory = null;
  private Tree tree = null;
  private Path root = null;

  TreeWalker(DigestUtil digestUtil, IOConsumer<DigestPath> fileObserver) {
    this.digestUtil = digestUtil;
    this.fileObserver = fileObserver;
  }

  Tree getTree() {
    // only valid after peforming a walk
    return checkNotNull(tree);
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    if (attrs.isSymbolicLink()) {
      visitSymbolicLink(file);
    } else {
      visitRegularFile(file);
    }
    return FileVisitResult.CONTINUE;
  }

  private void visitSymbolicLink(Path file) throws IOException {
    // TODO convert symlinks with absolute targets within execution root to relative ones
    currentDirectory.addSymlink(
        SymlinkNode.newBuilder()
            .setName(file.getFileName().toString())
            .setTarget(Files.readSymbolicLink(file).toString())
            .build());
  }

  private void visitRegularFile(Path file) throws IOException {
    Digest digest;
    try {
      // should we create symlink nodes in output?
      // is buildstream trying to execute in a specific container??
      // can get to NSFE for nonexistent symlinks
      // can fail outright for a symlink to a directory
      digest = digestUtil.compute(file);
    } catch (NoSuchFileException e) {
      log.log(
          Level.SEVERE,
          format(
              "error visiting file %s under output dir %s",
              root.relativize(file), root.toAbsolutePath()),
          e);
      return;
    }

    // should we cast to PosixFilePermissions and do gymnastics there for executable?

    // TODO symlink per revision proposal
    currentDirectory.addFile(
        FileNode.newBuilder()
            .setName(file.getFileName().toString())
            .setDigest(DigestUtil.toDigest(digest))
            .setIsExecutable(Files.isExecutable(file))
            .build());
    fileObserver.accept(new DigestPath(digest, file));
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
    path.push(currentDirectory);
    if (currentDirectory == null) {
      // reset state when at root
      treeBuilder = Tree.newBuilder();
      root = dir;
    }
    currentDirectory = new OutputDirectoryContext();
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
    OutputDirectoryContext parentDirectory = path.pop();
    Directory directory = currentDirectory.toDirectory();
    if (parentDirectory == null) {
      treeBuilder.setRoot(directory);
      tree = treeBuilder.build();
      treeBuilder = null;
      root = null;
    } else {
      parentDirectory.addDirectory(
          DirectoryNode.newBuilder()
              .setName(dir.getFileName().toString())
              // FIXME make one digestUtil for all
              .setDigest(DigestUtil.toDigest(digestUtil.compute(directory)))
              .build());
      treeBuilder.addChildren(directory);
    }
    currentDirectory = parentDirectory;
    return FileVisitResult.CONTINUE;
  }
}
