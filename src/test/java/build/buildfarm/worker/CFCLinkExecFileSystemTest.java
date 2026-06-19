// Copyright 2026 The Buildfarm Authors. All rights reserved.
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
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.PathResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CFCLinkExecFileSystemTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void createExecDirDoesNotLinkOutputPath() throws Exception {
    // larger scale test:
    //   validates that linkedDirectories ignores as requested
    //   ensures no symlink is created for a matching input directory (regardless of content) for
    //      an output_path
    String inputOutputPath = "path/to/input";
    Command command = Command.newBuilder().addOutputPaths(inputOutputPath).build();
    // path
    // |_ to
    //    |_ input <-- output_path
    //       |_ sub <-- should also be real
    //          |_ *file
    Digest fileDigest =
        DIGEST_UTIL.toDigest(
            build.bazel.remote.execution.v2.Digest.newBuilder()
                .setHash("file-hash")
                .setSizeBytes(1)
                .build());
    Directory subDirectory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setIsExecutable(true)
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest subDigest = DIGEST_UTIL.compute(subDirectory);
    Directory inputDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("sub")
                    .setDigest(DigestUtil.toDigest(subDigest))
                    .build())
            .build();
    Digest inputDigest = DIGEST_UTIL.compute(inputDirectory);
    Directory toDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("input")
                    .setDigest(DigestUtil.toDigest(inputDigest))
                    .build())
            .build();
    Digest toDigest = DIGEST_UTIL.compute(toDirectory);
    Directory pathDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("to")
                    .setDigest(DigestUtil.toDigest(toDigest))
                    .build())
            .build();
    Digest pathDigest = DIGEST_UTIL.compute(pathDirectory);
    Directory inputRootDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("path")
                    .setDigest(DigestUtil.toDigest(pathDigest))
                    .build())
            .build();
    Digest inputRootDigest = DIGEST_UTIL.compute(inputRootDirectory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(inputRootDigest), inputRootDirectory,
            DigestUtil.toDigest(pathDigest), pathDirectory,
            DigestUtil.toDigest(toDigest), toDirectory,
            DigestUtil.toDigest(inputDigest), inputDirectory,
            DigestUtil.toDigest(subDigest), subDirectory);
    Path root =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix().toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);
    Path fileEntryPath = root.resolve("cfc-entry-file");
    byte[] data = new byte[1];
    data[0] = 'f';
    Files.write(fileEntryPath, data);
    CASFileCache cfc = mock(CASFileCache.class);
    ExecutorService fetchService = newDirectExecutorService();
    when(cfc.put(fileDigest, true, fetchService))
        .thenReturn(
            immediateFuture(new PathResult(root.resolve("cfc-entry-file"), /* isMissed= */ false)));
    // should not be called, but supply a dir if we do so that we see the unexpected behavior below
    // in the symlink creation
    when(cfc.putDirectory(inputDigest, directoriesIndex, fetchService))
        .thenReturn(
            immediateFuture(
                new PathResult(root.resolve("cfc-entry-input"), /* isMissed= */ false)));
    when(cfc.putDirectory(subDigest, directoriesIndex, fetchService))
        .thenReturn(
            immediateFuture(new PathResult(root.resolve("cfc-entry-sub"), /* isMissed= */ false)));
    CFCLinkExecFileSystem efs =
        new CFCLinkExecFileSystem(
            root,
            cfc,
            ImmutableMap.of(),
            /* linkInputDirectories= */ true,
            ImmutableList.of(".*"),
            /* allowSymlinkTargetAbsolute= */ false,
            /* removeDirectoryService= */ null,
            /* accessRecorder= */ null,
            fetchService);
    Path execDir =
        efs.createExecDir(
            "outputPathDirOp",
            directoriesIndex,
            inputRootDigest,
            command,
            /* owner= */ null,
            WorkerExecutedMetadata.newBuilder());

    // first check: did we create "execDir/path/to/input" as a real dir?
    assertThat(Files.isDirectory(execDir.resolve(inputOutputPath))).isTrue();
    // second check: input/sub should also be a real dir
    assertThat(Files.isDirectory(execDir.resolve(inputOutputPath).resolve("sub"))).isTrue();
    // no symlinks up the chain
    assertThat(Files.isDirectory(execDir.resolve("path"))).isTrue();
    assertThat(Files.isDirectory(execDir.resolve("path/to"))).isTrue();
    // mock completionism
    verify(cfc, times(1)).put(fileDigest, true, fetchService);
    verifyNoMoreInteractions(cfc);
  }

  // DirectoryIterator constructor calls advance("") and then
  // unconditionally re-advances `current` in a trailing while loop, so the first
  // non-ignored root child gets skipped whenever there are >= 2 root children.
  // Concretely: the linked-directories pattern matches only the second root child,
  // not the first. If the first child were skipped, putDirectory would never be
  // requested for the second.
  @Test
  public void directoryIteratorDoesNotSkipFirstRootChild() throws Exception {
    // Two root children "a" and "b". Pattern matches only "b".
    // No output_paths, so ignorePaths is empty.
    Command command = Command.newBuilder().build();
    Digest aFileDigest =
        DIGEST_UTIL.toDigest(
            build.bazel.remote.execution.v2.Digest.newBuilder()
                .setHash("a-file-hash")
                .setSizeBytes(1)
                .build());
    Digest bFileDigest =
        DIGEST_UTIL.toDigest(
            build.bazel.remote.execution.v2.Digest.newBuilder()
                .setHash("b-file-hash")
                .setSizeBytes(1)
                .build());
    Directory aDirectory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("af")
                    .setIsExecutable(false)
                    .setDigest(DigestUtil.toDigest(aFileDigest))
                    .build())
            .build();
    Digest aDigest = DIGEST_UTIL.compute(aDirectory);
    Directory bDirectory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("bf")
                    .setIsExecutable(false)
                    .setDigest(DigestUtil.toDigest(bFileDigest))
                    .build())
            .build();
    Digest bDigest = DIGEST_UTIL.compute(bDirectory);
    Directory inputRootDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(aDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(bDigest))
                    .build())
            .build();
    Digest inputRootDigest = DIGEST_UTIL.compute(inputRootDirectory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(inputRootDigest), inputRootDirectory,
            DigestUtil.toDigest(aDigest), aDirectory,
            DigestUtil.toDigest(bDigest), bDirectory);
    Path root =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix().toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);
    Path afEntry = root.resolve("cfc-entry-af");
    Files.write(afEntry, new byte[] {'a'});
    Path bfEntry = root.resolve("cfc-entry-bf");
    Files.write(bfEntry, new byte[] {'b'});
    CASFileCache cfc = mock(CASFileCache.class);
    ExecutorService fetchService = newDirectExecutorService();
    when(cfc.put(aFileDigest, false, fetchService))
        .thenReturn(immediateFuture(new PathResult(afEntry, /* isMissed= */ false)));
    when(cfc.put(bFileDigest, false, fetchService))
        .thenReturn(immediateFuture(new PathResult(bfEntry, /* isMissed= */ false)));
    when(cfc.putDirectory(aDigest, directoriesIndex, fetchService))
        .thenReturn(
            immediateFuture(new PathResult(root.resolve("cfc-entry-a"), /* isMissed= */ false)));
    when(cfc.putDirectory(bDigest, directoriesIndex, fetchService))
        .thenReturn(
            immediateFuture(new PathResult(root.resolve("cfc-entry-b"), /* isMissed= */ false)));
    CFCLinkExecFileSystem efs =
        new CFCLinkExecFileSystem(
            root,
            cfc,
            ImmutableMap.of(),
            /* linkInputDirectories= */ true,
            // Match only "a" (the first root child). If the iterator skips "a", the
            // linkedDirectories set will be empty and "a" will be created as a real dir
            // instead of a symlink.
            ImmutableList.of("a"),
            /* allowSymlinkTargetAbsolute= */ false,
            /* removeDirectoryService= */ null,
            /* accessRecorder= */ null,
            fetchService);
    efs.createExecDir(
        "iteratorSkipOp",
        directoriesIndex,
        inputRootDigest,
        command,
        /* owner= */ null,
        WorkerExecutedMetadata.newBuilder());

    // "a" matched the linked-directories pattern; it should have been linked via
    // putDirectory. If the constructor double-advance bug skipped "a", this fails.
    verify(cfc, times(1)).putDirectory(aDigest, directoriesIndex, fetchService);
  }

  @Test
  public void directoryIteratorAdvancesPastEmptyDirectory() {
    Digest emptyDigest = DIGEST_UTIL.compute(Directory.getDefaultInstance());
    Directory nonEmptyDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("child")
                    .setDigest(DigestUtil.toDigest(emptyDigest))
                    .build())
            .build();
    Digest nonEmptyDigest = DIGEST_UTIL.compute(nonEmptyDirectory);
    Directory inputRootDirectory =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("empty")
                    .setDigest(DigestUtil.toDigest(emptyDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("nonempty")
                    .setDigest(DigestUtil.toDigest(nonEmptyDigest))
                    .build())
            .build();
    Iterator<String> dirs =
        CFCLinkExecFileSystem.directoriesIterator(
            inputRootDirectory,
            ImmutableMap.of(DigestUtil.toDigest(nonEmptyDigest), nonEmptyDirectory),
            ImmutableSet.of());

    assertThat(dirs.next()).isEqualTo("empty");
    assertThat(dirs.next()).isEqualTo("nonempty");
    assertThat(dirs.next()).isEqualTo("nonempty/child");
    assertThat(dirs.hasNext()).isFalse();
  }
}
