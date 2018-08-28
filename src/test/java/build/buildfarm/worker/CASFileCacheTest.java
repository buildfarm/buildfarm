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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

class CASFileCacheTest {
  private CASFileCache fileCache;
  private DigestUtil digestUtil;
  private Path root;
  private Map<Digest, ByteString> blobs;

  protected CASFileCacheTest(Path root) {
    this.root = root;
  }

  private static final class BrokenInputStream extends InputStream {
    private final IOException exception;

    public BrokenInputStream(IOException exception) {
      this.exception = exception;
    }

    @Override
    public int read() throws IOException {
      throw exception;
    }
  }

  @Before
  public void setUp() {
    digestUtil = new DigestUtil(HashFunction.SHA256);
    blobs = new HashMap<Digest, ByteString>();
    fileCache = new CASFileCache(
        new InputStreamFactory() {
          @Override
          public InputStream apply(Digest digest) {
            ByteString content = blobs.get(digest);
            if (content == null) {
              return new BrokenInputStream(new IOException("NOT_FOUND"));
            }
            return content.newInput();
          }
        },
        root,
        /* maxSizeInBytes=*/ 1024,
        digestUtil);
  }

  @Test
  public void putCreatesFile() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("Hello, World");
    Digest blobDigest = digestUtil.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, false);
    assertThat(Files.exists(path)).isTrue();
  }

  @Test(expected = IllegalStateException.class)
  public void putEmptyFileThrowsIllegalStateException() throws IOException, InterruptedException {
    InputStreamFactory mockInputStreamFactory = mock(InputStreamFactory.class);
    CASFileCache fileCache = new CASFileCache(
        mockInputStreamFactory,
        root,
        /* maxSizeInBytes=*/ 1024,
        digestUtil);

    ByteString blob = ByteString.copyFromUtf8("");
    Digest blobDigest = digestUtil.compute(blob);
    // supply an empty input stream if called for test clarity
    when(mockInputStreamFactory.apply(blobDigest))
        .thenReturn(ByteString.EMPTY.newInput());
    try {
      fileCache.put(blobDigest, false);
    } finally {
      verifyZeroInteractions(mockInputStreamFactory);
    }
  }

  @Test
  public void putCreatesExecutable() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("executable");
    Digest blobDigest = digestUtil.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, true);
    assertThat(Files.isExecutable(path)).isTrue();
  }

  @Test
  public void putDirectoryCreatesTree() throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = Digest.newBuilder()
        .setHash("file")
        .setSizeBytes(file.size())
        .build();
    blobs.put(fileDigest, file);
    Directory subDirectory = Directory.newBuilder().build();
    Digest subdirDigest = Digest.newBuilder().setHash("subdir").build();
    Directory directory = Directory.newBuilder()
        .addFiles(FileNode.newBuilder()
            .setName("file")
            .setDigest(fileDigest)
            .build())
        .addDirectories(DirectoryNode.newBuilder()
            .setName("subdir")
            .setDigest(subdirDigest)
            .build())
        .build();
    Digest dirDigest = Digest.newBuilder().setHash("test").build();
    Map<Digest, Directory> directoriesIndex = ImmutableMap.of(
        dirDigest, directory,
        subdirDigest, subDirectory);
    Path dirPath = fileCache.putDirectory(dirDigest, directoriesIndex);
    assertThat(Files.isDirectory(dirPath)).isTrue();
    assertThat(Files.exists(dirPath.resolve("file"))).isTrue();
    assertThat(Files.isDirectory(dirPath.resolve("subdir"))).isTrue();
  }

  @Test
  public void putDirectoryIOExceptionRollsBack() throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = Digest.newBuilder()
        .setHash("file")
        .setSizeBytes(file.size())
        .build();
    // omitting blobs.put to incur IOException
    Directory subDirectory = Directory.newBuilder().build();
    Digest subdirDigest = Digest.newBuilder().setHash("subdir").build();
    Directory directory = Directory.newBuilder()
        .addFiles(FileNode.newBuilder()
            .setName("file")
            .setDigest(fileDigest)
            .build())
        .addDirectories(DirectoryNode.newBuilder()
            .setName("subdir")
            .setDigest(subdirDigest)
            .build())
        .build();
    Digest dirDigest = Digest.newBuilder().setHash("test").build();
    Map<Digest, Directory> directoriesIndex = ImmutableMap.of(
        dirDigest, directory,
        subdirDigest, subDirectory);
    boolean exceptionHandled = false;
    try {
      fileCache.putDirectory(dirDigest, directoriesIndex);
    } catch (IOException e) {
      assertThat(e.getMessage()).isEqualTo("NOT_FOUND");
      exceptionHandled = true;
    }
    assertThat(exceptionHandled).isTrue();
    assertThat(Files.exists(fileCache.getDirectoryPath(dirDigest))).isFalse();
  }

  @Test
  public void expireUnreferencedEntryRemovesBlobFile() throws IOException, InterruptedException {
    byte[] bigData = new byte[1000];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = Digest.newBuilder()
        .setHash("big")
        .setSizeBytes(bigBlob.size())
        .build();
    blobs.put(bigDigest, bigBlob);
    Path bigPath = fileCache.put(bigDigest, false);

    fileCache.decrementReferences(ImmutableList.<Path>of(bigPath), ImmutableList.of());

    byte[] strawData = new byte[30]; // take us beyond our 1024 limit
    ByteString strawBlob = ByteString.copyFrom(strawData);
    Digest strawDigest = Digest.newBuilder()
        .setHash("straw")
        .setSizeBytes(strawBlob.size())
        .build();
    blobs.put(strawDigest, strawBlob);
    Path strawPath = fileCache.put(strawDigest, false);

    assertThat(Files.exists(bigPath)).isFalse();
    assertThat(Files.exists(strawPath)).isTrue();
  }

  @Test
  public void startLoadsExistingBlob() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = digestUtil.compute(blob);
    Path path = root.resolve(fileCache.getKey(blobDigest, false));
    Path execPath = root.resolve(fileCache.getKey(blobDigest, true));
    Files.write(path, blob.toByteArray());
    Files.write(execPath, blob.toByteArray());

    fileCache.start();

    // explicitly not providing blob via blobs, this would throw if fetched from factory
    //
    // FIXME https://github.com/google/truth/issues/285 assertThat(Path) is ambiguous
    assertThat(fileCache.put(blobDigest, false).equals(path)).isTrue();
    assertThat(fileCache.put(blobDigest, true).equals(execPath)).isTrue();
  }

  @Test
  public void startRemovesInvalidEntries() throws IOException, InterruptedException {
    Path tooFewComponents = root.resolve("toofewcomponents");
    Path tooManyComponents = root.resolve("too_many_components_here");
    Path invalidDigest = root.resolve("digest_10");
    ByteString validBlob = ByteString.copyFromUtf8("valid");
    Digest validDigest = digestUtil.compute(ByteString.copyFromUtf8("valid"));
    Path invalidSize = root.resolve(validDigest.getHash() + "_ten");
    Path incorrectSize = fileCache.getKey(validDigest
        .toBuilder()
        .setSizeBytes(validDigest.getSizeBytes() + 1)
        .build(), false);
    Path invalidExec = fileCache.getPath(CASFileCache.getFileName(validDigest, false) + "_regular");

    Files.write(tooFewComponents, ImmutableList.of("Too Few Components"), StandardCharsets.UTF_8);
    Files.write(tooManyComponents, ImmutableList.of("Too Many Components"), StandardCharsets.UTF_8);
    Files.write(invalidDigest, ImmutableList.of("Digest is not valid"), StandardCharsets.UTF_8);
    Files.write(invalidSize, validBlob.toByteArray()); // content would match but for size field
    Files.write(incorrectSize, validBlob.toByteArray()); // content would match but for size match
    Files.write(invalidExec, validBlob.toByteArray()); // content would match but for invalid exec field

    fileCache.start();

    assertThat(!Files.exists(tooFewComponents)).isTrue();
    assertThat(!Files.exists(tooManyComponents)).isTrue();
    assertThat(!Files.exists(invalidDigest)).isTrue();
    assertThat(!Files.exists(invalidSize)).isTrue();
    assertThat(!Files.exists(incorrectSize)).isTrue();
    assertThat(!Files.exists(invalidExec)).isTrue();
  }

  @Test
  public void removeDirectoryDeletesTree() throws IOException {
    Path tree = root.resolve("tree");
    Files.createDirectory(tree);
    Files.write(tree.resolve("file"), ImmutableList.of("Top level file"), StandardCharsets.UTF_8);
    Path subdir = tree.resolve("subdir");
    Files.createDirectory(subdir);
    Files.write(subdir.resolve("file"), ImmutableList.of("A file in a subdirectory"), StandardCharsets.UTF_8);

    CASFileCache.removeDirectory(tree);

    assertThat(Files.exists(tree)).isFalse();
  }
};
