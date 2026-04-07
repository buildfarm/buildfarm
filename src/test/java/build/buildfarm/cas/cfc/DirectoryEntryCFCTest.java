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

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class DirectoryEntryCFCTest {
  protected static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  protected CASFileCache fileCache;
  private final Path root;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock private Consumer<Digest> onPut;

  @Mock private Consumer<Iterable<Digest>> onExpire;

  @Mock private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  protected ConcurrentMap<String, Entry> storage;

  protected DirectoryEntryCFCTest(Path fileSystemRoot) {
    this.root = fileSystemRoot.resolve("cache");
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    when(delegate.newInput(any(Compressor.Value.class), any(Digest.class), any(Long.class)))
        .thenThrow(new NoSuchFileException("null sink delegate"));
    doAnswer(
            (Answer<Iterable<Digest>>)
                invocation -> {
                  return (Iterable<Digest>) invocation.getArguments()[0];
                })
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    expireService = newSingleThreadExecutor();
    fileCache =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              if (content == null) {
                return fileCache.newTransparentInput(compressor, digest, offset);
              }
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
    fileCache.initializeRootDirectory();
    // Force a known block size so tests are hermetic and don't depend on the host filesystem.
    fileCache.setBlockSizeForTesting(4096);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
    }
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
    Directories.remove(root, fileStore);
  }

  @Test
  public void estimateDirectorySizeOnDisk_emptyDirectory_returnsOneBlock() {
    Directory emptyDir = Directory.getDefaultInstance();
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(emptyDir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_fewEntries_returnsOneBlock() {
    Directory dir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file1")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .build();
    // 2 entries * 32 bytes = 64 bytes, fits in one 4096-byte block
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_manyEntries_scalesWithEntryCount() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 200; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    // 200 entries * 32 bytes = 6400 bytes, needs 2 blocks of 4096
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  @Test
  public void estimateDirectorySizeOnDisk_countsAllEntryTypes() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 80; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addDirectories(
          DirectoryNode.newBuilder()
              .setName("dir" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addSymlinks(SymlinkNode.newBuilder().setName("link" + i).setTarget("target").build());
    }
    // 200 total entries * 32 bytes = 6400 bytes, needs 2 blocks
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  // -- estimateSizeOnDisk tests --

  @Test
  public void estimateSizeOnDisk_zeroSize_returnsZero() {
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ false)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_oneByte_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(1, 4096, /* isHardlink= */ false)).isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_justUnderOneBlock_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(4095, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_exactlyOneBlock_returnsOneBlock() {
    // Idempotent: an already-aligned value should not round up to the next block.
    assertThat(CASFileCache.estimateSizeOnDisk(4096, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_oneByteOverBlock_returnsTwoBlocks() {
    assertThat(CASFileCache.estimateSizeOnDisk(4097, 4096, /* isHardlink= */ false))
        .isEqualTo(8192);
  }

  @Test
  public void estimateSizeOnDisk_negativeSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(-1, 4096, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_differentBlockSizes() {
    assertThat(CASFileCache.estimateSizeOnDisk(100, 512, /* isHardlink= */ false)).isEqualTo(512);
    assertThat(CASFileCache.estimateSizeOnDisk(100, 1, /* isHardlink= */ false)).isEqualTo(100);
  }

  @Test
  public void estimateSizeOnDisk_invalidBlockSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, 0, /* isHardlink= */ false));
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, -1, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_isIdempotent() {
    // Applying estimateSizeOnDisk twice should give the same result as applying it once.
    // This matters because directory Entry.size values are pre-aligned, and discharge()
    // applies estimateSizeOnDisk again.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ false);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_returnsZero() {
    // Hardlinks reuse an existing inode's blocks, so the physical cost is ~0 regardless of
    // logical size or block size.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 1L << 20};
    long[] blockSizes = {1, 512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        assertThat(CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true))
            .isEqualTo(0);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_zeroSize_returnsZero() {
    // Zero-size boundary with the hardlink branch still returns 0.
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ true)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_largeSize_returnsZero() {
    // Integer.MAX_VALUE-scale sizes with the hardlink branch still return 0 and do not overflow.
    assertThat(CASFileCache.estimateSizeOnDisk(Integer.MAX_VALUE, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
    assertThat(
            CASFileCache.estimateSizeOnDisk(
                (long) Integer.MAX_VALUE + 1, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_isIdempotent() {
    // Applying estimateSizeOnDisk twice with isHardlink=true still yields 0, mirroring the
    // existing estimateSizeOnDisk_isIdempotent test.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ true);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void directoryChargeDischargeSizeReturnsToZero() throws IOException, InterruptedException {
    // Put a directory, verify it charges sizeInBytes, then evict it and verify size returns to
    // zero.
    ByteString file = ByteString.copyFromUtf8("Hello Directory");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    long sizeAfterPut = fileCache.size();
    assertThat(sizeAfterPut).isGreaterThan(0);

    // Dereference the directory entry so it becomes evictable.
    fileCache.decrementReferences(
        ImmutableList.of(),
        ImmutableList.of(DigestUtil.toDigest(dirDigest)),
        DIGEST_UTIL.getDigestFunction());

    // Put a large blob that forces eviction of the directory entry.
    byte[] bigData = new byte[(int) (fileCache.maxSize() - 100)];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    fileCache.put(bigDigest, false);

    // After eviction and new charge, size should exactly equal the big blob's on-disk size —
    // no drift from asymmetric charge/discharge of the directory entry.
    assertThat(fileCache.size())
        .isEqualTo(CASFileCache.estimateSizeOnDisk(bigData.length, 4096, /* isHardlink= */ false));
  }

  @Test
  public void putDirectoryIncludesDirectoryOverheadInSize()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory subDirectory = Directory.getDefaultInstance();
    Digest subdirDigest = DIGEST_UTIL.compute(subDirectory);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(subdirDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(dirDigest), directory,
            DigestUtil.toDigest(subdirDigest), subDirectory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    long blobSize = file.size();
    // DirectoryEntryCFC copies files into _dir entries, so the blob content contributes to
    // sizeInBytes twice: once as the CAS blob entry, once as the copy within the directory entry.
    // Without the directory overhead fix, size() would equal 2 * blobSize.
    // With the fix, size() should include the estimated directory overhead for 2 directories
    // (the root dir and the empty subdir), making it greater than 2 * blobSize.
    assertThat(fileCache.size()).isGreaterThan(2 * blobSize);
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class JimfsDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    public JimfsDirectoryEntryCFCTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }

  // Native filesystem test variant (needed for computeDirectory which uses real dir sizes)
  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class NativeDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    private final Path tempDir;

    public NativeDirectoryEntryCFCTest() throws IOException {
      this(createTempDirectory());
    }

    private NativeDirectoryEntryCFCTest(Path tempDir) {
      super(tempDir);
      this.tempDir = tempDir;
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-dir-entry-cfc-test");
    }

    @After
    @Override
    public void tearDown() throws IOException, InterruptedException {
      super.tearDown();
      Files.deleteIfExists(tempDir);
    }

    @Test
    public void computeDirectoryIncludesDirectoryOverhead() throws Exception {
      byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
      // The key must go through the entry path strategy (hex bucket directories).
      String dirKey = fileCache.getDirectoryKey(DIGEST_UTIL.compute(ByteString.copyFrom(content)));
      Path dirEntry = fileCache.getPath(dirKey);
      Files.createDirectories(dirEntry.resolve("subdir"));
      Files.write(dirEntry.resolve("file.txt"), content);

      getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

      Entry entry = storage.get(dirKey);
      assertThat(entry).isNotNull();
      // On a real filesystem, directory attrs.size() returns at least one block (typically 4096).
      // The entry size should be greater than just the file content because it includes
      // the directory overhead for both the root _dir directory and the "subdir" subdirectory.
      assertThat(entry.size).isGreaterThan((long) content.length);
    }
  }
}
