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

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.cas.cfc.CASFileCache.CancellableOutputStream;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.cas.cfc.CASFileCache.StartupCacheResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.EvenMoreFiles;
import build.buildfarm.common.io.FeedbackOutputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class CASFileCacheTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private CASFileCache fileCache;
  private final Path root;
  private final boolean storeFileDirsIndexInMemory;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock private Consumer<Digest> onPut;

  @Mock private Consumer<Iterable<Digest>> onExpire;

  @Mock private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  private ConcurrentMap<String, Entry> storage;

  protected CASFileCacheTest(Path fileSystemRoot, boolean storeFileDirsIndexInMemory) {
    this.root = fileSystemRoot.resolve("cache");
    this.storeFileDirsIndexInMemory = storeFileDirsIndexInMemory;
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
        .findMissingBlobs(any(Iterable.class));
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    expireService = newSingleThreadExecutor();
    fileCache =
        new CASFileCache(
            root,
            /* maxSizeInBytes=*/ 1024,
            /* maxEntrySizeInBytes=*/ 1024,
            /* hexBucketLevels=*/ 1,
            storeFileDirsIndexInMemory,
            /* execRootFallback=*/ false,
            DIGEST_UTIL,
            expireService,
            /* accessRecorder=*/ directExecutor(),
            storage,
            /* directoriesIndexDbName=*/ ":memory:",
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad=*/ false) {
          @Override
          protected InputStream newExternalInput(
              Compressor.Value compressor, Digest digest, long offset) throws IOException {
            ByteString content = blobs.get(digest);
            if (content == null) {
              return fileCache.newTransparentInput(compressor, digest, offset);
            }
            checkArgument(compressor == Compressor.Value.IDENTITY);
            return content.substring((int) offset).newInput();
          }
        };
    // do this so that we can remove the cache root dir
    fileCache.initializeRootDirectory();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    // bazel appears to have a problem with us creating directories under
    // windows that are marked as no-delete. clean up after ourselves with
    // our utils
    Directories.remove(root, fileStore);
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
    }
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
  }

  @Test
  public void putCreatesFile() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("Hello, World");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, false);
    assertThat(Files.exists(path)).isTrue();
  }

  @Test(expected = IllegalStateException.class)
  public void putEmptyFileThrowsIllegalStateException() throws IOException, InterruptedException {
    InputStreamFactory mockInputStreamFactory = mock(InputStreamFactory.class);
    ByteString blob = ByteString.copyFromUtf8("");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    // supply an empty input stream if called for test clarity
    when(mockInputStreamFactory.newInput(Compressor.Value.IDENTITY, blobDigest, /* offset=*/ 0))
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
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, true);
    assertThat(Files.isExecutable(path)).isTrue();
  }

  @Test
  public void putDirectoryCreatesTree() throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);
    Directory subDirectory = Directory.getDefaultInstance();
    Digest subdirDigest = DIGEST_UTIL.compute(subDirectory);
    Directory directory =
        Directory.newBuilder()
            .addFiles(FileNode.newBuilder().setName("file").setDigest(fileDigest).build())
            .addDirectories(
                DirectoryNode.newBuilder().setName("subdir").setDigest(subdirDigest).build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            dirDigest, directory,
            subdirDigest, subDirectory);
    Path dirPath =
        getInterruptiblyOrIOException(
                fileCache.putDirectory(dirDigest, directoriesIndex, putService))
            .getPath();
    assertThat(Files.isDirectory(dirPath)).isTrue();
    assertThat(Files.exists(dirPath.resolve("file"))).isTrue();
    assertThat(Files.isDirectory(dirPath.resolve("subdir"))).isTrue();
  }

  @Test
  public void putDirectoryIOExceptionRollsBack() throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    // omitting blobs.put to incur IOException
    Directory subDirectory = Directory.getDefaultInstance();
    Digest subdirDigest = DIGEST_UTIL.compute(subDirectory);
    Directory directory =
        Directory.newBuilder()
            .addFiles(FileNode.newBuilder().setName("file").setDigest(fileDigest).build())
            .addDirectories(
                DirectoryNode.newBuilder().setName("subdir").setDigest(subdirDigest).build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            dirDigest, directory,
            subdirDigest, subDirectory);
    boolean exceptionHandled = false;
    try {
      getInterruptiblyOrIOException(
          fileCache.putDirectory(dirDigest, directoriesIndex, putService));
    } catch (PutDirectoryException e) {
      exceptionHandled = true;
    }
    assertThat(exceptionHandled).isTrue();
    assertThat(Files.exists(fileCache.getDirectoryPath(dirDigest))).isFalse();
  }

  @Test
  public void expireUnreferencedEntryRemovesBlobFile() throws IOException, InterruptedException {
    byte[] bigData = new byte[1000];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    Path bigPath = fileCache.put(bigDigest, false);

    decrementReference(bigPath);

    byte[] strawData = new byte[30]; // take us beyond our 1024 limit
    ByteString strawBlob = ByteString.copyFrom(strawData);
    Digest strawDigest = DIGEST_UTIL.compute(strawBlob);
    blobs.put(strawDigest, strawBlob);
    Path strawPath = fileCache.put(strawDigest, false);

    assertThat(Files.exists(bigPath)).isFalse();
    assertThat(Files.exists(strawPath)).isTrue();
  }

  @Test
  public void startEmptyCas() throws IOException, InterruptedException {
    // start the file cache with no files.
    // the cache should start without any initial files in the cache.
    StartupCacheResults results = fileCache.start(false);

    // check the startuo results to ensure no files were processed
    assertThat(results.load.loadSkipped).isFalse();
    assertThat(results.load.scan.computeDirs.size()).isEqualTo(0);
    assertThat(results.load.scan.deleteFiles.size()).isEqualTo(0);
    assertThat(results.load.scan.fileKeys.size()).isEqualTo(0);
    assertThat(results.load.invalidDirectories.size()).isEqualTo(0);
  }

  @Test
  public void startCasAssumeDirectory() throws IOException, InterruptedException {
    // create a "_dir" file on the root
    Path path = root.resolve("foobar_dir");
    ByteString blob = ByteString.copyFromUtf8("content");
    Files.write(path, blob.toByteArray());

    // start the CAS with a file whose name indicates its a directory
    // the cache should start and consider it a compute directory
    StartupCacheResults results = fileCache.start(false);

    // check the startup results to ensure no files were processed
    assertThat(results.load.loadSkipped).isFalse();
    assertThat(results.load.scan.computeDirs.size()).isEqualTo(0);
    assertThat(results.load.scan.deleteFiles.size()).isEqualTo(1);
    assertThat(results.load.scan.fileKeys.size()).isEqualTo(0);
    assertThat(results.load.invalidDirectories.size()).isEqualTo(0);
  }

  @Test
  public void startLoadsExistingBlob() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    Path path = fileCache.getPath(fileCache.getKey(blobDigest, false));
    Path execPath = fileCache.getPath(fileCache.getKey(blobDigest, true));
    Files.write(path, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path, false, fileStore);
    Files.write(execPath, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(execPath, true, fileStore);

    StartupCacheResults results = fileCache.start(false);

    // check the startup results to ensure our two files were processed
    assertThat(results.load.loadSkipped).isFalse();
    assertThat(results.load.scan.computeDirs.size()).isEqualTo(0);
    assertThat(results.load.scan.deleteFiles.size()).isEqualTo(0);
    assertThat(results.load.scan.fileKeys.size()).isEqualTo(2);
    assertThat(results.load.invalidDirectories.size()).isEqualTo(0);

    // explicitly not providing blob via blobs, this would throw if fetched from factory
    //
    // FIXME https://github.com/google/truth/issues/285 assertThat(Path) is ambiguous
    assertThat(fileCache.put(blobDigest, false).equals(path)).isTrue();
    assertThat(fileCache.put(blobDigest, true).equals(execPath)).isTrue();
  }

  @Test
  public void startSkipsLoadingExistingBlob() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    Path path = fileCache.getPath(fileCache.getKey(blobDigest, false));
    Path execPath = fileCache.getPath(fileCache.getKey(blobDigest, true));
    Files.write(path, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path, false, fileStore);
    Files.write(execPath, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(execPath, true, fileStore);

    StartupCacheResults results = fileCache.start(/* skipLoad=*/ true);

    // check the startup results to ensure our two files were processed
    assertThat(results.load.loadSkipped).isTrue();
    assertThat(results.load.scan.computeDirs.size()).isEqualTo(0);
    assertThat(results.load.scan.deleteFiles.size()).isEqualTo(0);
    assertThat(results.load.scan.fileKeys.size()).isEqualTo(0);
    assertThat(results.load.invalidDirectories.size()).isEqualTo(0);
  }

  @Test
  public void startRemovesInvalidEntries() throws IOException, InterruptedException {
    Path tooFewComponents = root.resolve("00").resolve("toofewcomponents");
    Path tooManyComponents = root.resolve("00").resolve("too_many_components_here");
    Path invalidDigest = root.resolve("00").resolve("digest");
    ByteString validBlob = ByteString.copyFromUtf8("valid");
    Digest validDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("valid"));
    Path invalidExec = fileCache.getPath(CASFileCache.getFileName(validDigest, false) + "_regular");

    Files.write(tooFewComponents, ImmutableList.of("Too Few Components"), StandardCharsets.UTF_8);
    Files.write(tooManyComponents, ImmutableList.of("Too Many Components"), StandardCharsets.UTF_8);
    Files.write(invalidDigest, ImmutableList.of("Digest is not valid"), StandardCharsets.UTF_8);
    Files.write(
        invalidExec, validBlob.toByteArray()); // content would match but for invalid exec field

    fileCache.start(/* skipLoad=*/ false);

    assertThat(!Files.exists(tooFewComponents)).isTrue();
    assertThat(!Files.exists(tooManyComponents)).isTrue();
    assertThat(!Files.exists(invalidDigest)).isTrue();
    assertThat(!Files.exists(invalidExec)).isTrue();
  }

  @Test
  public void newInputRemovesNonExistentEntry() throws IOException, InterruptedException {
    Digest nonexistentDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("file does not exist"));
    String nonexistentKey = fileCache.getKey(nonexistentDigest, false);
    Entry entry = new Entry(nonexistentKey, 1, Deadline.after(10, SECONDS));
    entry.before = entry;
    entry.after = entry;
    storage.put(nonexistentKey, entry);
    NoSuchFileException noSuchFileException = null;
    try (InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, nonexistentDigest, 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      noSuchFileException = e;
    }

    assertThat(noSuchFileException).isNotNull();
    assertThat(storage.containsKey(nonexistentKey)).isFalse();
  }

  @Test
  public void expireEntryWaitsForUnreferencedEntry()
      throws ExecutionException, IOException, InterruptedException {
    byte[] bigData = new byte[1023];
    Arrays.fill(bigData, (byte) 1);
    ByteString bigContent = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigContent);
    blobs.put(bigDigest, bigContent);
    Path bigPath = fileCache.put(bigDigest, /* isExecutable=*/ false);

    AtomicBoolean started = new AtomicBoolean(false);
    ExecutorService service = newSingleThreadExecutor();
    Future<Void> putFuture =
        service.submit(
            () -> {
              started.set(true);
              ByteString content = ByteString.copyFromUtf8("CAS Would Exceed Max Size");
              Digest digest = DIGEST_UTIL.compute(content);
              blobs.put(digest, content);
              fileCache.put(digest, /* isExecutable=*/ false);
              return null;
            });
    while (!started.get()) {
      MICROSECONDS.sleep(1);
    }
    // minimal test to ensure that we're blocked
    assertThat(putFuture.isDone()).isFalse();
    decrementReference(bigPath);
    try {
      putFuture.get();
    } finally {
      if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
        throw new RuntimeException("could not shut down service");
      }
    }
  }

  @Test
  public void containsRecordsAccess() throws IOException, InterruptedException {
    ByteString contentOne = ByteString.copyFromUtf8("one");
    Digest digestOne = DIGEST_UTIL.compute(contentOne);
    blobs.put(digestOne, contentOne);
    ByteString contentTwo = ByteString.copyFromUtf8("two");
    Digest digestTwo = DIGEST_UTIL.compute(contentTwo);
    blobs.put(digestTwo, contentTwo);
    ByteString contentThree = ByteString.copyFromUtf8("three");
    Digest digestThree = DIGEST_UTIL.compute(contentThree);
    blobs.put(digestThree, contentThree);

    String pathOne = fileCache.put(digestOne, /* isExecutable=*/ false).getFileName().toString();
    String pathTwo = fileCache.put(digestTwo, /* isExecutable=*/ false).getFileName().toString();
    String pathThree =
        fileCache.put(digestThree, /* isExecutable=*/ false).getFileName().toString();
    fileCache.decrementReferences(
        ImmutableList.of(pathOne, pathTwo, pathThree), ImmutableList.of());
    /* three -> two -> one */
    assertThat(storage.get(pathOne).after).isEqualTo(storage.get(pathTwo));
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));

    /* one -> three -> two */
    assertThat(fileCache.findMissingBlobs(ImmutableList.of(digestOne))).isEmpty();
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));
    assertThat(storage.get(pathThree).after).isEqualTo(storage.get(pathOne));
  }

  @Test
  public void mismatchedSizeIsNotContained() throws InterruptedException {
    ByteString content = ByteString.copyFromUtf8("mismatched");
    Blob blob = new Blob(content, DIGEST_UTIL);
    Digest digest = blob.getDigest();
    fileCache.put(blob);

    Digest mismatchedDigest = digest.toBuilder().setSizeBytes(digest.getSizeBytes() + 1).build();
    assertThat(fileCache.contains(digest, /* result=*/ null)).isTrue();
    assertThat(fileCache.contains(mismatchedDigest, /* result=*/ null)).isFalse();
  }

  @Test
  public void negativeSizeIsContainedAndPopulatesResult() throws InterruptedException {
    ByteString content = ByteString.copyFromUtf8("lookup");
    Blob blob = new Blob(content, DIGEST_UTIL);
    Digest digest = blob.getDigest();
    fileCache.put(blob);

    Digest.Builder result = Digest.newBuilder();
    Digest lookupDigest = digest.toBuilder().setSizeBytes(-1).build();
    assertThat(fileCache.contains(lookupDigest, result)).isTrue();
    assertThat(result.build()).isEqualTo(digest);
  }

  Write getWrite(Digest digest) throws IOException {
    return fileCache.getWrite(
        Compressor.Value.IDENTITY, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
  }

  @Test
  public void writeAddsEntry() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    AtomicBoolean notified = new AtomicBoolean(false);
    Write write = getWrite(digest);
    write.getFuture().addListener(() -> notified.set(true), directExecutor());
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    String key = fileCache.getKey(digest, false);
    assertThat(storage.get(key)).isNotNull();
    try (InputStream in = Files.newInputStream(fileCache.getPath(key))) {
      assertThat(ByteString.readFrom(in)).isEqualTo(content);
    }
  }

  @Test
  public void asyncWriteCompletionDischargesWriteSize() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write completingWrite = getWrite(digest);
    Write incompleteWrite = getWrite(digest);
    AtomicBoolean notified = new AtomicBoolean(false);
    // both should be size committed
    incompleteWrite.getFuture().addListener(() -> notified.set(true), directExecutor());
    OutputStream incompleteOut = incompleteWrite.getOutput(1, SECONDS, () -> {});
    try (OutputStream out = completingWrite.getOutput(1, SECONDS, () -> {})) {
      assertThat(fileCache.size()).isEqualTo(digest.getSizeBytes() * 2);
      content.writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    assertThat(fileCache.size()).isEqualTo(digest.getSizeBytes());
    assertThat(incompleteWrite.getCommittedSize()).isEqualTo(digest.getSizeBytes());
    assertThat(incompleteWrite.isComplete()).isTrue();
    incompleteOut.close(); // redundant
  }

  @Test
  public void cancelDischargesWriteSize() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write cancellingWrite = getWrite(digest);
    OutputStream out = cancellingWrite.getOutput(1, SECONDS, () -> {});
    assertThat(out).isInstanceOf(CancellableOutputStream.class);
    CancellableOutputStream cancelOut = (CancellableOutputStream) out;
    assertThat(fileCache.size()).isEqualTo(digest.getSizeBytes());
    cancelOut.cancel();
    assertThat(fileCache.size()).isEqualTo(0);
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(0);
    assertThat(cancellingWrite.isComplete()).isFalse();
  }

  @Test
  public void cancelNegatesProgressAndCanRestart() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write cancellingWrite = getWrite(digest);
    AtomicBoolean notified = new AtomicBoolean(false);
    cancellingWrite.getFuture().addListener(() -> notified.set(true), directExecutor());
    OutputStream out = cancellingWrite.getOutput(1, SECONDS, () -> {});
    assertThat(out).isInstanceOf(CancellableOutputStream.class);
    CancellableOutputStream cancelOut = (CancellableOutputStream) out;
    assertThat(fileCache.size()).isEqualTo(digest.getSizeBytes());
    content.substring(0, 6).writeTo(out);
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(6);
    assertThat(cancellingWrite.isComplete()).isFalse();
    cancelOut.cancel();
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(0);
    assertThat(cancellingWrite.isComplete()).isFalse();
    try (OutputStream restartedOut = cancellingWrite.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(restartedOut);
    }
    assertThat(notified.get()).isTrue();
    assertThat(fileCache.size()).isEqualTo(digest.getSizeBytes());
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(digest.getSizeBytes());
    assertThat(cancellingWrite.isComplete()).isTrue();
  }

  @Test
  public void incompleteWriteFileIsResumed() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    UUID writeId = UUID.randomUUID();
    String key = fileCache.getKey(digest, false);
    Path writePath = fileCache.getPath(key).resolveSibling(key + "." + writeId);
    try (OutputStream out = Files.newOutputStream(writePath)) {
      content.substring(0, 6).writeTo(out);
    }
    Write write =
        fileCache.getWrite(
            Compressor.Value.IDENTITY, digest, writeId, RequestMetadata.getDefaultInstance());
    AtomicBoolean notified = new AtomicBoolean(false);
    write.getFuture().addListener(() -> notified.set(true), directExecutor());
    assertThat(write.getCommittedSize()).isEqualTo(6);
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.substring(6, 9).writeTo(out);
    }
    // ensure that we can continue via a full call to getOutput
    assertThat(write.getCommittedSize()).isEqualTo(9);
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.substring(9).writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    assertThat(write.getCommittedSize()).isEqualTo(digest.getSizeBytes());
    assertThat(write.isComplete()).isTrue();
  }

  @Test
  public void writeOutputSynchronizesOnOutput() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    AtomicBoolean writeClosed = new AtomicBoolean(false);
    Write write = getWrite(digest);
    OutputStream out = write.getOutput(1, SECONDS, () -> {});
    // write is open and should block other output acquisition
    Thread closer =
        new Thread(
            () -> {
              try {
                MICROSECONDS.sleep(1);
                writeClosed.set(true);
                out.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    closer.start();
    try (OutputStream secondOut = write.getOutput(1, SECONDS, () -> {})) {
      assertThat(writeClosed.get()).isTrue();
    }
    write.reset(); // ensure that the output stream is closed
  }

  @Test
  public void writeOutputFutureIsSerialized() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write write = getWrite(digest);
    ListenableFuture<FeedbackOutputStream> firstOut = write.getOutputFuture(1, SECONDS, () -> {});
    ListenableFuture<FeedbackOutputStream> secondOut = write.getOutputFuture(1, SECONDS, () -> {});
    assertThat(firstOut.isDone()).isTrue();
    assertThat(secondOut.isDone()).isFalse();
    // close the first output
    firstOut.get().close();
    assertThat(secondOut.isDone()).isTrue();
    secondOut.get().close();
    write.reset(); // ensure that the output stream is closed
  }

  @Test(expected = DigestMismatchException.class)
  public void invalidContentThrowsDigestMismatch() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write write = getWrite(digest);
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      ByteString.copyFromUtf8("H3110, W0r1d").writeTo(out);
    }
  }

  @Test
  public void readRemovesNonexistentEntry() throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);

    fileCache.put(blob);
    String key = fileCache.getKey(blob.getDigest(), /* isExecutable=*/ false);
    // putCreatesFile verifies this
    Files.delete(fileCache.getPath(key));
    // update entry with expired deadline
    storage.get(key).existsDeadline = Deadline.after(0, SECONDS);

    try (InputStream in =
        fileCache.newInput(Compressor.Value.IDENTITY, blob.getDigest(), /* offset=*/ 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      // success
    }
    assertThat(storage.containsKey(key)).isFalse();
  }

  @Test
  public void emptyWriteIsComplete() throws IOException {
    Write write = getWrite(DIGEST_UTIL.compute(ByteString.EMPTY));
    assertThat(write.isComplete()).isTrue();
  }

  class UnsupportedWrite implements Write {
    @Override
    public long getCommittedSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isComplete() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FeedbackOutputStream getOutput(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Long> getFuture() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void expireInterruptCausesExpirySequenceHalt() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(1024)) {
      for (int i = 0; i < 1024; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
      fileCache.put(expiringBlob);
    }
    Digest expiringDigest = expiringBlob.getDigest();

    // set the delegate to throw interrupted on write output creation
    Write interruptingWrite =
        new UnsupportedWrite() {
          boolean canReset = false;

          @Override
          public FeedbackOutputStream getOutput(
              long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            canReset = true;
            throw new IOException(new InterruptedException());
          }

          @Override
          public void reset() {
            if (!canReset) {
              throw new UnsupportedOperationException();
            }
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(interruptingWrite);

    // FIXME we should have a guarantee that we did not iterate over another expiration
    InterruptedException sequenceException = null;
    try {
      fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));
      fail("should not get here");
    } catch (InterruptedException e) {
      sequenceException = e;
    }
    assertThat(sequenceException).isNotNull();

    verify(delegate, times(1))
        .getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class));
    assertThat(storage).isEmpty();
  }

  @Test
  public void delegateWriteCompleteIsNotAnError() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(1024)) {
      for (int i = 0; i < 1024; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
      fileCache.put(expiringBlob);
    }
    Digest expiringDigest = expiringBlob.getDigest();

    // set the delegate to throw on stream create, indicate write complete after
    Write completingWrite =
        new UnsupportedWrite() {
          boolean completed = false;

          @Override
          public FeedbackOutputStream getOutput(
              long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            completed = true;
            throw new IOException("indicates already complete");
          }

          @Override
          public boolean isComplete() {
            return completed;
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(completingWrite);

    Blob blob = new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL);
    fileCache.put(blob);

    verify(delegate, times(1))
        .getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class));
    assertThat(completingWrite.isComplete()).isTrue();
    assertThat(storage.keySet()).containsExactly(blob.getDigest().getHash());
  }

  void decrementReference(Path path) throws IOException, InterruptedException {
    fileCache.decrementReferences(
        ImmutableList.of(path.getFileName().toString()), ImmutableList.of());
  }

  @Test
  public void duplicateExpiredEntrySuppressesDigestExpiration()
      throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(512)) {
      for (int i = 0; i < 512; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());
    decrementReference(
        fileCache.put(expiringBlob.getDigest(), /* isExecutable=*/ false)); // expected eviction
    blobs.clear();
    decrementReference(
        fileCache.put(
            expiringBlob.getDigest(),
            /* isExecutable=*/ true)); // should be fed from storage directly, not through delegate

    fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));

    verifyZeroInteractions(onExpire);
    // assert expiration of non-executable digest
    String expiringKey = fileCache.getKey(expiringBlob.getDigest(), /* isExecutable=*/ false);
    assertThat(storage.containsKey(expiringKey)).isFalse();
    assertThat(Files.exists(fileCache.getPath(expiringKey))).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void interruptDeferredDuringExpirations() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(1024)) {
      for (int i = 0; i < 1024; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    fileCache.put(expiringBlob);
    // state of CAS
    //   1024-byte key

    AtomicReference<Throwable> exRef = new AtomicReference(null);
    // 0 = not blocking
    // 1 = blocking
    // 2 = delegate write
    AtomicInteger writeState = new AtomicInteger(0);
    // this will ensure that the discharge task is blocked until we release it
    Future<Void> blockingExpiration =
        expireService.submit(
            () -> {
              writeState.getAndIncrement();
              while (writeState.get() != 0) {
                try {
                  MICROSECONDS.sleep(1);
                } catch (InterruptedException e) {
                  // ignore
                }
              }
              return null;
            });
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringBlob.getDigest()),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(
            new NullWrite() {
              @Override
              public FeedbackOutputStream getOutput(
                  long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
                  throws IOException {
                try {
                  while (writeState.get() != 1) {
                    MICROSECONDS.sleep(1);
                  }
                } catch (InterruptedException e) {
                  throw new IOException(e);
                }
                writeState.getAndIncrement(); // move into output stream state
                return super.getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler);
              }
            });
    Thread expiringThread =
        new Thread(
            () -> {
              try {
                fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              fail("should not get here");
            });
    expiringThread.setUncaughtExceptionHandler((t, e) -> exRef.set(e));
    // wait for blocking state
    while (writeState.get() != 1) {
      MICROSECONDS.sleep(1);
    }
    expiringThread.start();
    while (writeState.get() != 2) {
      MICROSECONDS.sleep(1);
    }
    // expiry has been initiated, thread should be waiting
    MICROSECONDS.sleep(10); // just trying to ensure that we've reached the future wait point
    // hopefully this will be scheduled *after* the discharge task
    Future<Void> completedExpiration = expireService.submit(() -> null);
    // interrupt it
    expiringThread.interrupt();

    assertThat(expiringThread.isAlive()).isTrue();
    assertThat(completedExpiration.isDone()).isFalse();
    writeState.set(0);
    while (!blockingExpiration.isDone()) {
      MICROSECONDS.sleep(1);
    }
    expiringThread.join();
    // CAS should now be empty due to expiration and failed put
    while (!completedExpiration.isDone()) {
      MICROSECONDS.sleep(1);
    }
    assertThat(fileCache.size()).isEqualTo(0);
    Throwable t = exRef.get();
    assertThat(t).isNotNull();
    t = t.getCause();
    assertThat(t).isNotNull();
    assertThat(t).isInstanceOf(InterruptedException.class);
  }

  @Test
  public void readThroughSwitchesToLocalOnComplete() throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(0L)))
        .thenReturn(content.newInput());
    InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    // trigger the read through to complete immediately by supplying the blob
    fileCache.put(blob);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
    in.close();
  }

  @Test
  public void readThroughSwitchedToLocalContinues() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);
    ExecutorService service = newSingleThreadExecutor();
    SettableFuture<Void> writeComplete = SettableFuture.create();
    // we need to register callbacks on the shared write future
    Write write =
        new NullWrite() {
          @Override
          public ListenableFuture<Long> getFuture() {
            return Futures.transform(
                writeComplete, result -> blob.getDigest().getSizeBytes(), directExecutor());
          }

          @Override
          public FeedbackOutputStream getOutput(
              long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
            return new FeedbackOutputStream() {
              int offset = 0;

              @Override
              public void write(int b) {
                throw new UnsupportedOperationException();
              }

              @Override
              public void write(byte[] buf, int ofs, int len) throws IOException {
                // hangs on second read
                if (offset == 6) {
                  service.submit(() -> writeComplete.set(null));
                  throw new ClosedChannelException();
                }
                offset += len;
              }

              @Override
              public boolean isReady() {
                return true;
              }
            };
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(blob.getDigest()),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(write);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(0L)))
        .thenReturn(content.newInput());
    // the switch will reset to this point
    InputStream switchedIn = content.newInput();
    switchedIn.skip(6);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(6L)))
        .thenReturn(switchedIn);
    InputStream in =
        fileCache.newReadThroughInput(Compressor.Value.IDENTITY, blob.getDigest(), 0, write);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
    if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
      throw new RuntimeException("could not shut down service");
    }
  }

  @Test
  public void findMissingBlobsFiltersEmptyBlobs() throws Exception {
    Digest emptyDigest = Digest.getDefaultInstance();
    assertThat(fileCache.findMissingBlobs(ImmutableList.of(emptyDigest))).isEmpty();
  }

  @Test
  public void findMissingBlobsPopulatesUnknownSize() throws Exception {
    Blob blob = new Blob(ByteString.copyFromUtf8("content"), DIGEST_UTIL);
    Digest queryDigest = blob.getDigest().toBuilder().setSizeBytes(-1).build();
    Iterable<Digest> digests = ImmutableList.of(queryDigest);
    Digest responseDigest = Iterables.getOnlyElement(fileCache.findMissingBlobs(digests));
    assertThat(responseDigest).isEqualTo(queryDigest);

    // populate the digest
    fileCache.put(blob);

    responseDigest = Iterables.getOnlyElement(fileCache.findMissingBlobs(digests));
    assertThat(responseDigest).isEqualTo(blob.getDigest());
  }

  @Test
  public void copyExternalInputRetries() throws Exception {
    CASFileCache flakyExternalCAS =
        new CASFileCache(
            root,
            /* maxSizeInBytes=*/ 1024,
            /* maxEntrySizeInBytes=*/ 1024,
            /* hexBucketLevels=*/ 1,
            storeFileDirsIndexInMemory,
            /* execRootFallback=*/ false,
            DIGEST_UTIL,
            expireService,
            /* accessRecorder=*/ directExecutor(),
            storage,
            /* directoriesIndexDbName=*/ ":memory:",
            /* onPut=*/ digest -> {},
            /* onExpire=*/ digests -> {},
            /* delegate=*/ null,
            /* delegateSkipLoad=*/ false) {
          boolean throwUnavailable = true;

          @Override
          protected InputStream newExternalInput(
              Compressor.Value compressor, Digest digest, long offset) throws IOException {
            ByteString content = blobs.get(digest);
            if (throwUnavailable) {
              throwUnavailable = false;
              return new InputStream() {
                int count = 0;

                @Override
                public int read(byte[] buf) throws IOException {
                  return read(buf, 0, buf.length);
                }

                @Override
                public int read() {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int read(byte[] buf, int offset, int len) throws IOException {
                  if (count >= digest.getSizeBytes() / 2) {
                    throw new IOException(Status.UNAVAILABLE.asRuntimeException());
                  }
                  len = Math.min((int) digest.getSizeBytes() / 2 - count, len);
                  content.substring(count, count + len).copyTo(buf, offset);
                  count += len;
                  return len;
                }
              };
            }
            return content.substring((int) offset).newInput();
          }
        };
    flakyExternalCAS.initializeRootDirectory();
    ByteString blob = ByteString.copyFromUtf8("Flaky Entry");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = flakyExternalCAS.put(blobDigest, false);
    assertThat(Files.exists(path)).isTrue(); // would not have been created if not valid
  }

  @Test
  public void newInputThrowsNoSuchFileExceptionWithoutDelegate() throws Exception {
    ContentAddressableStorage undelegatedCAS =
        new CASFileCache(
            root,
            /* maxSizeInBytes=*/ 1024,
            /* maxEntrySizeInBytes=*/ 1024,
            /* hexBucketLevels=*/ 1,
            storeFileDirsIndexInMemory,
            /* execRootFallback=*/ false,
            DIGEST_UTIL,
            expireService,
            /* accessRecorder=*/ directExecutor(),
            storage,
            /* directoriesIndexDbName=*/ ":memory:",
            /* onPut=*/ digest -> {},
            /* onExpire=*/ digests -> {},
            /* delegate=*/ null,
            /* delegateSkipLoad=*/ false) {
          @Override
          protected InputStream newExternalInput(
              Compressor.Value compressor, Digest digest, long offset) throws IOException {
            ByteString content = blobs.get(digest);
            if (content == null) {
              return fileCache.newTransparentInput(compressor, digest, offset);
            }
            checkArgument(compressor == Compressor.Value.IDENTITY);
            return content.substring((int) offset).newInput();
          }
        };
    ByteString blob = ByteString.copyFromUtf8("Missing Entry");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    NoSuchFileException expected = null;
    try (InputStream in =
        undelegatedCAS.newInput(Compressor.Value.IDENTITY, blobDigest, /* offset=*/ 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      expected = e;
    }
    assertThat(expected).isNotNull();
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    ByteString blob = ByteString.copyFromUtf8("concurrent write");
    Digest digest = DIGEST_UTIL.compute(blob);
    UUID uuid = UUID.randomUUID();
    // The same instance of Write will be passed to both the threads, so that the both threads
    // try to get same output stream.
    Write write =
        fileCache.getWrite(
            Compressor.Value.IDENTITY, digest, uuid, RequestMetadata.getDefaultInstance());

    CyclicBarrier barrier = new CyclicBarrier(3);

    Thread write1 =
        new Thread(
            () -> {
              try {
                ConcurrentWriteStreamObserver writeStreamObserver =
                    new ConcurrentWriteStreamObserver(write);
                writeStreamObserver.registerCallback();
                barrier.await(); // let both the threads get same write stream.
                writeStreamObserver.ownStream(); // let other thread get the ownership of stream
                writeStreamObserver.write(blob);
                writeStreamObserver.close();
              } catch (Exception e) {
                // do nothing
              }
            },
            "FirstRequest");
    Thread write2 =
        new Thread(
            () -> {
              try {
                ConcurrentWriteStreamObserver writeStreamObserver =
                    new ConcurrentWriteStreamObserver(write);
                writeStreamObserver.registerCallback();
                writeStreamObserver.ownStream(); // this thread will get the ownership of stream
                barrier.await(); // let both the threads get same write stream.
                while (write1.getState() != WAITING) ; // wait for first request to go in wait state
                writeStreamObserver.write(blob);
                writeStreamObserver.close();
              } catch (Exception e) {
                // do nothing
              }
            },
            "SecondRequest");
    write1.start();
    write2.start();
    barrier.await(); // let both the requests reach the critical section

    // Wait for each write operation to complete, allowing a maximum of 100ms per write.
    // Note: A 100ms wait time allowed 1000 * 8 successful test runs.
    // In certain scenario, even this wait time may not be enough and test still be called flaky.
    // But setting wait time 0 may cause test to wait forever (if there is issue in code) and the
    // build might fail with timeout error.
    write1.join(100);
    write2.join(100);

    assertThat(write1.getState()).isEqualTo(TERMINATED);
    assertThat(write2.getState()).isEqualTo(TERMINATED);
  }

  static class ConcurrentWriteStreamObserver {
    Write write;
    FeedbackOutputStream out;

    ConcurrentWriteStreamObserver(Write write) {
      this.write = write;
    }

    void registerCallback() {
      Futures.addCallback(
          write.getFuture(),
          new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long committedSize) {
              commit();
            }

            @Override
            public void onFailure(Throwable t) {
              // do nothing
            }
          },
          directExecutor());
    }

    synchronized void ownStream() throws Exception {
      this.out = write.getOutput(10, MILLISECONDS, () -> {});
    }
    /**
     * Request 1 may invoke this method for request 2 or vice-versa via callback on
     * write.getFuture(). Synchronization is necessary to prevent conflicts when this method is
     * called simultaneously by different threads.
     */
    synchronized void commit() {
      // critical section
    }

    void write(ByteString data) throws IOException {
      data.writeTo(out);
    }

    void close() throws IOException {
      out.close();
    }
  }

  @RunWith(JUnit4.class)
  public static class NativeFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public NativeFileDirsIndexInMemoryCASFileCacheTest() throws IOException {
      super(createTempDirectory(), /* storeFileDirsIndexInMemory= */ true);
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-cas-test");
    }
  }

  @RunWith(JUnit4.class)
  public static class NativeFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public NativeFileDirsIndexInSqliteCASFileCacheTest() throws IOException {
      super(createTempDirectory(), /* storeFileDirsIndexInMemory= */ false);
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-cas-test");
    }
  }

  @RunWith(JUnit4.class)
  public static class OsXFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public OsXFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.osX()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  public static class OsXFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public OsXFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.osX()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }

  @RunWith(JUnit4.class)
  public static class UnixFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public UnixFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  public static class UnixFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public UnixFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }

  @RunWith(JUnit4.class)
  public static class WindowsFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public WindowsFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.windows()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "dos", "acl", "posix", "user")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  public static class WindowsFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public WindowsFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.windows()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "dos", "acl", "posix", "user")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }
}
