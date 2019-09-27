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

import static build.buildfarm.worker.CASFileCache.getInterruptiblyOrIOException;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.worker.CASFileCache.Entry;
import build.buildfarm.worker.CASFileCache.PutDirectoryException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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

class CASFileCacheTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private CASFileCache fileCache;
  private Path root;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock
  private Consumer<Digest> onPut;

  @Mock
  private Consumer<Iterable<Digest>> onExpire;

  @Mock
  private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  private ConcurrentMap<Path, Entry> storage;

  protected CASFileCacheTest(Path root) {
    this.root = root;
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(delegate.getWrite(any(Digest.class), any(UUID.class), any(RequestMetadata.class))).thenReturn(new NullWrite());
    when(delegate.newInput(any(Digest.class), any(Long.class))).thenThrow(new NoSuchFileException("null sink delegate"));
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    expireService = newSingleThreadExecutor();
    fileCache = new CASFileCache(
        root,
        /* maxSizeInBytes=*/ 1024,
        /* maxEntrySizeInBytes=*/ 1024,
        DIGEST_UTIL,
        expireService,
        /* accessRecorder=*/ directExecutor(),
        storage,
        onPut,
        onExpire,
        delegate) {
      @Override
      protected InputStream newExternalInput(Digest digest, long offset) throws IOException {
        ByteString content = blobs.get(digest);
        if (content == null) {
          return fileCache.newTransparentInput(digest, offset);
        }
        return content.substring((int) offset).newInput();
      }
    };
  }

  @After
  public void tearDown() throws InterruptedException {
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
    CASFileCache fileCache = new CASFileCache(
        root,
        /* maxSizeInBytes=*/ 1024,
        /* maxEntrySizeInBytes=*/ 1024,
        DIGEST_UTIL,
        /* expireService=*/ newDirectExecutorService(),
        /* accessRecorder=*/ directExecutor()) {
      @Override
      protected InputStream newExternalInput(Digest digest, long offset) throws IOException, InterruptedException {
        return mockInputStreamFactory.newInput(digest, offset);
      }
    };

    ByteString blob = ByteString.copyFromUtf8("");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    // supply an empty input stream if called for test clarity
    when(mockInputStreamFactory.newInput(blobDigest, /* offset=*/ 0))
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
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<Digest, Directory> directoriesIndex = ImmutableMap.of(
        dirDigest, directory,
        subdirDigest, subDirectory);
    Path dirPath = getInterruptiblyOrIOException(
        fileCache.putDirectory(dirDigest, directoriesIndex, putService));
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
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<Digest, Directory> directoriesIndex = ImmutableMap.of(
        dirDigest, directory,
        subdirDigest, subDirectory);
    boolean exceptionHandled = false;
    try {
      getInterruptiblyOrIOException(
          fileCache.putDirectory(
              dirDigest,
              directoriesIndex,
              putService));
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
  public void startLoadsExistingBlob() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
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
    Digest validDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("valid"));
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
  public void newInputRemovesNonExistentEntry() throws IOException, InterruptedException {
    Digest nonexistentDigest = Digest.newBuilder()
        .setHash("file_does_not_exist")
        .setSizeBytes(1)
        .build();
    Path nonexistentKey = fileCache.getKey(nonexistentDigest, false);
    Entry entry = new Entry(nonexistentKey, 1, null, Deadline.after(10, SECONDS));
    entry.before = entry;
    entry.after = entry;
    storage.put(nonexistentKey, entry);
    NoSuchFileException noSuchFileException = null;
    try (InputStream in = fileCache.newInput(nonexistentDigest, 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      noSuchFileException = e;
    }

    assertThat(noSuchFileException).isNotNull();
    assertThat(storage.containsKey(nonexistentKey)).isFalse();
  }

  @Test
  public void expireEntryWaitsForUnreferencedEntry() throws ExecutionException, IOException, InterruptedException {
    byte[] bigData = new byte[1023];
    Arrays.fill(bigData, (byte) 1);
    ByteString bigContent = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigContent);
    blobs.put(bigDigest, bigContent);
    Path bigPath = fileCache.put(bigDigest, /* isExecutable=*/ false);

    AtomicBoolean started = new AtomicBoolean(false);
    ExecutorService service = newSingleThreadExecutor();
    Future<Void> putFuture = service.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException, InterruptedException {
        started.set(true);
        ByteString content = ByteString.copyFromUtf8("CAS Would Exceed Max Size");
        Digest digest = DIGEST_UTIL.compute(content);
        blobs.put(digest, content);
        fileCache.put(digest, /* isExecutable=*/ false);
        return null;
      }
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

    Path pathOne = fileCache.put(digestOne, /* isExecutable=*/ false);
    Path pathTwo = fileCache.put(digestTwo, /* isExecutable=*/ false);
    Path pathThree = fileCache.put(digestThree, /* isExecutable=*/ false);
    fileCache.decrementReferences(ImmutableList.of(pathOne, pathTwo, pathThree), ImmutableList.of());
    /* three -> two -> one */
    assertThat(storage.get(pathOne).after).isEqualTo(storage.get(pathTwo));
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));

    /* one -> three -> two */
    assertThat(fileCache.findMissingBlobs(ImmutableList.of(digestOne))).isEmpty();
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));
    assertThat(storage.get(pathThree).after).isEqualTo(storage.get(pathOne));
  }

  Write getWrite(Digest digest) {
    return fileCache.getWrite(digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
  }

  @Test
  public void writeAddsEntry() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    AtomicBoolean notified = new AtomicBoolean(false);
    Write write = getWrite(digest);
    write.addListener(
        () -> notified.set(true),
        directExecutor());
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    Path key = fileCache.getKey(digest, false);
    assertThat(storage.get(key)).isNotNull();
    try (InputStream in = Files.newInputStream(key)) {
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
    incompleteWrite.addListener(
        () -> notified.set(true),
        directExecutor());
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
  public void incompleteWriteFileIsResumed() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    UUID writeId = UUID.randomUUID();
    Path key = fileCache.getKey(digest, false);
    Path writePath = key.resolveSibling(key.getFileName() + "." + writeId);
    try (OutputStream out = Files.newOutputStream(writePath)) {
      content.substring(0, 6).writeTo(out);
    }
    Write write = fileCache.getWrite(digest, writeId, RequestMetadata.getDefaultInstance());
    AtomicBoolean notified = new AtomicBoolean(false);
    write.addListener(
        () -> notified.set(true),
        directExecutor());
    assertThat(write.getCommittedSize()).isEqualTo(6);
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.substring(6).writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    assertThat(write.getCommittedSize()).isEqualTo(digest.getSizeBytes());
    assertThat(write.isComplete()).isTrue();
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
    Path path = fileCache.getKey(blob.getDigest(), /* isExecutable=*/ false);
    // putCreatesFile verifies this
    Files.delete(path);
    // update entry with expired deadline
    storage.get(path).existsDeadline = Deadline.after(0, SECONDS);

    try (InputStream in = fileCache.newInput(blob.getDigest(), /* offset=*/ 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      // success
    }
    assertThat(storage.containsKey(path)).isFalse();
  }

  @Test
  public void emptyWriteIsComplete() {
    Write write = fileCache.getWrite(
        DIGEST_UTIL.compute(ByteString.EMPTY),
        UUID.randomUUID(),
        RequestMetadata.getDefaultInstance());
    assertThat(write.isComplete()).isTrue();
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
    Write interruptingWrite = new Write() {
      boolean canReset = false;

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
          long deadlineAfter,
          TimeUnit deadlineAfterUnits,
          Runnable onReadyHandler) throws IOException {
        canReset = true;
        throw new IOException(new InterruptedException());
      }

      @Override
      public void reset() {
        if (!canReset) {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public void addListener(Runnable onCompleted, Executor executor) {
        throw new UnsupportedOperationException();
      }
    };
    when(delegate.getWrite(eq(expiringDigest), any(UUID.class), any(RequestMetadata.class)))
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

    verify(delegate, times(1)).getWrite(eq(expiringDigest), any(UUID.class), any(RequestMetadata.class));
  }

  void decrementReference(Path path) {
    fileCache.decrementReferences(ImmutableList.of(path), ImmutableList.of());
  }

  @Test
  public void duplicateExpiredEntrySuppressesDigestExpiration() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(512)) {
      for (int i = 0; i < 512; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());
    decrementReference(fileCache.put(expiringBlob.getDigest(), /* isExecutable=*/ false)); // expected eviction
    blobs.clear();
    decrementReference(fileCache.put(expiringBlob.getDigest(), /* isExecutable=*/ true)); // should be fed from storage directly, not through delegate

    fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));

    verifyZeroInteractions(onExpire);
    // assert expiration of non-executable digest
    Path expiringKey = fileCache.getKey(expiringBlob.getDigest(), /* isExecutable=*/ false);
    assertThat(storage.containsKey(expiringKey)).isFalse();
    assertThat(Files.exists(expiringKey)).isFalse();
  }

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
    Future<Void> blockingExpiration = expireService.submit(() -> {
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
    when(delegate.getWrite(eq(expiringBlob.getDigest()), any(UUID.class), any(RequestMetadata.class))).thenReturn(new NullWrite() {
      @Override
      public FeedbackOutputStream getOutput(long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) throws IOException {
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
    Thread expiringThread = new Thread(() -> {
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
    when(delegate.newInput(eq(blob.getDigest()), eq(0l))).thenReturn(content.newInput());
    InputStream in = fileCache.newInput(blob.getDigest(), 0);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(blob.getDigest(), 0l);
    // trigger the read through to complete immediately by supplying the blob
    fileCache.put(blob);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
  }

  @Test
  public void readThroughSwitchedToLocalContinues() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);
    ExecutorService service = newSingleThreadExecutor();
    SettableFuture<Void> writeComplete = SettableFuture.create();
    // we need to register callbacks on the shared write future
    Write write = new NullWrite() {
      @Override
      public void addListener(Runnable onCompleted, Executor executor) {
        writeComplete.addListener(onCompleted, executor);
      }

      @Override
      public FeedbackOutputStream getOutput(long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
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
    when(delegate.getWrite(eq(blob.getDigest()), any(UUID.class), any(RequestMetadata.class))).thenReturn(write);
    when(delegate.newInput(eq(blob.getDigest()), eq(0l))).thenReturn(content.newInput());
    // the switch will reset to this point
    InputStream switchedIn = content.newInput();
    switchedIn.skip(6);
    when(delegate.newInput(eq(blob.getDigest()), eq(6l))).thenReturn(switchedIn);
    InputStream in = fileCache.newReadThroughInput(blob.getDigest(), 0, write);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(blob.getDigest(), 0l);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
    if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
      throw new RuntimeException("could not shut down service");
    }
  }

  @RunWith(JUnit4.class)
  public static class NativeCASFileCacheTest extends CASFileCacheTest {
    public NativeCASFileCacheTest() throws IOException {
      super(createTempDirectory());
    }
    
    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      Path path = Files.createTempDirectory("native-cas-test");
      return path;
    }
  }

  @RunWith(JUnit4.class)
  public static class OsXCASFileCacheTest extends CASFileCacheTest {
    public OsXCASFileCacheTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.osX()).getRootDirectories(),
          null));
    }
  }

  @RunWith(JUnit4.class)
  public static class UnixCASFileCacheTest extends CASFileCacheTest {
    public UnixCASFileCacheTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.unix()).getRootDirectories(),
          null));
    }
  }

  @RunWith(JUnit4.class)
  public static class WindowsCASFileCacheTest extends CASFileCacheTest {
    public WindowsCASFileCacheTest() {
      super(Iterables.getFirst(
          Jimfs.newFileSystem(Configuration.windows()).getRootDirectories(),
          null));
    }
  }
}
