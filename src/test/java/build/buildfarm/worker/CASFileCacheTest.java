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
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.worker.CASFileCache.Entry;
import build.buildfarm.worker.CASFileCache.PutDirectoryException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  private ConcurrentMap<Path, Entry> storage;

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
    MockitoAnnotations.initMocks(this);
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    fileCache = new CASFileCache(
        root,
        /* maxSizeInBytes=*/ 1024,
        DIGEST_UTIL,
        /* expireService=*/
        newDirectExecutorService(),
        storage,
        onPut,
        onExpire) {
      @Override
      protected InputStream newExternalInput(Digest digest, long offset) {
        ByteString content = blobs.get(digest);
        if (content == null) {
          return new BrokenInputStream(new IOException("NOT_FOUND: " + DigestUtil.toString(digest)));
        }
        return content.substring((int) offset).newInput();
      }
    };
  }

  @After
  public void tearDown() {
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
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
        DIGEST_UTIL,
        /* expireService=*/ newDirectExecutorService()) {
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

    fileCache.decrementReferences(ImmutableList.<Path>of(bigPath), ImmutableList.of());

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

    AtomicReference<Boolean> started = new AtomicReference<>(false);
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
    fileCache.decrementReferences(ImmutableList.<Path>of(bigPath), ImmutableList.of());
    try {
      putFuture.get();
    } finally {
      if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
        throw new RuntimeException("could not shut down put service");
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
