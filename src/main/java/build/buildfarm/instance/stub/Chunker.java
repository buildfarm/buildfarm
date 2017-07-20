// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.stub;

import build.buildfarm.common.Digests;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/** An iterator-type object that transforms byte sources into a stream of Chunks. */
public final class Chunker implements Iterator {
  // This is effectively final, should be changed only in unit-tests!
  public static int DEFAULT_CHUNK_SIZE = 1024 * 16;
  private static byte[] EMPTY_BLOB = new byte[0];

  @VisibleForTesting
  static void setDefaultChunkSizeForTesting(int value) {
    DEFAULT_CHUNK_SIZE = value;
  }

  public static int getDefaultChunkSize() {
    return DEFAULT_CHUNK_SIZE;
  }

  /** A piece of a byte[] blob. */
  public static final class Chunk {

    private final Digest digest;
    private final long offset;
    // TODO(olaola): consider saving data in a different format that byte[].
    private final byte[] data;

    @VisibleForTesting
    public Chunk(Digest digest, byte[] data, long offset) {
      this.digest = digest;
      this.data = data;
      this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Chunk)) {
        return false;
      }
      Chunk other = (Chunk) o;
      return other.offset == offset
          && other.digest.equals(digest)
          && Arrays.equals(other.data, data);
    }

    @Override
    public int hashCode() {
      return Objects.hash(digest, offset, Arrays.hashCode(data));
    }

    public Digest getDigest() {
      return digest;
    }

    public long getOffset() {
      return offset;
    }

    // This returns a mutable copy, for efficiency.
    public byte[] getData() {
      return data;
    }
  }

  /** An Item is an opaque digestable source of bytes. */
  interface Item {
    Digest getDigest();

    InputStream getInputStream();
  }

  private final Iterator<Item> inputIterator;
  private InputStream currentStream;
  private Digest digest;
  private long bytesLeft;
  private final int chunkSize;

  Chunker(Iterator<Item> inputIterator, int chunkSize) {
    Preconditions.checkArgument(chunkSize > 0, "Chunk size must be greater than 0");
    this.inputIterator = inputIterator;
    this.chunkSize = chunkSize;
    advanceInput();
  }

  public void advanceInput() {
    if (inputIterator != null && inputIterator.hasNext()) {
      Item input = inputIterator.next();
      digest = input.getDigest();
      currentStream = input.getInputStream();
      bytesLeft = digest.getSizeBytes();
    } else {
      digest = null;
      currentStream = null;
      bytesLeft = 0;
    }
  }

  /** True if the object has more Chunk elements. */
  public boolean hasNext() {
    return currentStream != null;
  }

  /** Consume the next Chunk element. */
  public Chunk next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final long offset = digest.getSizeBytes() - bytesLeft;
    byte[] blob = EMPTY_BLOB;
    try {
      if (bytesLeft > 0) {
        blob = new byte[(int) Math.min(bytesLeft, chunkSize)];
        currentStream.read(blob);
        bytesLeft -= blob.length;
      }
      final Chunk result = new Chunk(digest, blob, offset);
      if (bytesLeft == 0) {
        currentStream.close();
        advanceInput(); // Sets the current stream to null, if it was the last.
      }
      return result;
    } catch(IOException ex) {
      throw new NoSuchElementException();
    }
  }

  static Item toItem(final ByteString blob) {
    return new Item() {
      Digest digest = null;

      @Override
      public Digest getDigest() {
        if (digest == null) {
          digest = Digests.computeDigest(blob);
        }
        return digest;
      }

      @Override
      public InputStream getInputStream() {
        return blob.newInput();
      }
    };
  }

  static class MemberOf implements Predicate<Item> {
    private final Set<Digest> digests;

    public MemberOf(Set<Digest> digests) {
      this.digests = digests;
    }

    @Override
    public boolean apply(Item item) {
      return digests.contains(item.getDigest());
    }
  }

  /**
   * Create a Chunker from multiple input sources. The order of the sources provided to the Builder
   * will be the same order they will be chunked by.
   */
  public static final class Builder {
    private final ArrayList<Item> items = new ArrayList<>();
    private Set<Digest> digests = null;
    private int chunkSize = getDefaultChunkSize();

    public Chunker build() {
      return new Chunker(
          digests == null
              ? items.iterator()
              : Iterators.filter(items.iterator(), new MemberOf(digests)),
          chunkSize);
    }

    public Builder chunkSize(int chunkSize) {
      this.chunkSize = chunkSize;
      return this;
    }

    /**
     * Restricts the Chunker to use only inputs with these digests. This is an optimization for CAS
     * uploads where a list of digests missing from the CAS is known.
     */
    public Builder onlyUseDigests(Set<Digest> digests) {
      this.digests = digests;
      return this;
    }

    public Builder addInput(ByteString blob) {
      items.add(toItem(blob));
      return this;
    }

    public Builder addAllInputs(Iterable<? extends ByteString> blobs) {
      for (ByteString blob : blobs) {
        items.add(toItem(blob));
      }
      return this;
    }
  }
}
