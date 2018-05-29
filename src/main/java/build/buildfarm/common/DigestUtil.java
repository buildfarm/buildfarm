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
package build.buildfarm.common;

import build.buildfarm.v1test.InstanceConfig;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Utility methods to work with {@link Digest}. */
public class DigestUtil {

  /** Type of hash function to use for digesting blobs. */
  // The underlying HashFunctions are immutable and thread safe.
  @SuppressWarnings("ImmutableEnumChecker")
  public enum HashFunction {
    MD5(Hashing.md5()),
    SHA1(Hashing.sha1()),
    SHA256(Hashing.sha256());

    private final com.google.common.hash.HashFunction hash;
    final HashCode empty;

    HashFunction(com.google.common.hash.HashFunction hash) {
      this.hash = hash;
      empty = this.hash.newHasher().hash();
    }

    public static HashFunction get(InstanceConfig.HashFunction hashFunction) throws IllegalArgumentException {
      switch(hashFunction) {
      default:
      case UNRECOGNIZED:
        throw new IllegalArgumentException(hashFunction.toString());
      case MD5: return MD5;
      case SHA1: return SHA1;
      case SHA256: return SHA256;
      }
    }

    public com.google.common.hash.HashFunction getHash() {
      return hash;
    }

    public boolean isValidHexDigest(String hexDigest) {
      return hexDigest != null && hexDigest.length() * 8 / 2 == hash.bits();
    }

    public HashCode empty() {
      return empty;
    }
  }

  /**
   * A special type of Digest that is used only as a remote action cache key. This is a separate
   * type in order to prevent accidentally using other Digests as action keys.
   */
  public static final class ActionKey {
    private final Digest digest;

    public Digest getDigest() {
      return digest;
    }

    private ActionKey(Digest digest) {
      this.digest = digest;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ActionKey) {
        ActionKey actionKey = (ActionKey) o;
        return digest.equals(actionKey.getDigest());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return digest.hashCode();
    }
  }

  private final HashFunction hashFn;
  private final Digest empty;

  public static DigestUtil forHash(String hashName) {
    InstanceConfig.HashFunction hash = InstanceConfig.HashFunction.valueOf(
        InstanceConfig.HashFunction.getDescriptor().findValueByName(hashName));
    HashFunction hashFunction = HashFunction.get(hash);
    return new DigestUtil(hashFunction);
  }

  public DigestUtil(HashFunction hashFn) {
    this.hashFn = hashFn;
    empty = buildDigest(hashFn.empty().toString(), 0);
  }

  public Digest compute(Path file) throws IOException {
    return compute(file, Files.size(file));
  }

  private String computeHash(Path file) throws IOException {
    return new ByteSource() {
      @Override
      public InputStream openStream() throws IOException {
        return Files.newInputStream(file);
      }
    }.hash(hashFn.getHash()).toString();
  }

  public Digest compute(Path file, long fileSize) throws IOException {
    return buildDigest(computeHash(file), fileSize);
  }

  public Digest compute(ByteString blob) {
    Hasher hasher = hashFn.getHash().newHasher();
    try {
      blob.writeTo(Funnels.asOutputStream(hasher));
    } catch (IOException e) {
      /* impossible, due to Funnels.asOutputStream behavior */
    }
    return buildDigest(hasher.hash().toString(), blob.size());
  }

  public Digest build(String hexHash, long size) {
    if (!hashFn.isValidHexDigest(hexHash)) {
      throw new NumberFormatException(
        String.format("[%s] is not a valid %s hash.", hexHash, hashFn.name())
      );
    }
    return buildDigest(hexHash, size);
  }

  /**
   * Computes a digest of the given proto message. Currently, we simply rely on message output as
   * bytes, but this implementation relies on the stability of the proto encoding, in particular
   * between different platforms and languages. TODO(olaola): upgrade to a better implementation!
   */
  public Digest compute(Message message) {
    return compute(message.toByteString());
  }

  public ActionKey computeActionKey(Action action) {
    return new ActionKey(compute(action));
  }

  /**
   * Assumes that the given Digest is a valid digest of an Action, and creates an ActionKey wrapper.
   * This should not be called on the client side!
   */
  public static ActionKey asActionKey(Digest digest) {
    return new ActionKey(digest);
  }

  public Digest empty() {
    return empty;
  }

  public static Digest buildDigest(String hexHash, long size) {
    return Digest.newBuilder().setHash(hexHash).setSizeBytes(size).build();
  }

  public static String toString(Digest digest) {
    return String.format("%s/%d", digest.getHash(), digest.getSizeBytes());
  }

  public static Digest parseDigest(String digest) {
    String[] components = digest.split("/");
    return Digest.newBuilder()
        .setHash(components[0])
        .setSizeBytes(Integer.parseInt(components[1]))
        .build();
  }
}
