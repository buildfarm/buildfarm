// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.blake3.Blake3HashFunction;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/** Utility methods to work with {@link Digest}. */
public class DigestUtil {
  /**
   * If the digest function used is one of MD5, MURMUR3, SHA1, SHA256, SHA384, SHA512, or VSO, this
   * component MUST be omitted.
   *
   * <pre>
   * Omit these types from consideration until they are supported:
   * - DigestFunction.Value.MURMUR3 : 32- or 128-bit hash
   * - DigestFunction.Value.VSO : 33-byte hash
   * </pre>
   */
  public static final Set<DigestFunction.Value> OMITTED_DIGEST_FUNCTIONS =
      ImmutableSet.of(
          DigestFunction.Value.MD5,
          DigestFunction.Value.SHA1,
          DigestFunction.Value.SHA256,
          DigestFunction.Value.SHA384,
          DigestFunction.Value.SHA512);

  public static final Map<DigestFunction.Value, Digest> empty =
      ImmutableMap.of(
          DigestFunction.Value.MD5, DigestUtil.forHash("MD5").computeImpl(ByteString.empty()),
          DigestFunction.Value.SHA1, DigestUtil.forHash("SHA1").computeImpl(ByteString.empty()),
          DigestFunction.Value.SHA256, DigestUtil.forHash("SHA256").computeImpl(ByteString.empty()),
          DigestFunction.Value.SHA384, DigestUtil.forHash("SHA384").computeImpl(ByteString.empty()),
          DigestFunction.Value.SHA512, DigestUtil.forHash("SHA512").computeImpl(ByteString.empty()),
          DigestFunction.Value.BLAKE3,
              DigestUtil.forHash("BLAKE3").computeImpl(ByteString.empty()));

  /** Type of hash function to use for digesting blobs. */
  // The underlying HashFunctions are immutable and thread safe.
  @SuppressWarnings("ImmutableEnumChecker")
  public enum HashFunction {
    @SuppressWarnings("deprecation")
    MD5(Hashing.md5(), DigestFunction.Value.MD5),
    @SuppressWarnings("deprecation")
    SHA1(Hashing.sha1(), DigestFunction.Value.SHA1),
    SHA256(Hashing.sha256(), DigestFunction.Value.SHA256),
    SHA384(Hashing.sha384(), DigestFunction.Value.SHA384),
    SHA512(Hashing.sha512(), DigestFunction.Value.SHA512),
    BLAKE3(new Blake3HashFunction(), DigestFunction.Value.BLAKE3);

    @Getter private final com.google.common.hash.HashFunction hash;
    @Getter private final DigestFunction.Value digestFunction;

    HashFunction(com.google.common.hash.HashFunction hash, DigestFunction.Value digestFunction) {
      this.hash = hash;
      this.digestFunction = digestFunction;
    }

    public static HashFunction forHash(String hexDigest) {
      if (SHA256.isValidHexDigest(hexDigest)) {
        return SHA256;
      }
      if (SHA384.isValidHexDigest(hexDigest)) {
        return SHA384;
      }
      if (SHA512.isValidHexDigest(hexDigest)) {
        return SHA512;
      }
      if (SHA1.isValidHexDigest(hexDigest)) {
        return SHA1;
      }
      if (MD5.isValidHexDigest(hexDigest)) {
        return MD5;
      }
      throw new IllegalArgumentException("hash type unrecognized: " + hexDigest);
    }

    public static HashFunction get(DigestFunction.Value digestFunction) {
      switch (digestFunction) {
        default:
        case UNRECOGNIZED:
        case UNKNOWN:
          throw new IllegalArgumentException(digestFunction.toString());
        case BLAKE3:
          return BLAKE3;
        case SHA256:
          return SHA256;
        case SHA384:
          return SHA384;
        case SHA512:
          return SHA512;
        case SHA1:
          return SHA1;
        case MD5:
          return MD5;
      }
    }

    public boolean isValidHexDigest(String hexDigest) {
      return hexDigest != null && hexDigest.length() * 8 / 2 == hash.bits();
    }
  }

  /**
   * A special type of Digest that is used only as a remote action cache key. This is a separate
   * type in order to prevent accidentally using other Digests as action keys.
   */
  @Getter
  public static final class ActionKey {
    private final Digest digest;

    private ActionKey(Digest digest) {
      this.digest = digest;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ActionKey actionKey) {
        return digest.equals(actionKey.getDigest());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return digest.hashCode();
    }

    @Override
    public String toString() {
      return DigestUtil.toString(digest);
    }
  }

  private final HashFunction hashFn;

  public static DigestUtil forHash(String hashName) {
    try {
      DigestFunction.Value digestFunction = DigestFunction.Value.valueOf(hashName.toUpperCase());
      HashFunction hashFn = HashFunction.get(digestFunction);
      return new DigestUtil(hashFn);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public DigestUtil(HashFunction hashFn) {
    this.hashFn = hashFn;
  }

  public DigestFunction.Value getDigestFunction() {
    return hashFn.getDigestFunction();
  }

  public Digest compute(Path file) throws IOException {
    return buildDigest(computeHash(file), Files.size(file), getDigestFunction());
  }

  private String computeHash(Path file) throws IOException {
    return new ByteSource() {
      @Override
      public InputStream openStream() throws IOException {
        return Files.newInputStream(file);
      }
    }.hash(hashFn.getHash()).toString();
  }

  public HashCode computeHash(ByteString blob) {
    Hasher hasher = hashFn.getHash().newHasher();
    try {
      blob.writeTo(Funnels.asOutputStream(hasher));
    } catch (IOException e) {
      /* impossible, due to Funnels.asOutputStream behavior */
    }
    return hasher.hash();
  }

  public Digest compute(ByteString blob) {
    if (blob.size() == 0) {
      return empty();
    }
    return computeImpl(blob);
  }

  private Digest computeImpl(ByteString blob) {
    return buildDigest(computeHash(blob).toString(), blob.size(), getDigestFunction());
  }

  private static HashFunction parseHashFunction(String hexHash) {
    for (DigestFunction.Value digestFunction : OMITTED_DIGEST_FUNCTIONS) {
      try {
        HashFunction hashFn = HashFunction.get(digestFunction);
        if (hashFn.isValidHexDigest(hexHash)) {
          return hashFn;
        }
      } catch (IllegalArgumentException e) {
        // no matching hash, ignore
      }
    }
    return null;
  }

  public static DigestFunction.Value parseDigestFunction(String hexHash) {
    HashFunction hashFn = parseHashFunction(hexHash);
    if (hashFn == null) {
      return DigestFunction.Value.UNKNOWN;
    }
    return hashFn.getDigestFunction();
  }

  // only valid for OMITTED_DIGEST_FUNCTIONs
  public static DigestUtil parseHash(String hexHash) {
    HashFunction hashFn = parseHashFunction(hexHash);
    if (hashFn == null) {
      return null;
    }
    return new DigestUtil(hashFn);
  }

  public Digest build(String hexHash, long size) {
    if (!hashFn.isValidHexDigest(hexHash)) {
      throw new NumberFormatException(
          String.format("[%s] is not a valid %s hash.", hexHash, hashFn.name()));
    }
    return buildDigest(hexHash, size, getDigestFunction());
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
   */
  public static ActionKey asActionKey(Digest digest) {
    return new ActionKey(digest);
  }

  public Digest empty() {
    return empty.get(getDigestFunction());
  }

  public static Digest buildDigest(String hexHash, long size, DigestFunction.Value digestFunction) {
    return Digest.newBuilder()
        .setHash(hexHash)
        .setSize(size)
        .setDigestFunction(digestFunction)
        .build();
  }

  public HashingOutputStream newHashingOutputStream(OutputStream out) {
    return new HashingOutputStream(hashFn.getHash(), out);
  }

  public static String optionalDigestFunction(DigestFunction.Value digestFunction) {
    if (OMITTED_DIGEST_FUNCTIONS.contains(digestFunction)
        || digestFunction == DigestFunction.Value.UNKNOWN) {
      return "";
    }
    return digestFunction.toString().toLowerCase() + "/";
  }

  public static String toString(Digest digest) {
    return String.format(
        "%s%s/%d",
        optionalDigestFunction(digest.getDigestFunction()), digest.getHash(), digest.getSize());
  }

  public static build.bazel.remote.execution.v2.Digest toDigest(Digest digest) {
    return build.bazel.remote.execution.v2.Digest.newBuilder()
        .setHash(digest.getHash())
        .setSizeBytes(digest.getSize())
        .build();
  }

  public Digest toDigest(build.bazel.remote.execution.v2.Digest digest) {
    return Digest.newBuilder()
        .setHash(digest.getHash())
        .setSize(digest.getSizeBytes())
        .setDigestFunction(getDigestFunction())
        .build();
  }

  public static Digest parseDigest(String digest) {
    String[] components = digest.split("/");
    int length = components.length;
    DigestUtil digestUtil;
    // FIXME index range
    String hash = components[length - 2];
    if (length == 2) {
      digestUtil = parseHash(hash);
    } else {
      digestUtil = forHash(components[0]);
    }
    return Digest.newBuilder()
        .setHash(hash)
        .setSize(Long.parseLong(components[length - 1]))
        .setDigestFunction(digestUtil.getDigestFunction())
        .build();
  }

  public static DigestUtil forDigest(build.bazel.remote.execution.v2.Digest digest) {
    return new DigestUtil(HashFunction.forHash(digest.getHash()));
  }

  public static Digest fromDigest(
      build.bazel.remote.execution.v2.Digest digest, DigestFunction.Value digestFunction) {
    digestFunction = inferDigestFunction(digestFunction, digest.getHash());
    return Digest.newBuilder()
        .setHash(digest.getHash())
        .setSize(digest.getSizeBytes())
        .setDigestFunction(digestFunction)
        .build();
  }

  // maybe do this as a future to avoid the error sequence abort??
  public static DigestFunction.Value inferDigestFunction(
      DigestFunction.Value digestFunction, String hash) {
    // TODO we should probably also be validating every digest that comes in here or somewhere
    // similar
    if (digestFunction == DigestFunction.Value.UNKNOWN) {
      return parseDigestFunction(hash);
    }
    return digestFunction;
  }

  public static DigestFunction.Value inferDigestFunction(String hash) {
    return inferDigestFunction(DigestFunction.Value.UNKNOWN, hash);
  }
}
