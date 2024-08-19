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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.blake3.Blake3HashFunction;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import lombok.Getter;

/** Utility methods to work with {@link Digest}. */
public class DigestUtil {
  /** Type of hash function to use for digesting blobs. */
  // The underlying HashFunctions are immutable and thread safe.
  @SuppressWarnings("ImmutableEnumChecker")
  public enum HashFunction {
    @SuppressWarnings("deprecation")
    MD5(Hashing.md5()),
    @SuppressWarnings("deprecation")
    SHA1(Hashing.sha1()),
    SHA256(Hashing.sha256()),
    BLAKE3(new Blake3HashFunction());

    @Getter private final com.google.common.hash.HashFunction hash;
    final HashCode empty;

    HashFunction(com.google.common.hash.HashFunction hash) {
      this.hash = hash;
      empty = this.hash.newHasher().hash();
    }

    public DigestFunction.Value getDigestFunction() {
      if (this == BLAKE3) {
        return DigestFunction.Value.BLAKE3;
      }
      if (this == SHA256) {
        return DigestFunction.Value.SHA256;
      }
      if (this == SHA1) {
        return DigestFunction.Value.SHA1;
      }
      if (this == MD5) {
        return DigestFunction.Value.MD5;
      }
      return DigestFunction.Value.UNKNOWN;
    }

    // Correct me if I'm wrong but this function doesn't make any sense?
    // It is always calling the same isValidHexDigest function with the same parameters.
    // So it will always either return SHA256, or throw an exception.
    // With the new method we still can't distinguish between SHA256 and BLAKE3, but at least it's better...
    public static HashFunction forHash(String hexDigest) {
      int hashLength = hexDigest.length();
        return switch (hashLength) {
            case 64 -> // 32 bytes * 2 characters per byte
                    HashFunction.SHA256;
            case 40 -> // 20 bytes * 2 characters per byte
                    HashFunction.SHA1;
            case 32 -> // 16 bytes * 2 characters per byte
                    HashFunction.MD5;
            default -> throw new IllegalArgumentException("hash type unrecognized: " + hexDigest);
        };
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
        case SHA1:
          return SHA1;
        case MD5:
          return MD5;
      }
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
  }

  private final HashFunction hashFn;
  private final Digest empty;

  public static DigestUtil forHash(String hashName) {
    EnumValueDescriptor hashEnumDescriptor =
        DigestFunction.Value.getDescriptor().findValueByName(hashName);
    if (hashEnumDescriptor == null) {
      throw new IllegalArgumentException("hash type unrecognized: " + hashName);
    }
    DigestFunction.Value digestFunction = DigestFunction.Value.valueOf(hashEnumDescriptor);
    HashFunction hashFunction = HashFunction.get(digestFunction);
    return new DigestUtil(hashFunction);
  }

  public DigestUtil(HashFunction hashFn) {
    this.hashFn = hashFn;
    empty = buildDigest(hashFn.empty().toString(), 0);
  }

  public DigestFunction.Value getDigestFunction() {
    return hashFn.getDigestFunction();
  }

  public static List<DigestFunction.Value> getSupportedDigestFunctions() {
    return Stream.of(HashFunction.values())
        .map(HashFunction::getDigestFunction)
        .toList();
  }

  public Digest compute(Path file) throws IOException {
    return buildDigest(computeHash(file), Files.size(file));
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
    return buildDigest(computeHash(blob).toString(), blob.size());
  }

  public Digest build(String hexHash, long size) {
    return buildDigest(hexHash, size);
  }

  public Digest build(byte[] hash, long size) {
    return build(HashCode.fromBytes(hash).toString(), size);
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
    return empty;
  }

  public static Digest buildDigest(String hexHash, long size) {
    return Digest.newBuilder().setHash(hexHash).setSizeBytes(size).build();
  }

  public HashingOutputStream newHashingOutputStream(OutputStream out) {
    return new HashingOutputStream(hashFn.getHash(), out);
  }

  public static String toString(Digest digest) {
    return String.format("%s/%d", digest.getHash(), digest.getSizeBytes());
  }

  public static Digest parseDigest(String digest) {
    String[] components = digest.split("/");
    return Digest.newBuilder()
        .setHash(components[0])
        .setSizeBytes(Long.parseLong(components[1]))
        .build();
  }

  public static DigestUtil forDigest(Digest digest) {
    return new DigestUtil(HashFunction.forHash(digest.getHash()));
  }

  private static final Map<DigestFunction.Value, DigestUtil> DIGEST_UTIL_MAP;

  static {
    DIGEST_UTIL_MAP = new EnumMap<>(DigestFunction.Value.class);
    DIGEST_UTIL_MAP.put(DigestFunction.Value.MD5, new DigestUtil(HashFunction.MD5));
    DIGEST_UTIL_MAP.put(DigestFunction.Value.SHA1, new DigestUtil(HashFunction.SHA1));
    DIGEST_UTIL_MAP.put(DigestFunction.Value.SHA256, new DigestUtil(HashFunction.SHA256));
    DIGEST_UTIL_MAP.put(DigestFunction.Value.BLAKE3, new DigestUtil(HashFunction.BLAKE3));
  }

  public static DigestUtil forDigestFunction(DigestFunction.Value digestFunction) {
    DigestUtil digestUtil = DIGEST_UTIL_MAP.get(digestFunction);
    if (digestUtil == null) {
      throw new IllegalArgumentException("Unsupported digest function: " + digestFunction);
    }
    return digestUtil;
  }

}
