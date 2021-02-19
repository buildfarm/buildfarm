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
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.Tree;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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

    public DigestFunction.Value getDigestFunction() {
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

    public static HashFunction forHash(String hexDigest) {
      if (SHA256.isValidHexDigest(hexDigest)) {
        return SHA256;
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
        case SHA256:
          return SHA256;
        case SHA1:
          return SHA1;
        case MD5:
          return MD5;
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
    if (!hashFn.isValidHexDigest(hexHash)) {
      throw new NumberFormatException(
          String.format("[%s] is not a valid %s hash.", hexHash, hashFn.name()));
    }
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

  public static Map<Digest, Directory> proxyDirectoriesIndex(
      Map<String, Directory> directoriesIndex) {
    return new Map<Digest, Directory>() {
      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsKey(Object key) {
        if (key instanceof Digest) {
          return directoriesIndex.containsKey(DigestUtil.toString((Digest) key));
        }
        return false;
      }

      @Override
      public boolean containsValue(Object value) {
        return directoriesIndex.containsValue(value);
      }

      @Override
      public Set<Map.Entry<Digest, Directory>> entrySet() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean equals(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Directory get(Object key) {
        if (key instanceof Digest) {
          Digest digest = (Digest) key;
          return directoriesIndex.get(digest.getHash());
        }
        return null;
      }

      @Override
      public int hashCode() {
        return directoriesIndex.hashCode();
      }

      @Override
      public boolean isEmpty() {
        return directoriesIndex.isEmpty();
      }

      @Override
      public Set<Digest> keySet() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Directory put(Digest key, Directory value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void putAll(Map<? extends Digest, ? extends Directory> m) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Directory remove(Object key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        return directoriesIndex.size();
      }

      @Override
      public Collection<Directory> values() {
        return directoriesIndex.values();
      }
    };
  }

  public Map<Digest, Directory> createDirectoriesIndex(Tree tree) {
    Set<Digest> directoryDigests = Sets.newHashSet();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = ImmutableMap.builder();
    directoriesIndex.put(compute(tree.getRoot()), tree.getRoot());
    for (Directory directory : tree.getChildrenList()) {
      Digest directoryDigest = compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return directoriesIndex.build();
  }
}
