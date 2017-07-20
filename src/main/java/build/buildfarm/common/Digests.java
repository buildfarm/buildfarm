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

import com.google.common.hash.HashCode;
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

/** Helper methods relating to computing Digest messages for remote execution. */
public final class Digests {
  private Digests() {}

  public static Digest computeDigest(ByteString blob) {
    try {
      return buildDigest(
          new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
              return blob.newInput();
            }
          }.hash(Hashing.sha1()).toString(),
          blob.size());
    } catch(IOException ex) {
      /* impossible */
      return null;
    }
  }

  private static byte[] getSHA1Digest(final Path path) throws IOException {
    // Naive I/O implementation.  TODO(olaola): optimize!
    return new ByteSource() {
      @Override
      public InputStream openStream() throws IOException {
        return Files.newInputStream(path);
      }
    }.hash(Hashing.sha1()).asBytes();
  }

  public static Digest computeDigest(Path file) throws IOException {
    long fileSize = Files.size(file);
    byte[] digest = getSHA1Digest(file);
    return buildDigest(digest, fileSize);
  }

  /**
   * Computes a digest of the given proto message. Currently, we simply rely on message output as
   * bytes, but this implementation relies on the stability of the proto encoding, in particular
   * between different platforms and languages. TODO(olaola): upgrade to a better implementation!
   */
  public static Digest computeDigest(Message message) {
    return computeDigest(message.toByteString());
  }

  /**
   * A special type of Digest that is used only as a remote action cache key. This is a
   * separate type in order to prevent accidentally using other Digests as action keys.
   */
  public static final class ActionKey {
    private final Digest digest;

    public Digest getDigest() {
      return digest;
    }

    private ActionKey(Digest digest) {
      this.digest = digest;
    }
  }

  public static ActionKey computeActionKey(Action action) {
    return new ActionKey(computeDigest(action));
  }

  /**
   * Assumes that the given Digest is a valid digest of an Action, and creates an ActionKey
   * wrapper. This should not be called on the client side!
   */
  public static ActionKey unsafeActionKeyFromDigest(Digest digest) {
    return new ActionKey(digest);
  }

  public static Digest buildDigest(byte[] hash, long size) {
    return buildDigest(HashCode.fromBytes(hash).toString(), size);
  }

  public static Digest buildDigest(String hexHash, long size) {
    return Digest.newBuilder().setHash(hexHash).setSizeBytes(size).build();
  }

  public static String toString(Digest digest) {
    return String.format("%s/%d", digest.getHash(), digest.getSizeBytes());
  }
}
