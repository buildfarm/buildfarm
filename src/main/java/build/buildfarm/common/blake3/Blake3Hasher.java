// Copyright 2023 The Buildfarm Authors. All rights reserved.
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
package build.buildfarm.common.blake3;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/** A {@link Hasher} for BLAKE3. */
public final class Blake3Hasher implements Hasher {
  private final Blake3MessageDigest messageDigest;
  private boolean isDone = false;

  public Blake3Hasher(Blake3MessageDigest blake3MessageDigest) {
    messageDigest = blake3MessageDigest;
  }

  /* The following methods implement the {Hasher} interface. */
  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param bytes the bytes parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the hasher result
   */
  public Hasher putBytes(ByteBuffer b) {
    messageDigest.engineUpdate(b);
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param bytes the bytes parameter
   * @return the hasher result
   */
  public Hasher putBytes(byte[] bytes, int off, int len) {
    messageDigest.engineUpdate(bytes, off, len);
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param b the b parameter
   * @return the hasher result
   */
  public Hasher putBytes(byte[] bytes) {
    messageDigest.engineUpdate(bytes, 0, bytes.length);
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the hashcode result
   */
  public Hasher putByte(byte b) {
    messageDigest.engineUpdate(b);
    return this;
  }

  @Override
  /**
   * Stores a blob in the Content Addressable Storage
   * @param b the b parameter
   * @return the hasher result
   */
  public HashCode hash() {
    checkState(!isDone);
    isDone = true;

    return HashCode.fromBytes(messageDigest.engineDigest());
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param d the d parameter
   * @return the hasher result
   */
  public Hasher putBoolean(boolean b) {
    return putByte(b ? (byte) 1 : (byte) 0);
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param f the f parameter
   * @return the hasher result
   */
  public Hasher putDouble(double d) {
    return putLong(Double.doubleToRawLongBits(d));
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param charSequence the charSequence parameter
   * @return the hasher result
   */
  public Hasher putFloat(float f) {
    return putInt(Float.floatToRawIntBits(f));
  }

  @Override
  @CanIgnoreReturnValue
  @SuppressWarnings("PMD.ForLoopVariableCount")
  /**
   * Stores a blob in the Content Addressable Storage
   * @param charSequence the charSequence parameter
   * @param charset the charset parameter
   * @return the hasher result
   */
  public Hasher putUnencodedChars(CharSequence charSequence) {
    for (int i = 0, len = charSequence.length(); i < len; i++) {
      putChar(charSequence.charAt(i));
    }
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param s the s parameter
   * @return the hasher result
   */
  public Hasher putString(CharSequence charSequence, Charset charset) {
    return putBytes(charSequence.toString().getBytes(charset));
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param i the i parameter
   * @return the hasher result
   */
  public Hasher putShort(short s) {
    putByte((byte) s);
    putByte((byte) (s >>> 8));
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param l the l parameter
   * @return the hasher result
   */
  public Hasher putInt(int i) {
    putByte((byte) i);
    putByte((byte) (i >>> 8));
    putByte((byte) (i >>> 16));
    putByte((byte) (i >>> 24));
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param c the c parameter
   * @return the hasher result
   */
  public Hasher putLong(long l) {
    for (int i = 0; i < 64; i += 8) {
      putByte((byte) (l >>> i));
    }
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  /**
   * Stores a blob in the Content Addressable Storage
   * @param instance the instance parameter
   * @param funnel the funnel parameter
   * @return the hasher result
   */
  public Hasher putChar(char c) {
    putByte((byte) c);
    putByte((byte) (c >>> 8));
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  public <T> Hasher putObject(T instance, Funnel<? super T> funnel) {
    funnel.funnel(instance, this);
    return this;
  }
}
