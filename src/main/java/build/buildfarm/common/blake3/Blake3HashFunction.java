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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/** A {@link HashFunction} for BLAKE3. */
public final class Blake3HashFunction implements HashFunction {
  @Override
  public int bits() {
    return 256;
  }

  @Override
  public Hasher newHasher() {
    return new Blake3Hasher(new Blake3MessageDigest());
  }

  @Override
  public Hasher newHasher(int expectedInputSize) {
    checkArgument(
        expectedInputSize >= 0, "expectedInputSize must be >= 0 but was %s", expectedInputSize);
    return newHasher();
  }

  /* The following methods implement the {HashFunction} interface. */
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @return the hashcode result
   */
  public <T> HashCode hashObject(T instance, Funnel<? super T> funnel) {
    return newHasher().putObject(instance, funnel).hash();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @param charset the charset parameter
   * @return the hashcode result
   */
  public HashCode hashUnencodedChars(CharSequence input) {
    int len = input.length();
    return newHasher(len * 2).putUnencodedChars(input).hash();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @return the hashcode result
   */
  public HashCode hashString(CharSequence input, Charset charset) {
    return newHasher().putString(input, charset).hash();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @return the hashcode result
   */
  public HashCode hashInt(int input) {
    return newHasher(4).putInt(input).hash();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @return the hashcode result
   */
  public HashCode hashLong(long input) {
    return newHasher(8).putLong(input).hash();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the hashcode result
   */
  public HashCode hashBytes(byte[] input) {
    return hashBytes(input, 0, input.length);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param input the input parameter
   * @return the hashcode result
   */
  public HashCode hashBytes(byte[] input, int off, int len) {
    checkPositionIndexes(off, off + len, input.length);
    return newHasher(len).putBytes(input, off, len).hash();
  }

  @Override
  public HashCode hashBytes(ByteBuffer input) {
    return newHasher(input.remaining()).putBytes(input).hash();
  }
}
