/**
 * Stores a blob in the Content Addressable Storage
 * @param key the key parameter
 * @param length the length parameter
 * @param in the in parameter
 */
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

package build.buildfarm.proxy.http;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An interface for storing BLOBs each one indexed by a string (hash in hexadecimal).
 *
 * <p>Implementations must be thread-safe.
 */
public interface SimpleBlobStore {
  /** Returns {@code key} if the provided {@code key} is stored in the CAS. */
  boolean containsKey(String key) throws IOException, InterruptedException;

  /**
   * Fetches the BLOB associated with the {@code key} from the CAS and writes it to {@code out}.
   *
   * <p>The caller is responsible to close {@code out}.
   *
   * @return {@code true} if the {@code key} was found. {@code false} otherwise.
   */
  ListenableFuture<Boolean> get(String key, OutputStream out);

  /**
   * Fetches the BLOB associated with the {@code key} from the Action Cache and writes it to {@code
   * out}.
   *
   * <p>The caller is responsible to close {@code out}.
   *
   * @return {@code true} if the {@code key} was found. {@code false} otherwise.
   */
  boolean getActionResult(String actionKey, OutputStream out)
      throws IOException, InterruptedException;

  /**
   * Uploads a BLOB (as {@code in}) with length {@code length} indexed by {@code key} to the CAS.
   *
   * <p>This function results in a close of {@code in}.
   */
  void put(String key, long length, InputStream in) throws IOException, InterruptedException;

  /** Uploads a bytearray BLOB (as {@code in}) indexed by {@code key} to the Action Cache. */
  void putActionResult(String actionKey, byte[] in) throws IOException, InterruptedException;

  /** Close resources associated with the blob store. */
  void close();
}
