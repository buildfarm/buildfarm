// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.ContentAddressableStorage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @class CasFallbackHandler
 * @brief The CasFileCache provides a fallback mechanism to store CAS data in another data source.
 *     This module handles the various fallback behaviors.
 * @details These methods are called by the primary CAS manager.
 */
public class CasFallbackHandler {

  /**
   * @brief Start the CAS delegate.
   * @details Some delegates need started depending on the CAS type that they are.
   * @param delegate The delgate to start.
   * @param onStartPut A callback when CAS objects are put (relevant for CasFileCache).
   * @param removeDirectoryService Service for deleting files (relevant for CasFileCache).
   * @param skipLoad Whether to load the existing cache (relevant for CasFileCache).
   */
  public static void start(
      ContentAddressableStorage delegate,
      Consumer<Digest> onStartPut,
      ExecutorService removeDirectoryService,
      boolean skipLoad)
      throws IOException, InterruptedException {
    // start delegate if we specifically have a CASFileCache
    if (delegate != null && delegate instanceof CASFileCache) {
      CASFileCache fileCacheDelegate = (CASFileCache) delegate;
      fileCacheDelegate.start(onStartPut, removeDirectoryService, skipLoad);
    }
  }
  /**
   * @brief Get an inputstream to read the given digest data.
   * @details Creates the delegate's inputstream if possible.
   * @param delegate The delgate to get the inputstream from.
   * @param e Exception from primary (refactor to not use exceptions for control flow).
   * @param digest The digest to read.
   * @param offset The reading offset for the data to read.
   * @return Inputstream to blob.
   * @note Suggested return identifier: istream.
   */
  public static InputStream newTransparentInput(
      ContentAddressableStorage delegate, NoSuchFileException e, Digest digest, long offset)
      throws IOException {
    if (delegate == null) {
      throw e;
    }
    return delegate.newInput(digest, offset);
  }
}
