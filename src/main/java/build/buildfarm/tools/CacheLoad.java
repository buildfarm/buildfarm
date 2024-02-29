// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.tools;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.StartupCacheResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Size;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Cas;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

// This tool is helpful for testing the startup of the CasFileCache.
// Give an existing file path, and the tool will load and print startup information about it.
class CacheLoad {
  private static class LocalCASFileCache extends CASFileCache {
    LocalCASFileCache(
        Path root,
        Cas config,
        long maxSizeInBytes,
        DigestUtil digestUtil,
        ExecutorService expireService,
        Executor accessRecorder) {
      super(root, config, maxSizeInBytes, digestUtil, expireService, accessRecorder);
    }

    @Override
    protected InputStream newExternalInput(Compressor.Value compressor, Digest digest, long offset)
        throws IOException {
      throw new IOException();
    }
  }

  /*
    When starting the CAS, ensure that the "max size" appropriately reflects the content size of the CAS's root.
    Otherwise, reaching the "max size" will result in files being deleted from the root.
    The appropriate size may not be obvious by observing actual disk usage (this especially true for zero filled test data)
    A closer calculation for ample "max size" could be calculated with "du -hs --apparent-size".
  */
  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]);
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    CASFileCache fileCache =
        new LocalCASFileCache(
            root,
            configs.getWorker().getStorages().get(0),
            /* maxSizeInBytes= */ Size.gbToBytes(500),
            new DigestUtil(HashFunction.SHA1),
            /* expireService= */ newDirectExecutorService(),
            /* accessRecorder= */ directExecutor());

    // Start cache and measure startup time (reported internally).
    StartupCacheResults results = fileCache.start(newDirectExecutorService(), true);

    // Report information on started cache.
    System.out.println("CAS Started.");
    System.out.println("Start Time: " + results.startupTime.getSeconds() + "s");

    // Load Information
    System.out.println("Loaded Cache: " + !results.load.loadSkipped);

    // Entry Information
    System.out.println("Total Entry Count: " + fileCache.entryCount());
    System.out.println("Unreferenced Entry Count: " + fileCache.unreferencedEntryCount());

    // File Information
    System.out.println("Cache Root: " + results.cacheDirectory);
    System.out.println("Directory Count: " + fileCache.directoryStorageCount());
    System.out.println("Current Size: " + Size.bytesToGb(fileCache.size()) + "GB");
  }
}
