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

package build.buildfarm;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

class CASTest {
  private static class LocalCASFileCache extends CASFileCache {
    LocalCASFileCache(
        Path root,
        long maxSizeInBytes,
        DigestUtil digestUtil,
        ExecutorService expireService,
        Executor accessRecorder) {
      super(root, maxSizeInBytes, maxSizeInBytes, digestUtil, expireService, accessRecorder);
    }

    @Override
    protected InputStream newExternalInput(Digest digest, long offset) throws IOException {
      throw new IOException();
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]);
    CASFileCache fileCache = new LocalCASFileCache(
        root,
        /* maxSizeInBytes=*/ 100l * 1024 * 1024 * 1024,
        new DigestUtil(HashFunction.SHA1),
        /* expireService=*/ newDirectExecutorService(),
        /* accessRecorder=*/ directExecutor());
    fileCache.start(newDirectExecutorService());
    System.out.println("Done with start, ready to roll...");
  }
}
