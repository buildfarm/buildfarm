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

package build.buildfarm.worker.operationqueue;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

class InjectedCASFileCache extends CASFileCache {
  private final InputStreamFactory inputStreamFactory;

  InjectedCASFileCache(
      InputStreamFactory inputStreamFactory,
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      boolean storeFileDirsIndexInMemory,
      DigestUtil digestUtil,
      ExecutorService expireService,
      Executor accessRecorder) {
    super(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        storeFileDirsIndexInMemory,
        digestUtil,
        expireService,
        accessRecorder);
    this.inputStreamFactory = inputStreamFactory;
  }

  @Override
  protected InputStream newExternalInput(Digest digest, long offset)
      throws IOException, InterruptedException {
    return inputStreamFactory.newInput(digest, offset);
  }
}
