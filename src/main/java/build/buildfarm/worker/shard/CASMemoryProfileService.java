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

package build.buildfarm.server;

import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.v1test.*;
import build.buildfarm.worker.CASFileCache;
import io.grpc.stub.StreamObserver;
import java.util.logging.Logger;

public class CASMemoryProfileService extends CASMemoryProfileGrpc.CASMemoryProfileImplBase {
  private static final Logger logger = Logger.getLogger(CASMemoryProfileService.class.getName());

  private final CASFileCache storage;

  public CASMemoryProfileService(ContentAddressableStorage storage) {

    this.storage = (CASFileCache) storage;
  }

  @Override
  public void getCASMemoryUsage(
      MemoryUsageRequest request, StreamObserver<MemoryUsageMessage> responseObserver) {
    // get Entry storage size
    MemoryUsage entry =
        MemoryUsage.newBuilder()
            .setMemoryUsedFor("Number of Entry")
            .setMemoryUsage(storage.storageCount())
            .build();

    // get DirecotyEntry storage size
    MemoryUsage dirEntry =
        MemoryUsage.newBuilder()
            .setMemoryUsedFor("Number of DirectoryEntry")
            .setMemoryUsage(storage.directoryStorageCount())
            .build();

    MemoryUsageMessage reply =
        MemoryUsageMessage.newBuilder().addEntry(entry).addEntry(dirEntry).build();

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
