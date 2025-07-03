// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import io.grpc.stub.StreamObserver;
import javax.annotation.Nullable;

/**
 * A Worker profile service for a shard without execution functionality. <br>
 * It could be used without storage (null), but it wouldn't be very useful.
 *
 * @see WorkerExecProfileService for shards with execution.
 */
public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  // What use is a nullable storage? it allows the subclass to use it for storage+exec and exec-only
  // shards.
  private final @Nullable CASFileCache storage;

  /**
   * Worker profile service for storage-only shard.
   *
   * @param storage Storage subsystem for metrics.
   * @see WorkerExecProfileService for a shard with execution functionality.
   */
  public WorkerProfileService(CASFileCache storage) {
    this.storage = storage;
  }

  WorkerProfileMessage.Builder reportStorageUsage(WorkerProfileMessage.Builder replyBuilder) {
    // FIXME deliver full local storage chain
    if (storage != null) {
      replyBuilder
          .setCasSize(storage.size())
          .setCasEntryCount(storage.entryCount())
          .setCasMaxSize(storage.maxSize())
          .setCasMaxEntrySize(storage.maxEntrySize())
          .setCasUnreferencedEntryCount(storage.unreferencedEntryCount())
          .setCasDirectoryEntryCount(storage.directoryStorageCount())
          .setCasEvictedEntryCount(storage.getEvictedCount())
          .setCasEvictedEntrySize(storage.getEvictedSize());
    }
    return replyBuilder;
  }

  @Override
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {
    // get usage of CASFileCache
    WorkerProfileMessage.Builder replyBuilder = WorkerProfileMessage.newBuilder();
    reportStorageUsage(replyBuilder);
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}
