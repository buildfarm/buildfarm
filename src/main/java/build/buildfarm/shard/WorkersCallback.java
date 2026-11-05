// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.shard;

import com.google.common.util.concurrent.FutureCallback;
import io.grpc.Status;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Random;

public abstract class WorkersCallback implements FutureCallback<List<String>> {
  private final Random rand;

  public WorkersCallback(Random rand) {
    this.rand = rand;
  }

  @Override
  public void onSuccess(List<String> workersList) {
    if (workersList.isEmpty()) {
      onFailure(Status.NOT_FOUND.withDescription("No workers found.").asException());
    } else {
      Collections.shuffle(workersList, rand);
      onQueue(new ArrayDeque<>(workersList));
    }
  }

  protected abstract void onQueue(Deque<String> workers);
}
