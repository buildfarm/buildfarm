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

package build.buildfarm.instance.shard;

import build.buildfarm.backplane.Backplane;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public final class Shard {
  private Shard() {}

  public static String getRandomWritingWorker(Backplane backplane, Random rand) throws IOException {
    List<String> workers =
        backplane.getStorageWorkers().stream()
            .filter(w -> !w.getReadOnly())
            .map(w -> w.getEndpoint())
            .collect(Collectors.toList());
    if (workers.isEmpty()) {
      throw new IOException("no available workers");
    }
    int index = rand.nextInt(workers.size());
    return workers.get(index);
  }
}
