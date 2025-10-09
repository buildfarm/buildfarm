// Copyright 2023-2024 The Buildfarm Authors. All rights reserved.
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

package persistent.testutil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import java.nio.file.Path;
import persistent.bazel.client.WorkerKey;

public class WorkerUtils {
  public static WorkerKey emptyWorkerKey(Path execDir, ImmutableList<String> initCmd) {
    return new WorkerKey(
        initCmd,
        ImmutableList.of(),
        ImmutableMap.of(),
        execDir,
        "TestOp-Adder",
        HashCode.fromInt(0),
        ImmutableSortedMap.of(),
        false,
        false);
  }
}
