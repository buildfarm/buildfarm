// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.SortedMap;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;

/** Much of the logic (hashing) is from Bazel itself (private library/methods, i.e. WorkerKey). */
public class Keymaker {
  // Constructs a key with its worker tool input files being relative paths
  public static WorkerKey make(
      Path opRoot,
      Path workRootsDir,
      ImmutableList<String> workerInitCmd,
      ImmutableList<String> workerInitArgs,
      ImmutableMap<String, String> workerEnv,
      String executionName,
      WorkerInputs workerFiles) {
    // Cancellation not yet supported; can change in the future,
    //  Presumably, following how Bazel's own persistent workers work
    boolean sandboxed = true;
    boolean cancellable = false;

    Path workRoot =
        calculateWorkRoot(
            workRootsDir,
            workerInitCmd,
            workerInitArgs,
            workerEnv,
            executionName,
            sandboxed,
            cancellable);
    Path toolsRoot = workRoot.resolve(PersistentWorker.TOOL_INPUT_SUBDIR);

    SortedMap<Path, HashCode> hashedTools = workerFilesWithHashes(workerFiles);
    HashCode combinedToolsHash = workerFilesCombinedHash(toolsRoot, hashedTools);

    return new WorkerKey(
        workerInitCmd,
        workerInitArgs,
        workerEnv,
        workRoot,
        executionName,
        combinedToolsHash,
        hashedTools,
        sandboxed,
        cancellable);
  }

  // Hash of a subset of the WorkerKey
  private static Path calculateWorkRoot(
      Path workRootsDir,
      ImmutableList<String> workerInitCmd,
      ImmutableList<String> workerInitArgs,
      ImmutableMap<String, String> workerEnv,
      String executionName,
      boolean sandboxed,
      boolean cancellable) {
    int workRootId = Objects.hash(workerInitCmd, workerInitArgs, workerEnv, sandboxed, cancellable);
    String workRootDirName = "work-root_" + executionName + "_" + workRootId;
    return workRootsDir.resolve(workRootDirName);
  }

  private static ImmutableSortedMap<Path, HashCode> workerFilesWithHashes(
      WorkerInputs workerFiles) {
    ImmutableSortedMap.Builder<Path, HashCode> workerFileHashBuilder =
        ImmutableSortedMap.naturalOrder();

    for (Path opPath : workerFiles.opToolInputs) {
      Path relPath = workerFiles.opRoot.relativize(opPath);

      HashCode toolInputHash = HashCode.fromBytes(workerFiles.digestFor(opPath).toByteArray());
      workerFileHashBuilder.put(relPath, toolInputHash);
    }

    return workerFileHashBuilder.build();
  }

  // Even though we hash the toolsRoot-resolved path, it doesn't exist yet.
  private static HashCode workerFilesCombinedHash(
      Path toolsRoot, SortedMap<Path, HashCode> hashedTools) {
    Hasher hasher = Hashing.sha256().newHasher();
    hashedTools.forEach(
        (relPath, toolHash) -> {
          hasher.putString(toolsRoot.resolve(relPath).toString(), StandardCharsets.UTF_8);
          hasher.putBytes(toolHash.asBytes());
        });
    return hasher.hash();
  }
}
