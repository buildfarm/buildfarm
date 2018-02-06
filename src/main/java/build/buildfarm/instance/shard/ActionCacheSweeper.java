// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.ShardBackplane.ActionCacheScanResult;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class ActionCacheSweeper implements Runnable {
  private final ShardBackplane backplane;
  private final Function<Iterable<Digest>, Iterable<Digest>> findMissingBlobs;
  private final int sweepPeriodSeconds;

  ActionCacheSweeper(
      ShardBackplane backplane,
      Function<Iterable<Digest>, Iterable<Digest>> findMissingBlobs,
      int sweepPeriodSeconds) {
    this.backplane = backplane;
    this.findMissingBlobs = findMissingBlobs;
    this.sweepPeriodSeconds = sweepPeriodSeconds;
  }

  public void sweep() throws IOException {
    List<Digest> outputDigests = new ArrayList<>();
    Map<Digest, Set<ActionKey>> outputDigestActionKeyDigests = new HashMap<>();
    ImmutableSet.Builder<ActionKey> expiredActionKeysBuilder = new ImmutableSet.Builder<>();

    String scanToken = null;

    do {
      ActionCacheScanResult scanResult = backplane.scanActionCache(scanToken, 25000);

      for (Map.Entry<ActionKey, ActionResult> actionCacheEntry : scanResult.entries) {
        ActionKey actionKey = actionCacheEntry.getKey();
        ActionResult actionResult = actionCacheEntry.getValue();

        // invalid action result value
        if (actionResult == null) {
          expiredActionKeysBuilder.add(actionKey);
        } else {
          for (OutputFile outputFile : actionResult.getOutputFilesList()) {
            Digest outputDigest = outputFile.getDigest();
            outputDigests.add(outputDigest);
            if (!outputDigestActionKeyDigests.containsKey(outputDigest)) {
              outputDigestActionKeyDigests.put(outputDigest, new HashSet<>());
            }
            outputDigestActionKeyDigests.get(outputDigest).add(actionKey);
            
            if (outputDigests.size() == 25000) {
              for (Digest missingOutputDigest : findMissingBlobs.apply(outputDigests)) {
                expiredActionKeysBuilder.addAll(outputDigestActionKeyDigests.get(missingOutputDigest));
              }
              outputDigests.clear();
              outputDigestActionKeyDigests.clear();
            }
          }
        }
      }

      scanToken = scanResult.token;
    } while (scanToken != null);

    if (outputDigests.size() > 0) {
      for (Digest missingOutputDigest : findMissingBlobs.apply(outputDigests)) {
        expiredActionKeysBuilder.addAll(outputDigestActionKeyDigests.get(missingOutputDigest));
      }
    }

    // maybe we don't need to aggregate this and can just ship it off incrementally
    Set<ActionKey> expiredActionKeys = expiredActionKeysBuilder.build();
    if (expiredActionKeys.size() > 0) {
      backplane.removeActionResults(expiredActionKeys);
    }
  }

  @Override
  public synchronized void run() {
    for (;;) {
      try {
        sweep();
        TimeUnit.SECONDS.sleep(sweepPeriodSeconds);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
