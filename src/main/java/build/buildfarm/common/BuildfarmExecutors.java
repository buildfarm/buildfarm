// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

import build.buildfarm.common.config.BuildfarmConfigs;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @class BuildfarmExecutors
 * @brief Group executor services to easier manage thread counts across software stack.
 * @details Thread usage is a global concern across the java application. Although these thread
 *     pools are used in different contexts we group them here so the application's threads can be
 *     managed and derived from the same class. Ideally we won't have any of these at all. Grpc does
 *     its threading thing, IO should be all async, and we only have as many threads as cores in a
 *     single per-process pool.
 */
public class BuildfarmExecutors {
  public static ExecutorService getScanCachePool() {
    String threadNameFormat = "scan-cache-pool-%d";
    return Executors.newFixedThreadPool(
        BuildfarmConfigs.getInstance().getExecutors().getNumScanCachePoolThreads(),
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ExecutorService getComputeCachePool() {
    String threadNameFormat = "compute-cache-pool-%d";
    return Executors.newFixedThreadPool(
        BuildfarmConfigs.getInstance().getExecutors().getNumComputeCachePoolThreads(),
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ExecutorService getRemoveDirectoryPool() {
    String threadNameFormat = "remove-directory-pool-%d";
    return Executors.newFixedThreadPool(
        BuildfarmConfigs.getInstance().getExecutors().getNumRemoveDirectoryPoolThreads(),
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ExecutorService getSubscriberPool() {
    String threadNameFormat = "subscriber-service-pool-%d";
    return Executors.newFixedThreadPool(
        BuildfarmConfigs.getInstance().getExecutors().getNumSubscriberPoolThreads(),
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ExecutorService getDequeuePool() {
    String threadNameFormat = "dequeue-pool-%d";
    return Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ListeningExecutorService getTransformServicePool() {
    String threadNameFormat = "transform-service-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            BuildfarmConfigs.getInstance().getExecutors().getNumTransformServicePoolThreads(),
            new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return listeningDecorator(pool);
  }

  public static ListeningExecutorService getDeprequeuePool() {
    String threadNameFormat = "deprequeue-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            BuildfarmConfigs.getInstance().getExecutors().getNumDeprequeuePoolThreads(),
            new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return listeningDecorator(pool);
  }

  public static ListeningExecutorService getActionCacheFetchServicePool() {
    String threadNameFormat = "action-cache-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            BuildfarmConfigs.getInstance()
                .getExecutors()
                .getNumActionCacheFetchServicePoolThreads(),
            new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return listeningDecorator(pool);
  }

  public static ExecutorService getFetchServicePool() {
    return Executors.newWorkStealingPool(
        BuildfarmConfigs.getInstance().getExecutors().getNumFetchServicePoolThreads());
  }
}
