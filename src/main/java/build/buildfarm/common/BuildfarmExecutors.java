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
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public static ExecutorService getScanCachePool() {
    int nThreads = executorWidth();
    String threadNameFormat = "scan-cache-pool-%d";
    // We had workers locking up on startup so we lower the priority of the CAS loading threads.
    return Executors.newFixedThreadPool(
        nThreads,
        new ThreadFactoryBuilder()
            .setNameFormat(threadNameFormat)
            .setPriority(Thread.MIN_PRIORITY)
            .build());
  }

  public static ExecutorService getComputeCachePool() {
    int nThreads = executorWidth();
    String threadNameFormat = "compute-cache-pool-%d";
    // We had workers locking up on startup so we lower the priority of the CAS loading threads.
    return Executors.newFixedThreadPool(
        nThreads,
        new ThreadFactoryBuilder()
            .setNameFormat(threadNameFormat)
            .setPriority(Thread.MIN_PRIORITY)
            .build());
  }

  public static ExecutorService getRemoveDirectoryPool() {
    int nThreads = 32;
    String threadNameFormat = "remove-directory-pool-%d";
    return Executors.newFixedThreadPool(
        nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ExecutorService getSubscriberPool() {
    int nThreads = SystemProcessors.get();
    String threadNameFormat = "subscriber-service-pool-%d";
    return Executors.newFixedThreadPool(
        nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  public static ListeningExecutorService getTransformServicePool() {
    int nThreads = 24;
    String threadNameFormat = "transform-service-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return listeningDecorator(pool);
  }

  public static ListeningExecutorService getActionCacheFetchServicePool() {
    int nThreads = 24;
    String threadNameFormat = "action-cache-pool-%d";
    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return listeningDecorator(pool);
  }

  public static ExecutorService getFetchServicePool() {
    int nThreads = 128;
    return Executors.newWorkStealingPool(nThreads);
  }

  public static ExecutorService getOperationDequeuePool(int queueAmount) {
    int nThreads = queueAmount;
    String threadNameFormat = "operation-dequeue-%d";
    return Executors.newFixedThreadPool(
        nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
  }

  private static int executorWidth() {
    return Math.max(1, SystemProcessors.get() - configs.getWorker().getExecuteStageWidthOffset());
  }
}
