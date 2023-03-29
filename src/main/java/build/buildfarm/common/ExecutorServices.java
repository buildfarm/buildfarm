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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @class ExecutorServices
 * @brief Group executor services to easier manage thread counts.
 */
public class ExecutorServices {
  public static ExecutorService getScanCachePool() {
    int nThreads = SystemProcessors.get();
    String threadNameFormat = "scan-cache-pool-%d";

    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return pool;
  }

  public static ExecutorService getComputeCachePool() {
    int nThreads = SystemProcessors.get();
    String threadNameFormat = "compute-cache-pool-%d";

    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return pool;
  }

  public static ExecutorService getRemoveDirectoryPool() {
    int nThreads = 32;
    String threadNameFormat = "remove-directory-pool-%d";

    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return pool;
  }

  public static ExecutorService getSubscriberPool() {
    int nThreads = 32;
    String threadNameFormat = "subscriber-service-pool-%d";

    ExecutorService pool =
        Executors.newFixedThreadPool(
            nThreads, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build());
    return pool;
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
}
