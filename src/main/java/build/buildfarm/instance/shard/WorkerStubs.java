// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.common.grpc.Channels.createChannel;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.protobuf.Duration;
import java.util.concurrent.TimeUnit;

public final class WorkerStubs {
  private WorkerStubs() {}

  @SuppressWarnings("rawtypes")
  public static LoadingCache create(DigestUtil digestUtil, Duration timeout) {
    return CacheBuilder.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .removalListener(
            (RemovalListener<String, Instance>)
                notification -> stopInstance(notification.getValue()))
        .build(
            new CacheLoader<String, Instance>() {
              @SuppressWarnings("NullableProblems")
              @Override
              public Instance load(String worker) {
                return newStubInstance(worker, digestUtil, timeout);
              }
            });
  }

  private static Instance newStubInstance(String worker, DigestUtil digestUtil, Duration timeout) {
    return new StubInstance(
        "",
        worker,
        digestUtil,
        createChannel(worker),
        timeout,
        newStubRetrier(),
        newStubRetryService());
  }

  private static Retrier newStubRetrier() {
    return new Retrier(
        Backoff.exponential(
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
            java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
            /*options.experimentalRemoteRetryMultiplier=*/ 2,
            /*options.experimentalRemoteRetryJitter=*/ 0.1,
            /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
        Retrier.DEFAULT_IS_RETRIABLE);
  }

  private static ListeningScheduledExecutorService newStubRetryService() {
    return listeningDecorator(newSingleThreadScheduledExecutor());
  }

  private static void stopInstance(Instance instance) {
    try {
      instance.stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
