// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.server.services.OperationsService.LIST_OPERATIONS_MAXIMUM_PAGE_SIZE;

import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/** Display a live histogram of dispatched operations. */
@Command(
    name = "hist",
    mixinStandardHelpOptions = true,
    description = "Display a live histogram of dispatched operations")
class Hist implements Callable<Integer> {

  @Parameters(index = "0", description = "The instance name")
  private String instanceName;

  @Parameters(
      index = "1",
      description =
          "The [scheme://]host:port of the buildfarm server. Scheme should be 'grpc://',\""
              + " 'grpcs://', or omitted (default 'grpc://')")
  private String host;

  @Override
  @SuppressWarnings("BusyWait")
  public Integer call() throws Exception {
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, /* digestUtil= */ null, channel);
    try {
      //noinspection InfiniteLoopStatement
      for (; ; ) {
        printHistogram(instance);
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      instance.stop();
    }
    return 0;
  }

  @SuppressWarnings("ConstantConditions")
  private static void printHistogramValue(int dispatched) {
    StringBuilder s = new StringBuilder();
    int p = 0;
    int n = 5;
    while (p > 0) {
      p /= 10;
      n++;
    }
    int h = dispatched > 100 ? (100 - n) : dispatched;
    for (int i = 0; i < h; i++) {
      s.append('#');
    }
    if (dispatched > 100) {
      s.replace(0, n, "# (" + dispatched + ") ");
    }
    System.out.println(s.toString());
  }

  @SuppressWarnings("CatchMayIgnoreException")
  private static void printHistogram(Instance instance) throws IOException {
    AtomicInteger dispatched = new AtomicInteger(0);

    String pageToken = Instance.SENTINEL_PAGE_TOKEN;
    do {
      pageToken =
          instance.listOperations(
              "executions",
              LIST_OPERATIONS_MAXIMUM_PAGE_SIZE,
              pageToken,
              "status=dispatched",
              name -> dispatched.incrementAndGet());
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
    printHistogramValue(dispatched.get());
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Hist()).execute(args);
    System.exit(exitCode);
  }
}
