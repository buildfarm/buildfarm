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
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Cancel operations on the server. */
@Command(
    name = "cancel",
    mixinStandardHelpOptions = true,
    description = "Cancel operations on the buildfarm server")
class Cancel implements Callable<Integer> {

  @Parameters(
      index = "0",
      description =
          "The [scheme://]host:port of the buildfarm server. Scheme should be 'grpc://',"
              + " 'grpcs://', or omitted (default 'grpc://')")
  private String host;

  @Parameters(index = "1", description = "The instance name")
  private String instanceName;

  @Parameters(
      index = "2..*",
      arity = "0..*",
      description = "Operation names to cancel. If omitted, reads from stdin (one per line)")
  private List<String> operationNames;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  private boolean helpRequested;

  @Override
  public Integer call() throws Exception {
    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);
    try {
      if (operationNames != null && !operationNames.isEmpty()) {
        for (String operationName : operationNames) {
          instance.cancelOperation(operationName);
        }
      } else {
        // read them from STDIN, separated by newlines
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
          instance.cancelOperation(scanner.nextLine());
        }
      }
    } finally {
      instance.stop();
      channel.awaitTermination(1, SECONDS);
    }
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cancel()).execute(args);
    System.exit(exitCode);
  }
}
