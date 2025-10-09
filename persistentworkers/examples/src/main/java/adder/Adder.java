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

package adder;

import com.google.common.collect.ImmutableList;
import java.util.List;
import persistent.bazel.processes.WorkRequestHandler;

/** Example of a service which supports being run as a persistent worker */
public class Adder {
  public static void main(String[] args) {
    if (args.length == 1 && args[0].equals("--persistent_worker")) {
      WorkRequestHandler handler = initialize();
      int exitCode = handler.processForever(System.in, System.out, System.err);
      System.exit(exitCode);
    }
  }

  private static WorkRequestHandler initialize() {
    return new WorkRequestHandler(
        (actionArgs, pw) -> {
          String res = work(actionArgs);
          pw.write(res);
          return 0;
        });
  }

  private static String work(List<String> args) {
    if (args.size() == 1 && args.get(0).equals("stop!")) {
      System.err.println("exiting!");
      System.exit(0);
    } else if (args.size() != 2) {
      System.err.println("Cannot handle args: " + ImmutableList.copyOf(args).toString());
      System.exit(2);
    }
    int a = Integer.parseInt(args.get(0));
    int b = Integer.parseInt(args.get(1));
    return String.valueOf(compute(a, b));
  }

  private static int compute(int a, int b) {
    return a + b;
  }
}
