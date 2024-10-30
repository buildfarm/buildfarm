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
