package persistent.common.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Args {
  public static final String ARGSFILE_START = "@";

  public static final String ARGSFILE_ESCAPE = "@@";

  public static boolean isArgsFile(String arg) {
    return arg.startsWith(ARGSFILE_START) && !arg.startsWith(ARGSFILE_ESCAPE);
  }

  // Expand arguments of argsfiles
  public static List<String> expandArgsfiles(List<String> args) throws IOException {
    List<String> res = new ArrayList<>();
    for (String arg : args) {
      if (isArgsFile(arg)) {
        res.addAll(readAllLines(arg));
      } else {
        res.add(arg);
      }
    }
    return res;
  }

  public static List<String> readAllLines(String fileArg) throws IOException {
    String file = isArgsFile(fileArg) ? fileArg.substring(1) : fileArg;

    return Files.readAllLines(Paths.get(file));
  }
}
