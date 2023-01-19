package persistent.common.processes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class JavaProcessWrapper extends ProcessWrapper {

  public JavaProcessWrapper(
      Path workDir, String classPath, String fullClassName, String[] args
  ) throws IOException {
    super(workDir, cmdArgs(
        new String[]{
            "java",
            "-cp",
            classPath,
            fullClassName
        },
        args
    ));
  }

  public static ImmutableList<String> cmdArgs(String[] cmd, String[] args) {
      List<String> resultList = new ArrayList<>(cmd.length + args.length);
      Collections.addAll(resultList, cmd);
      Collections.addAll(resultList, args);
      return ImmutableList.copyOf(resultList);
  }
}
