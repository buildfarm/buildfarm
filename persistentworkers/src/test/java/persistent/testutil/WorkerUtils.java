package persistent.testutil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import java.nio.file.Path;
import persistent.bazel.client.WorkerKey;

public class WorkerUtils {
  public static WorkerKey emptyWorkerKey(Path execDir, ImmutableList<String> initCmd) {
    return new WorkerKey(
        initCmd,
        ImmutableList.of(),
        ImmutableMap.of(),
        execDir,
        "TestOp-Adder",
        HashCode.fromInt(0),
        ImmutableSortedMap.of(),
        false,
        false);
  }
}
