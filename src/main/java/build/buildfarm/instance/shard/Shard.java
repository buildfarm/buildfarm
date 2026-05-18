package build.buildfarm.instance.shard;

import build.buildfarm.backplane.Backplane;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public final class Shard {
  private Shard() {}

  public static String getRandomWritingWorker(Backplane backplane, Random rand) throws IOException {
    List<String> workers =
        backplane.getStorageWorkers().stream()
            .filter(w -> !w.getReadOnly())
            .map(w -> w.getEndpoint())
            .collect(Collectors.toList());
    if (workers.isEmpty()) {
      throw new IOException("no available workers");
    }
    int index = rand.nextInt(workers.size());
    return workers.get(index);
  }
}
