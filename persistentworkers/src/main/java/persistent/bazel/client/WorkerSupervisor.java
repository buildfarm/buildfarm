package persistent.bazel.client;

import com.google.common.hash.HashCode;
import java.nio.file.Path;
import java.util.Optional;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import persistent.common.CommonsSupervisor;

public abstract class WorkerSupervisor extends CommonsSupervisor<WorkerKey, PersistentWorker> {
  private final Logger logger = Logger.getLogger(this.getClass().getName());
  private final WorkerIndex workerIndex;

  public WorkerSupervisor(WorkerIndex workerIndex) {
    this.workerIndex = workerIndex;
  }

  public abstract PersistentWorker createUnderlying(WorkerKey workerKey) throws Exception;

  public final PersistentWorker create(WorkerKey key) throws Exception {
    PersistentWorker worker = createUnderlying(key);

    // The worker is about to be entered into the pool, so add it to the index
    workerIndex.registerWorker(worker);

    return worker;
  }

  @Override
  public PooledObject<PersistentWorker> wrap(PersistentWorker persistentWorker) {
    return new DefaultPooledObject<>(persistentWorker);
  }

  @Override
  public boolean validateObject(WorkerKey key, PooledObject<PersistentWorker> p) {
    PersistentWorker worker = p.getObject();
    Optional<Integer> exitValue = worker.getExitValue();
    if (exitValue.isPresent()) {
      String errorStr;
      try {
        String err = worker.getStdErr();
        errorStr = "Stderr:\n" + err;
      } catch (Exception e) {
        errorStr = "Couldn't read Stderr: " + e;
      }
      String msg =
          String.format(
              "Worker unexpectedly died with exit code %d. Key:\n%s\n%s",
              exitValue.get(), key, errorStr);
      logger.log(Level.SEVERE, msg);
      return false;
    }

    WorkerKey currentWorkerKey = worker.getKey();
    boolean filesChanged =
        !key.getWorkerFilesCombinedHash().equals(currentWorkerKey.getWorkerFilesCombinedHash());

    if (filesChanged) {
      StringBuilder msg = new StringBuilder();
      msg.append("Worker can no longer be used, because its files have changed on disk:\n");
      msg.append(key);
      TreeSet<Path> files = new TreeSet<>();
      files.addAll(key.getWorkerFilesWithHashes().keySet());
      files.addAll(currentWorkerKey.getWorkerFilesWithHashes().keySet());
      for (Path file : files) {
        HashCode oldHash = currentWorkerKey.getWorkerFilesWithHashes().get(file);
        HashCode newHash = key.getWorkerFilesWithHashes().get(file);
        if (!oldHash.equals(newHash)) {
          msg.append("\n")
              .append(file.normalize())
              .append(": ")
              .append(oldHash != null ? oldHash : "<none>")
              .append(" -> ")
              .append(newHash != null ? newHash : "<none>");
        }
      }
      logger.log(Level.SEVERE, msg.toString());
    }

    return !filesChanged;
  }
}
