package persistent.testutil;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import persistent.bazel.client.PersistentWorker;
import persistent.common.processes.JavaProcessWrapper;

public class ProcessUtils {

  /**
   * Creates a new java process with the specified classpath and classname (with the main method),
   * passing along the --persistent_worker flag.
   */
  public static JavaProcessWrapper spawnPersistentWorkerProcess(String classpath, String className)
      throws IOException {
    JavaProcessWrapper jpw =
        new JavaProcessWrapper(
            Paths.get("."),
            classpath,
            className,
            new String[] {PersistentWorker.PERSISTENT_WORKER_FLAG});
    assertThat(jpw.isAlive()).isTrue();
    return jpw;
  }

  public static JavaProcessWrapper spawnPersistentWorkerProcess(Path jar, String className)
      throws IOException {
    return spawnPersistentWorkerProcess(jar.toString(), className);
  }

  public static JavaProcessWrapper spawnPersistentWorkerProcess(String classpath, Class<?> clazz)
      throws IOException {
    String className = clazz.getPackage().getName() + "." + clazz.getSimpleName();
    return spawnPersistentWorkerProcess(classpath, className);
  }

  public static Path retrieveFileResource(ClassLoader classLoader, String filename, Path targetPath)
      throws IOException {

    InputStream is = classLoader.getResourceAsStream(filename);

    Files.write(targetPath, IOUtils.toByteArray(is));

    return targetPath;
  }
}
