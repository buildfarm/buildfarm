package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.io.File;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import lombok.Data;

@Data
public class Cas {
  private static final long DEFAULT_CAS_SIZE = 2147483648L; // 2 * 1024 * 1024 * 1024

  public enum TYPE {
    FILESYSTEM,
    GRPC,
    MEMORY,
    FUSE
  }

  private TYPE type = TYPE.FILESYSTEM;

  // MEMORY/FILESYSTEM
  private String path = "cache";
  private int hexBucketLevels = 0;
  private long maxSizeBytes = 0;
  private boolean fileDirectoriesIndexInMemory = false;
  private boolean skipLoad = false;

  // if creating a hardlink fails, copy the file instead
  private boolean execRootCopyFallback = false;

  // GRPC
  private String target;

  // Metrics
  private boolean publishTtlMetric = false;

  /*
  Automatically set disk space to 90% of available space on the worker volume.
  User configured value in .yaml will always take presedence.
   */
  public long getMaxSizeBytes() {
    if (maxSizeBytes == 0) {
      try {
        maxSizeBytes =
            (long)
                (new File(BuildfarmConfigs.getInstance().getWorker().getRoot()).getTotalSpace()
                    * 0.9);
      } catch (Exception e) {
        maxSizeBytes = DEFAULT_CAS_SIZE;
      }
    }
    return maxSizeBytes;
  }

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
