package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import lombok.Data;

@Data
public class Cas {
  public enum TYPE {
    FILESYSTEM,
    GRPC,
    MEMORY,
    FUSE
  }

  private TYPE type;

  // MEMORY/FILESYSTEM
  private String path;
  private int hexBucketLevels;
  private long maxSizeBytes;
  private boolean fileDirectoriesIndexInMemory;
  private boolean skipLoad;

  // if creating a hardlink fails, copy the file instead
  private boolean execRootCopyFallback;

  // GRPC
  private String target;
  private boolean readonly;

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
