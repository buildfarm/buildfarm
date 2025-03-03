package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
public class Cas {
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

  private int maxHardLinkCount = 0;
  // if creating a hardlink fails, copy the file instead
  private boolean execRootCopyFallback = false;

  // GRPC
  private String target;
  private boolean readonly = false;

  @Getter(AccessLevel.NONE)
  private boolean publishTtlMetric = false; // deprecated

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
