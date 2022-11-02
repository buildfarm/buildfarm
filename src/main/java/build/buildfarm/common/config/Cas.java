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

  private TYPE type = TYPE.FILESYSTEM;
  private String path = "cache";
  private long maxSizeBytes = 2147483648L; // 2 * 1024 * 1024 * 1024
  private boolean fileDirectoriesIndexInMemory = false;
  private boolean skipLoad = false;
  private String target;

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
