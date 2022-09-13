package build.buildfarm.common.config.yml;

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
  private long maxEntrySizeBytes = 2147483648L; // 2 * 1024 * 1024 * 1024
  private boolean fileDirectoriesIndexInMemory = false;
  private boolean skipLoad = false;
  private String target;
}
