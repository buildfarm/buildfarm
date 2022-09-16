package build.buildfarm.common.config;

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
  private String path;
  private long maxSizeBytes;
  private long maxEntrySizeBytes;
  private boolean fileDirectoriesIndexInMemory;
  private boolean skipLoad;
  private String target;
}
