package build.buildfarm.common.config;

import build.buildfarm.common.SystemProcessors;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.java.Log;

@Data
@Log
public class Executors {
  @Getter(AccessLevel.NONE)
  private int numScanCachePoolThreads = -1;

  @Getter(AccessLevel.NONE)
  private int numComputeCachePoolThreads = -1;

  private int numRemoveDirectoryPoolThreads = 32;
  private int numSubscriberPoolThreads = 32;
  private int numTransformServicePoolThreads = 24;
  private int numDeprequeuePoolThreads = 12;
  private int numActionCacheFetchServicePoolThreads = 24;
  private int numFetchServicePoolThreads = 128;

  public int getNumScanCachePoolThreads() {
    if (numScanCachePoolThreads == -1) {
      return SystemProcessors.get();
    }
    return numScanCachePoolThreads;
  }

  public int getNumComputeCachePoolThreads() {
    if (numScanCachePoolThreads == -1) {
      return SystemProcessors.get();
    }
    return numScanCachePoolThreads;
  }
}
