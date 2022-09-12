package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class Metrics {
  public enum PUBLISHER {
    LOG,
    AWS,
    GCP
  }

  public enum LOG_LEVEL {
    OFF,
    SEVERE,
    WARNING,
    INFO,
    FINE,
    FINER,
    FINEST,
    ALL
  }

  private PUBLISHER publisher = PUBLISHER.LOG;
  private LOG_LEVEL logLevel = LOG_LEVEL.OFF;
  private String topic;
  private int topicMaxConnections;
  private String secretName;
}
