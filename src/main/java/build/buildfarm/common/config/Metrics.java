package build.buildfarm.common.config;

import lombok.Data;

@Data
public class Metrics {
  public enum PUBLISHER {
    LOG,
    AWS,
    GCP
  }

  public enum LOG_LEVEL {
    SEVERE,
    WARNING,
    INFO,
    FINE,
    FINER,
    FINEST,
  }

  private LOG_LEVEL logLevel = LOG_LEVEL.FINEST;
  private String topic;
  private int topicMaxConnections;
  private String secretName;
}
