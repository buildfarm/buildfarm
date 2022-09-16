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
    OFF,
    SEVERE,
    WARNING,
    INFO,
    FINE,
    FINER,
    FINEST,
    ALL
  }

  private PUBLISHER publisher;
  private LOG_LEVEL logLevel;
  private String topic;
  private int topicMaxConnections;
  private String secretName;
}
