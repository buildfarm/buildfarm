package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class Metrics {
  private String publisher = "log";
  private String logLevel = "FINE";
  private String topic;
  private int topicMaxConnections;
  private String secretName;
}
