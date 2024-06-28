package build.buildfarm.common.config;

import lombok.Data;

@Data
public class CASMetrics {
  private boolean enabled = false;
  private String casReadCountSetName = "CasReadCount";
  private int casReadCountWindow = 14400; // 4 hours
  private int casReadCountUpdateInterval = 900; // 15 mins
}
