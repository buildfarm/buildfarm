package build.buildfarm.common.config;

import lombok.Data;

@Data
public class GrpcMetrics {
  private boolean enabled = false;
  private boolean provideLatencyHistograms = false;
}
