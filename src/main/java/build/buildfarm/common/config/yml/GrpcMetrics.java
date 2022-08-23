package build.buildfarm.common.config.yml;

public class GrpcMetrics {
    private boolean enabled = false;
    private boolean provideLatencyHistograms = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isProvideLatencyHistograms() {
        return provideLatencyHistograms;
    }

    public void setProvideLatencyHistograms(boolean provideLatencyHistograms) {
        this.provideLatencyHistograms = provideLatencyHistograms;
    }

    @Override
    public String toString() {
        return "GrpcMetrics{" +
                "enabled=" + enabled +
                ", provideLatencyHistograms=" + provideLatencyHistograms +
                '}';
    }
}
