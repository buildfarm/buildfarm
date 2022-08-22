package build.buildfarm.common.config.yml;

public class GrpcMetrics {
    private boolean enabled;
    private boolean provideLatencyHistograms;

    public boolean getEnabled() { return enabled; }
    public void setEnabled(boolean value) { this.enabled = value; }
    public boolean getProvideLatencyHistograms() { return provideLatencyHistograms; }
    public void setProvideLatencyHistograms(boolean value) { this.provideLatencyHistograms = value; }

    public String toString() {
        return "\n" +
                "\t\tenabled: " + enabled + "\n" +
                "\t\tprovideLatencyHistograms: " + provideLatencyHistograms;
    }
}
