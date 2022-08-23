package build.buildfarm.common.config.yml;

public class Metrics {
    private String logLevel;

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    @Override
    public String toString() {
        return "Metrics{" +
                "logLevel='" + logLevel + '\'' +
                '}';
    }
}
