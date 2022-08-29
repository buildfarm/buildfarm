package build.buildfarm.common.config.yml;

import build.bazel.remote.execution.v2.Platform;

public class Queue {
    private String name;
    private boolean allowUnmatched;

    private Platform platform = Platform.getDefaultInstance();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAllowUnmatched() {
        return allowUnmatched;
    }

    public void setAllowUnmatched(boolean allowUnmatched) {
        this.allowUnmatched = allowUnmatched;
    }

    public Platform getPlatform() {
        return platform;
    }

    public void setPlatform(Platform platform) {
        this.platform = platform;
    }

    @Override
    public String toString() {
        return "Queue{" +
                "name='" + name + '\'' +
                ", allowUnmatched=" + allowUnmatched +
                ", platform=" + platform +
                '}';
    }
}
