package build.buildfarm.common.config.yml;

import java.util.Arrays;

public class Queue {
    private String name;
    private boolean allowUnmatched;
    private Property[] properties;

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

    public Property[] getProperties() {
        return properties;
    }

    public void setProperties(Property[] properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "Queue{" +
                "name='" + name + '\'' +
                ", allowUnmatched=" + allowUnmatched +
                ", properties=" + Arrays.toString(properties) +
                '}';
    }
}
