package build.buildfarm.common.config.yml;

import java.util.List;

public class ExecutionPolicy {
    private String name;
    private ExecutionWrapper executionWrapper;
    public class ExecutionWrapper {
        private String path;
        private List<String> arguments;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public List<String> getArguments() {
            return arguments;
        }

        public void setArguments(List<String> arguments) {
            this.arguments = arguments;
        }

        @Override
        public String toString() {
            return "ExecutionWrapper{" +
                    "path='" + path + '\'' +
                    ", arguments=" + arguments +
                    '}';
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExecutionWrapper getExecutionWrapper() {
        return executionWrapper;
    }

    public void setExecutionWrapper(ExecutionWrapper executionWrapper) {
        this.executionWrapper = executionWrapper;
    }

    @Override
    public String toString() {
        return "ExecutionPolicy{" +
                "name='" + name + '\'' +
                ", executionWrapper=" + executionWrapper +
                '}';
    }
}
