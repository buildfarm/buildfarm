package build.buildfarm.common.config.yml;

public class Cas {
    private String path = "cache";
    private long maxSizeBytes = 2147483648L; //2 * 1024 * 1024 * 1024
    private long maxEntrySizeBytes = 2147483648L; //2 * 1024 * 1024 * 1024
    private boolean fileDirectoriesIndexInMemory = false;
    private boolean skipLoad = false;
    private String type = "FILESYSTEM";

    private String target;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    public void setMaxSizeBytes(long maxSizeBytes) {
        this.maxSizeBytes = maxSizeBytes;
    }

    public long getMaxEntrySizeBytes() {
        return maxEntrySizeBytes;
    }

    public void setMaxEntrySizeBytes(long maxEntrySizeBytes) {
        this.maxEntrySizeBytes = maxEntrySizeBytes;
    }

    public boolean isFileDirectoriesIndexInMemory() {
        return fileDirectoriesIndexInMemory;
    }

    public void setFileDirectoriesIndexInMemory(boolean fileDirectoriesIndexInMemory) {
        this.fileDirectoriesIndexInMemory = fileDirectoriesIndexInMemory;
    }

    public boolean isSkipLoad() {
        return skipLoad;
    }

    public void setSkipLoad(boolean skipLoad) {
        this.skipLoad = skipLoad;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @Override
    public String toString() {
        return "Cas{" +
                "path='" + path + '\'' +
                ", maxSizeBytes=" + maxSizeBytes +
                ", maxEntrySizeBytes=" + maxEntrySizeBytes +
                ", fileDirectoriesIndexInMemory=" + fileDirectoriesIndexInMemory +
                ", skipLoad=" + skipLoad +
                ", type='" + type + '\'' +
                ", target='" + target + '\'' +
                '}';
    }
}
