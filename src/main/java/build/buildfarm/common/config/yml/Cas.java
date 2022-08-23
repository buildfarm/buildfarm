package build.buildfarm.common.config.yml;

public class Cas {
    private String path;
    private long maxSizeBytes;
    private long maxEntrySizeBytes;
    private boolean fileDirectoriesIndexInMemory;
    private boolean skipLoad;

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

    @Override
    public String toString() {
        return "Cas{" +
                "path='" + path + '\'' +
                ", maxSizeBytes=" + maxSizeBytes +
                ", maxEntrySizeBytes=" + maxEntrySizeBytes +
                ", fileDirectoriesIndexInMemory=" + fileDirectoriesIndexInMemory +
                ", skipLoad=" + skipLoad +
                '}';
    }
}
