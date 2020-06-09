package build.buildfarm.worker.cgroup;

import java.io.IOException;

public class Cpu extends Controller {
  Cpu(Group group) {
    super(group);
  }

  @Override
  public String getName() {
    return "cpu";
  }

  public int getShares() throws IOException {
    open();
    return readInt("cpu.shares");
  }

  public void setShares(int shares) throws IOException {
    open();
    writeInt("cpu.shares", shares);
  }

  public int getCFSPeriod() throws IOException {
    open();
    return readInt("cpu.cfs_period_us");
  }

  public void setCFSPeriod(int microseconds) throws IOException {
    open();
    writeInt("cpu.cfs_period_us", microseconds);
  }

  public void setCFSQuota(int microseconds) throws IOException {
    open();
    writeInt("cpu.cfs_quota_us", microseconds);
  }
}
