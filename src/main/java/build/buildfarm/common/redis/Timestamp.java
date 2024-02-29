package build.buildfarm.common.redis;

public class Timestamp {
  public Long getMillis() {
    return System.currentTimeMillis();
  }
  ;

  public Long getNanos() {
    return System.nanoTime();
  }
  ;
}
