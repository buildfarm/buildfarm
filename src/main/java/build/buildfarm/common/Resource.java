package build.buildfarm.common;

public interface Resource {
  void release(int amount);
  int availablePermits();
  boolean tryAcquire(int amount);
  Claim.Stage stage();
}
