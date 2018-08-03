package build.buildfarm.server;

public class ServiceNotFoundException extends Exception {
  private static final long serialVersionUID = 4376829927643439988L;

  public final String resourceName;

  ServiceNotFoundException(String resourceName) {
    this.resourceName = resourceName;
  }
}
