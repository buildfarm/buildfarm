package build.buildfarm.server;

public class InstanceNotFoundException extends Exception {
  private static final long serialVersionUID = 2288236781501706181L;

  public final String instanceName;

  InstanceNotFoundException(String instanceName) {
    this.instanceName = instanceName;
  }
}
