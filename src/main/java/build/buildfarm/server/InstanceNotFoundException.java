package build.buildfarm.server;

public class InstanceNotFoundException extends Exception {
  private static final long serialVersionUID = 1;

  public final String instanceName;

  InstanceNotFoundException( String instanceName ) {
    this.instanceName = instanceName;
  }
}
