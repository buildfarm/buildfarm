package build.buildfarm.server;

class InstanceNotFoundException extends Exception {
  public final String instanceName;

  InstanceNotFoundException( String instanceName ) {
    this.instanceName = instanceName;
  }
}
