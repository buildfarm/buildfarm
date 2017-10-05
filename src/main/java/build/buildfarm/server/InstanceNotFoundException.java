package build.buildfarm.server;

class InstanceNotFoundException extends Exception {
  private final String instanceName;

  InstanceNotFoundException( String instanceName ) {
    this.instanceName = instanceName;
  }
}
