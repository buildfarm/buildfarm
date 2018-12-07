package build.buildfarm.proxy.http;

abstract class CompleteWrite implements Write {
  @Override
  public boolean getComplete() {
    return true;
  }
}
