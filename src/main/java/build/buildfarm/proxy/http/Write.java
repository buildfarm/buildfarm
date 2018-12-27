package build.buildfarm.proxy.http;

interface Write {
  long getCommittedSize();

  boolean getComplete();
}
