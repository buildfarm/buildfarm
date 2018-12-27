package build.buildfarm.proxy.http;

import build.buildfarm.common.UrlPath.InvalidResourceNameException;

interface WriteObserverSource {
  WriteObserver get(String resourceName) throws InvalidResourceNameException;

  WriteObserver remove(String resourceName);
}
