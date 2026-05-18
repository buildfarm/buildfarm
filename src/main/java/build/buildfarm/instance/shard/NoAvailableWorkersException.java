package build.buildfarm.instance.shard;

import java.io.IOException;

class NoAvailableWorkersException extends IOException {
  NoAvailableWorkersException() {
    super("no available workers");
  }
}
