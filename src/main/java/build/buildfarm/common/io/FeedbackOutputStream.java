package build.buildfarm.common.io;

import java.io.OutputStream;

public abstract class FeedbackOutputStream extends OutputStream {
  public abstract boolean isReady();
}
