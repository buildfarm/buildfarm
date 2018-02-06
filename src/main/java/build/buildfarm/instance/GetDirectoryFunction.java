package build.buildfarm.instance;

import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import java.io.IOException;

@FunctionalInterface
public interface GetDirectoryFunction {
  Directory apply(Digest digest) throws InterruptedException, IOException;
}
