package build.buildfarm.common;

import build.bazel.remote.execution.v2.Platform;
import javax.annotation.Nullable;

public interface ClaimAcquirer {
  @Nullable
  Claim acquireClaim(Platform platform);
}
