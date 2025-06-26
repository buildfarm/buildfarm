package build.buildfarm.common;

import build.bazel.remote.execution.v2.Platform;
import javax.annotation.Nullable;

public class StubClaimAcquirer implements ClaimAcquirer {
  @Override
  public @Nullable Claim acquireClaim(Platform platform) {
    return null;
  }
}
