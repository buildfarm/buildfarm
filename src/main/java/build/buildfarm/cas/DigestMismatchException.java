package build.buildfarm.cas;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import java.io.IOException;

class DigestMismatchException extends IOException {
  private final Digest actual;
  private final Digest expected;

  DigestMismatchException(Digest actual, Digest expected) {
    super(
        String.format(
            "computed digest %s does not match expected %s",
            DigestUtil.toString(actual),
            DigestUtil.toString(expected)));
    this.actual = actual;
    this.expected = expected;
  }
}
