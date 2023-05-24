// Copyright 2019 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.cas;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import java.io.IOException;
import java.net.InetAddress;
import com.google.common.base.Strings;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class DigestMismatchException extends IOException {
  private final Digest actual;
  private final Digest expected;

  // Not great - consider using publicName if we upstream
  private static String hostname = null;
  private static String getHostname() {
    if (!Strings.isNullOrEmpty(hostname)) {
      return hostname;
    }
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      hostname = "_unknown_host_";
    }
    return hostname;
  }


  public DigestMismatchException(Digest actual, Digest expected, String context) {
    super(String.format(
            "[%s] computed digest %s does not match expected %s : context %s",
            DigestMismatchException.getHostname(), DigestUtil.toString(actual), DigestUtil.toString(expected), context));
      String msg = String.format(
            "[%s] computed digest %s does not match expected %s : context %s",
            DigestMismatchException.getHostname(), DigestUtil.toString(actual), DigestUtil.toString(expected), context);
      log.log(Level.SEVERE, "DigestMismatchException " + msg);

    this.actual = actual;
    this.expected = expected;
  }

  public Digest getActual() {
    return actual;
  }

  public Digest getExpected() {
    return expected;
  }
}
