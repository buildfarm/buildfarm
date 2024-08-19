// Copyright 2022 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.net;

import java.io.IOException;
import java.net.URLConnection;

// solely exists so that we can mock this
public class URL {
  private final java.net.URL url;

  public URL(java.net.URL url) {
    this.url = url;
  }

  public String getHost() {
    return url.getHost();
  }

  public URLConnection openConnection() throws IOException {
    return url.openConnection();
  }
}
