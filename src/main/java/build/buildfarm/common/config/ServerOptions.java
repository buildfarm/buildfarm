// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.config;

import com.google.devtools.common.options.Option;

/** Command-line options definition for example server. */
public class ServerOptions extends BuildfarmOptions {
  @Option(name = "port", abbrev = 'p', help = "Port to use.", defaultValue = "-1")
  public int port;

  @Option(name = "public_name", abbrev = 'n', help = "Name of this server.", defaultValue = "")
  public String publicName;
}
