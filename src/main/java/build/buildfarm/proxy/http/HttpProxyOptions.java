// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.proxy.http;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

/** Command-line options definition for example server. */
public class HttpProxyOptions extends OptionsBase {
  @Option(name = "help", abbrev = 'h', help = "Prints usage info.", defaultValue = "true")
  public boolean help;

  @Option(name = "port", abbrev = 'p', help = "Port to use.", defaultValue = "-1")
  public int port;

  @Option(
      name = "timeout",
      abbrev = 't',
      defaultValue = "60",
      help = "The maximum number of seconds to wait for remote execution and cache calls.")
  public int timeout;

  @Option(
      name = "http_cache",
      abbrev = 'c',
      defaultValue = "null",
      help =
          "A base URL of a HTTP caching service. Both http:// and https:// are "
              + "supported. Resources are stored with PUT and retrieved with "
              + "GET. BLOBs are stored under the path /cas/<hash>. Results are "
              + "stored under the path /ac/<actionKey>.")
  public String httpCache;

  @Option(
      name = "readonly",
      abbrev = 'r',
      defaultValue = "true",
      help = "Whether or not the http_cache is read-only. Defaults to true.")
  public boolean readOnly;

  @Option(
      name = "tree_default_page_size",
      defaultValue = "1024",
      help = "The default number of directories per tree page.")
  public int treeDefaultPageSize;

  @Option(
      name = "tree_max_page_size",
      defaultValue = "16384",
      help = "The maximum number of directories per tree page.")
  public int treeMaxPageSize;
}
