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

package build.buildfarm.worker.shard;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

/**
 * Command-line options definition for Worker.
 */
public class WorkerOptions extends OptionsBase {

  @Option(
      name = "help",
      abbrev = 'h',
      help = "Prints usage info.",
      defaultValue = "true"
    )
  public boolean help;

  @Option(
      name = "root",
      help = "Root base directory for all work being performed.",
      defaultValue = ""
    )
  public String root;

  @Option(
      name = "cas_cache_directory",
      help = "(Absolute or relative to root) path to cached files from CAS.",
      defaultValue = ""
    )
  public String casCacheDirectory;
}

