// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import java.util.ArrayList;

/**
 * @class LinuxSandboxOptions
 * @brief These options correlate to the CLI arguments that need passed to bazel's linux sandbox.
 * @details This data structure is intended to match bazel's linux sandbox options. See the
 *     following permalink this was implemented off of:
 *     https://github.com/bazelbuild/bazel/blob/e8a6db03d2f279910297d2ac2657ca3202241711/src/main/tools/linux-sandbox-options.h.
 */
public class LinuxSandboxOptions {
  /**
   * @field workingDir
   * @brief Working directory.
   * @details Flag value: -W.
   */
  public String workingDir = "";

  /**
   * @field timeoutSecs
   * @brief How long to wait before killing the child.
   * @details Flag value: -T.
   */
  public int timeoutSecs = 0;

  /**
   * @field killDelaySecs
   * @brief How long to wait before sending SIGKILL in case of timeout.
   * @details Flag value: -t.
   */
  public int killDelaySecs = 0;

  /**
   * @field sigintSendsSigterm
   * @brief Send a SIGTERM to the child on receipt of a SIGINT.
   * @details Flag value: -i.
   */
  public boolean sigintSendsSigterm = false;

  /**
   * @field stdoutPath
   * @brief Where to redirect stdout.
   * @details Flag value: -l.
   */
  public String stdoutPath = "";

  /**
   * @field stderrPath
   * @brief Where to redirect stderr.
   * @details Flag value: -L.
   */
  public String stderrPath = "";

  /**
   * @field writableFiles
   * @brief Files or directories to make writable for the sandboxed process.
   * @details Flag value: -w.
   */
  public final ArrayList<String> writableFiles = new ArrayList<>();

  /**
   * @field tmpfsDirs
   * @brief Directories where to mount an empty tmpfs.
   * @details Flag value: -e.
   */
  public final ArrayList<String> tmpfsDirs = new ArrayList<>();

  /**
   * @field bindMountSources
   * @brief Source of files or directories to explicitly bind mount in the sandbox.
   * @details Flag value: -M.
   */
  public ArrayList<String> bindMountSources = new ArrayList<>();

  /**
   * @field bindMountTargets
   * @brief Target of files or directories to explicitly bind mount in the sandbox.
   * @details Flag value: -m.
   */
  public ArrayList<String> bindMountTargets = new ArrayList<>();

  /**
   * @field statsPath
   * @brief Where to write stats, in protobuf format.
   * @details Flag value: -S.
   */
  public String statsPath = "";

  /**
   * @field fakeHostname
   * @brief Set the hostname inside the sandbox to 'localhost'.
   * @details Flag value: -H.
   */
  public boolean fakeHostname = false;

  /**
   * @field createNetns
   * @brief Create a new network namespace.
   * @details Flag value: -N.
   */
  public boolean createNetns = false;

  /**
   * @field fakeRoot
   * @brief Pretend to be root inside the namespace.
   * @details Flag value: -R.
   */
  public boolean fakeRoot = false;

  /**
   * @field fakeUsername
   * @brief Set the username inside the sandbox to 'nobody'.
   * @details Flag value: -U.
   */
  public boolean fakeUsername = false;

  /**
   * @field debug
   * @brief Print debugging messages.
   * @details Flag value: -D.
   */
  public boolean debug = false;

  /**
   * @field hermetic
   * @brief Run the sandbox in hermetic mode.
   * @details Flag value: -h.
   * if set, chroot to sandbox-dir and only  mount whats been specified with
   * -M/-m for improved hermeticity.
   */
  public boolean hermetic = false;
}
