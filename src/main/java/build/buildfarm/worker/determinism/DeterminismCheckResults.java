// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.determinism;

/**
 * @class DeterminismCheckResults
 * @brief The results of running an action multiple times to decide whether it is deterministic.
 * @details In the event that an action is not deterministic these result contents should be
 *     sufficient for informing the client what the problem was. In particular, the client will want
 *     to know which output files were not deterministic and what the varying output hashes are. I
 *     considered storing the actual file contents for diffing, but this may be too large for
 *     certain actions so only the hashes are provided in the output. Hopefully just knowing that an
 *     action is not deterministic and which output files are involved is enough to debug issues.
 */
public class DeterminismCheckResults {
  /**
   * @field isDeterministic
   * @brief Determines whether the action that was tested was deterministic or not.
   * @details When true, we expect the action to succeed overall, otherwise fail with debug
   *     information.
   */
  public boolean isDeterministic = false;

  /**
   * @field determinisimFailMessage
   * @brief Information that is sent back to the client in the event that the action is not
   *     deterministic.
   * @details This information will show which file contents changed over action iteration and what
   *     those digests were.
   */
  public String determinisimFailMessage;
}
