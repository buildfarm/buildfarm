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

package build.buildfarm.worker;

import build.buildfarm.worker.resources.ResourceLimits;
import com.google.protobuf.Duration;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * @class DockerExecutorSettings
 * @brief The information needed by the docker executor to execute an action inside a container.
 * @details These settings do not contain the docker client which is also needed to execute actions
 *     inside a container.
 */
public class DockerExecutorSettings {
  /**
   * @field executionContext
   * @brief Contains information about an action and where its results exist on a worker.
   * @details We need this for potentially extracting outputs out of the container.
   */
  public ExecutionContext executionContext;

  /**
   * @field execDir
   * @brief The execution root for the action.
   * @details This will be used to sync with the container environment.
   */
  public Path execDir;

  /**
   * @field limits
   * @brief Limitations imposed on the action.
   * @details These limitations also contain relevant container information such as image.
   */
  public ResourceLimits limits;

  /**
   * @field envVars
   * @brief EnvVars that should be made available to the action inside the container.
   * @details These are a combination of env vars from the action and limits.
   */
  public Map<String, String> envVars;

  /**
   * @field timeout
   * @brief When the action should timeout.
   * @details This is for the action specifically and not the setup/destruction of execution
   *     enviornment.
   */
  public Duration timeout;

  /**
   * @field fetchTimeout
   * @brief How long image fetching can take before timing out.
   * @details This waits on docker pull <image>.
   */
  public Duration fetchTimeout;

  /**
   * @field arguments
   * @brief The arguments that represent the action.
   * @details This will be spawned as is from inside the container.
   */
  public List<String> arguments;
}
