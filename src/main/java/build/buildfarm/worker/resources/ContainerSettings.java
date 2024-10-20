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

package build.buildfarm.worker.resources;

import java.util.ArrayList;

/**
 * @class ContainerSettings
 * @brief Some actions may need to run in a container. These settings are used to determine which
 *     container to use and how to use it.
 * @details These settings come from bazelbuild/bazel-toolchains. See the following:
 *     https://github.com/bazelbuild/bazel-toolchains/blob/dac71231098d891e5c4b74a2078fe9343feef510/rules/exec_properties/exec_properties.bzl#L143.
 *     Additional settings may be added as needed, but we should try to contribute these back to
 *     bazel-toolchains.
 */
public class ContainerSettings {
  /**
   * @field enabled
   * @brief Whether the action should run in the custom container.
   * @details This will be enabled when specific exec_properties are used. Global buildfarm settings
   *     may also effect whether the feature is available.
   */
  public boolean enabled = false;

  /**
   * @field containerImage
   * @brief The container image to use.
   * @details In example format may look like:
   *     docker://gcr.io/my-image@sha256:d7407d58cee310e7ab788bf4256bba704334630621d8507f3c9cf253c7fc664f.
   */
  public String containerImage = "";

  /**
   * @field addCapabilities
   * @brief Docker add capabilities.
   * @details Allows adding capabilities through --cap-add.
   */
  public ArrayList<String> addCapabilities = new ArrayList<>();

  /**
   * @field network
   * @brief Docker network.
   * @details Whether or not the container should allow network (standard, off).
   */
  public boolean network = false;

  /**
   * @field privileged
   * @brief Docker privileged.
   * @details Whether or not the container is given extended privileges (--privileged).
   */
  public boolean privileged = false;

  /**
   * @field runAsRoot
   * @brief Docker run as root.
   * @details Whether or not run the container as root.
   */
  public boolean runAsRoot = false;

  /**
   * @field runtime
   * @brief Docker runtime.
   * @details Runtime to use for the container (--runtime).
   */
  public String runtime = "";

  /**
   * @field shmSize
   * @brief Docker size of /dev/shm.
   * @details The format is <number><unit>. number must be greater than 0. Unit is optional and can
   *     be b (bytes), k (kilobytes), m (megabytes), or g (gigabytes). If you omit the unit, the
   *     system uses bytes. If you omit the size entirely, the system uses 64m.
   */
  public String shmSize = "";

  /**
   * @field siblingContainers
   * @brief Docker sibling containers.
   * @details Allow using sibling containers.
   */
  public boolean siblingContainers = false;

  /**
   * @field ulimits
   * @brief Docker ulimits.
   * @details Ulimits to set on the container (--ulimit).
   */
  public String ulimits = "";

  /**
   * @field useUrandom
   * @brief Docker use urandom.
   * @details Allow the container access to using urandom.
   */
  public boolean useUrandom = false;

  /**
   * @field description
   * @brief Description.
   * @details Description explaining why settings were chosen.
   */
  public ArrayList<String> description = new ArrayList<>();
}
