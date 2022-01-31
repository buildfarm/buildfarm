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

package build.buildfarm.common.config;

import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.v1test.WorkerConfig;
import com.google.common.base.Strings;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @class ConfigAdjuster
 * @brief Modify the loaded configuration to avoid any unsuitable defaults.
 * @details Users may exclude certain options from their configuration files. The default values
 *     given to these options may also be invalid. Since protobuf does not allow custom defaults, we
 *     will adjust accordingly after a configuration is loaded.
 */
public class ConfigAdjuster {
  private static final Logger logger = Logger.getLogger(ConfigAdjuster.class.getName());

  /**
   * @brief Adjust.
   * @details Adjust.
   * @param builder The loaded configuration.
   * @param options Worker options.
   * @note Overloaded.
   */
  public static void adjust(ShardWorkerConfig.Builder builder, ShardWorkerOptions options) {
    // Handle env overrides.  A typical pattern for docker builds.
    String redisURI = System.getenv("REDIS_URI");
    if (redisURI != null) {
      logger.log(Level.INFO, String.format("Overwriting redis URI: %s", redisURI));
      builder.getRedisShardBackplaneConfigBuilder().setRedisUri(redisURI);
    }
    String instanceName = System.getenv("INSTANCE_NAME");
    if (instanceName != null) {
      logger.log(Level.INFO, String.format("Overwriting public name: %s", instanceName));
      builder.setPublicName(instanceName);
    }

    if (!Strings.isNullOrEmpty(options.root)) {
      logger.log(Level.INFO, "setting root from CLI: " + options.root);
      builder.setRoot(options.root);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      logger.log(Level.INFO, "public name from CLI: " + options.publicName);
      builder.setPublicName(options.publicName);
    }

    if (!builder.getShardWorkerInstanceConfig().hasGrpcTimeout()) {
      Duration defaultDuration = Durations.fromSeconds(60);
      builder.setShardWorkerInstanceConfig(
          builder.getShardWorkerInstanceConfigBuilder().setGrpcTimeout(defaultDuration).build());
      logger.log(
          Level.INFO,
          "grpc timeout not configured.  Setting to: " + defaultDuration.getSeconds() + "s");
    }

    builder.setExecuteStageWidth(
        adjustExecuteStageWidth(
            builder.getExecuteStageWidth(), builder.getExecuteStageWidthOffset()));
  }

  /**
   * @brief Adjust.
   * @details Adjust.
   * @param builder The loaded configuration.
   * @param options Worker options.
   * @note Overloaded.
   */
  public static void adjust(WorkerConfig.Builder builder, MemoryWorkerOptions options) {
    if (!Strings.isNullOrEmpty(options.root)) {
      logger.log(Level.INFO, "setting root from CLI: " + options.root);
      builder.setRoot(options.root);
    }

    if (!Strings.isNullOrEmpty(options.casCacheDirectory)) {
      logger.log(Level.INFO, "casCacheDirectory from CLI: " + options.casCacheDirectory);
      builder.setCasCacheDirectory(options.casCacheDirectory);
    }

    builder.setExecuteStageWidth(
        adjustExecuteStageWidth(
            builder.getExecuteStageWidth(), builder.getExecuteStageWidthOffset()));
  }

  /**
   * @brief Adjust.
   * @details Adjust.
   * @param builder The loaded configuration.
   * @param options Server options.
   * @note Overloaded.
   */
  public static void adjust(BuildFarmServerConfig.Builder builder, ServerOptions options) {
    // Handle env overrides.  A typical pattern for docker builds.
    String redisURI = System.getenv("REDIS_URI");
    if (redisURI != null) {
      logger.log(Level.INFO, String.format("Overwriting redis URI: %s", redisURI));
      builder
          .getInstanceBuilder()
          .getShardInstanceConfigBuilder()
          .getRedisShardBackplaneConfigBuilder()
          .setRedisUri(redisURI);
    }

    if (options.port > 0) {
      logger.log(Level.INFO, "setting port from CLI: " + options.port);
      builder.setPort(options.port);
    }

    if (!builder.hasCasWriteTimeout()) {
      Duration defaultDuration = Durations.fromSeconds(3600);
      builder.setCasWriteTimeout(defaultDuration);
      logger.log(
          Level.INFO,
          "CAS write timeout not configured.  Setting to: " + defaultDuration.getSeconds() + "s");
    }

    if (!builder.hasBytestreamTimeout()) {
      Duration defaultDuration = Durations.fromSeconds(3600);
      builder.setBytestreamTimeout(defaultDuration);
      logger.log(
          Level.INFO,
          "Bytestream timeout not configured.  Setting to: " + defaultDuration.getSeconds() + "s");
    }
  }

  private static int adjustExecuteStageWidth(int currentWidth, int widthOffset) {
    int availableCores = Runtime.getRuntime().availableProcessors();
    availableCores -= widthOffset;
    if (currentWidth <= 0) {
      logger.log(
          Level.INFO,
          "Execute stage width is not valid.  Setting to available cores: " + availableCores);
      return availableCores;
    }
    if (currentWidth != availableCores) {
      logger.log(
          Level.WARNING,
          String.format(
              "The configured 'execute stage width' does not optimally saturate available cores: %d < %d (offset: %d)",
              currentWidth, availableCores, widthOffset));
    }

    return currentWidth;
  }
}
