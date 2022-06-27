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

import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.ExecutionWrapperProperties;
import build.buildfarm.common.ExecutionWrappers;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.v1test.WorkerConfig;
import com.google.common.base.Strings;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @class ConfigAdjuster
 * @brief Modify the loaded configuration to avoid any unsuitable defaults.
 * @details Users may exclude certain options from their configuration files. The default values
 *     given to these options may also be invalid. Since protobuf does not allow custom defaults, we
 *     will adjust accordingly after a configuration is loaded. This is primarily useful when adding
 *     new configuration values that aren't an existing config files. Adjusting the defaults here
 *     allow users to upgrade buildfarm in forwards compatible way (i.e. not needing to adjust their
 *     configs during upgrades).
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
    String executionStageWidth = System.getenv("EXECUTION_STAGE_WIDTH");
    if (executionStageWidth != null) {
      builder.setExecuteStageWidth(Integer.parseInt(executionStageWidth));
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

    checkExecutionWrapperAvailability();
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

    checkExecutionWrapperAvailability();
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
    // Is this the best way to derive system processors?
    // It seems to work fine on host machines & inside containers.
    int availableCores = Runtime.getRuntime().availableProcessors();

    // Some cores of a machine are reserved for usage other than buildfarm's execution.
    // Adjust via a user provided offset when deriving the execute stage width to avoid over
    // saturation.
    availableCores -= widthOffset;

    // The user has chosen to have their execution width derived for them
    if (currentWidth <= 0) {
      logger.log(
          Level.INFO,
          String.format(
              "Execute stage width is not valid.  Setting to available cores: %d (offset: %d)",
              availableCores, widthOffset));
      return availableCores;
    }

    // The user is providing their own execution width.
    // Show a warning to help them decide if their selection is optimal
    if (currentWidth != availableCores) {
      logger.log(
          Level.WARNING,
          String.format(
              "The configured 'execute stage width' does not optimally saturate available cores: %d < %d (offset: %d).  You can derive your execution stage width automatically by excluding it from the config.",
              currentWidth, availableCores, widthOffset));
    }

    return currentWidth;
  }

  private static ExecutionWrapperProperties createExecutionWrapperProperties() {
    // Create a mapping from the execution wrappers to the features they enable.
    ExecutionWrapperProperties wrapperProperties = new ExecutionWrapperProperties();
    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(ExecutionWrappers.CGROUPS)),
        new ArrayList<String>(
            Arrays.asList(
                "limit_execution",
                ExecutionProperties.CORES,
                ExecutionProperties.MIN_CORES,
                ExecutionProperties.MAX_CORES,
                ExecutionProperties.MIN_MEM,
                ExecutionProperties.MAX_MEM)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(ExecutionWrappers.LINUX_SANDBOX)),
        new ArrayList<String>(
            Arrays.asList(
                ExecutionProperties.LINUX_SANDBOX,
                ExecutionProperties.BLOCK_NETWORK,
                ExecutionProperties.TMPFS)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(ExecutionWrappers.AS_NOBODY)),
        new ArrayList<String>(Arrays.asList(ExecutionProperties.AS_NOBODY)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(Arrays.asList(ExecutionWrappers.PROCESS_WRAPPER)),
        new ArrayList<String>(Arrays.asList(ExecutionProperties.PROCESS_WRAPPER)));

    wrapperProperties.mapping.put(
        new ArrayList<String>(
            Arrays.asList(
                ExecutionWrappers.SKIP_SLEEP,
                ExecutionWrappers.SKIP_SLEEP_PRELOAD,
                ExecutionWrappers.DELAY)),
        new ArrayList<String>(
            Arrays.asList(ExecutionProperties.SKIP_SLEEP, ExecutionProperties.TIME_SHIFT)));

    return wrapperProperties;
  }

  private static void checkExecutionWrapperAvailability() {
    ExecutionWrapperProperties wrapperProperties = createExecutionWrapperProperties();

    // Find missing tools, and warn the user that missing tools mean missing features.
    wrapperProperties.mapping.forEach(
        (tools, features) ->
            tools.forEach(
                (tool) -> {
                  if (Files.notExists(Paths.get(tool))) {
                    String message =
                        String.format(
                            "the execution wrapper %s is missing and therefore the following features will not be available: %s",
                            tool, String.join(", ", features));
                    logger.log(Level.WARNING, message);
                  }
                }));
  }
}
