/**
 * Performs specialized operation based on method logic
 * @return the public result
 */
// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.metrics.log;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.metrics.AbstractMetricsPublisher;
import build.buildfarm.v1test.OperationRequestMetadata;
import com.google.longrunning.Operation;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class LogMetricsPublisher extends AbstractMetricsPublisher {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private static Level logLevel;

  /**
   * Carries request context and tracing information Performs side effects including logging and state modifications.
   * @param operation the operation parameter
   * @param requestMetadata the requestMetadata parameter
   */
  public LogMetricsPublisher() {
    super(configs.getServer().getClusterId());
    if (configs.getServer().getMetrics().getLogLevel() != null) {
      logLevel = Level.parse(configs.getServer().getMetrics().getLogLevel().name());
    } else {
      logLevel = Level.FINEST;
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param metricName the metricName parameter
   * @param metricValue the metricValue parameter
   */
  public void publishRequestMetadata(Operation operation, RequestMetadata requestMetadata) {
    try {
      OperationRequestMetadata metadata = populateRequestMetadata(operation, requestMetadata);
      if (metadata.getDone()) {
        log.log(logLevel, formatRequestMetadataToJson(metadata));
      }
    } catch (Exception e) {
      log.log(
          Level.WARNING,
          String.format("Could not publish request metadata to LOG for %s.", operation.getName()),
          e);
    }
  }

  @Override
  public void publishMetric(String metricName, Object metricValue) {
    log.log(Level.INFO, String.format("%s: %s", metricName, metricValue.toString()));
  }
}
