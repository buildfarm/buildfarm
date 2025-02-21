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

package build.buildfarm.worker.resources;

import io.prometheus.client.Gauge;

/**
 * @class LocalResourceSetMetrics
 * @brief Tracks metrics related to a worker's limited local resources.
 * @details Answers how many resources exist, how many are claimed, and by how many requesters.
 */
public class LocalResourceSetMetrics {
  public static final Gauge resourceUsageMetric =
      Gauge.build()
          .name("local_resource_usage")
          .labelNames("resource_name")
          .help("The number of claims for each resource currently being used for execution")
          .register();

  public static final Gauge resourceTotalMetric =
      Gauge.build()
          .name("local_resource_total")
          .labelNames("resource_name")
          .help("The total number of claims exist for a particular resource")
          .register();

  public static final Gauge requestersMetric =
      Gauge.build()
          .name("local_resource_requesters")
          .labelNames("resource_name")
          .help(
              "Tracks how many actions have requested local resources. This can help determine if"
                  + " resources are being hogged by some actions.")
          .register();
}
