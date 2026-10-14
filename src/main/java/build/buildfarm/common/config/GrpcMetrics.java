// Copyright 2022-2023 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.config;

import io.grpc.ServerBuilder;
import java.util.List;
import lombok.Data;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

@Data
public class GrpcMetrics {
  private boolean enabled = false;
  private boolean provideLatencyHistograms = false;
  private double[] latencyBuckets;
  private List<String> labelsToReport;

  public static void handleGrpcMetricIntercepts(
      ServerBuilder<?> serverBuilder, GrpcMetrics grpcMetrics) {
    // Decide how to capture GRPC Prometheus metrics.
    // By default, we don't capture any.
    if (grpcMetrics.isEnabled()) {
      // Assume core metrics.
      // Core metrics include send/receive totals tagged with return codes.  No latencies.
      Configuration grpcConfig = Configuration.cheapMetricsOnly();

      // Enable latency buckets.
      if (grpcMetrics.isProvideLatencyHistograms()) {
        grpcConfig = Configuration.allMetrics();
      }

      // provide custom latency buckets
      if (grpcMetrics.getLatencyBuckets() != null) {
        grpcConfig = grpcConfig.withLatencyBuckets(grpcMetrics.getLatencyBuckets());
      }

      // report custom metric labels
      if (grpcMetrics.getLabelsToReport() != null) {
        grpcConfig = grpcConfig.withLabelHeaders(grpcMetrics.getLabelsToReport());
      }

      // Apply config to create an interceptor and apply it to the GRPC server.
      MonitoringServerInterceptor monitoringInterceptor =
          MonitoringServerInterceptor.create(grpcConfig);
      serverBuilder.intercept(monitoringInterceptor);
    }
  }
}
