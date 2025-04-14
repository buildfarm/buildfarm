package build.buildfarm.common.config;

import io.grpc.ServerBuilder;
import java.util.List;
import lombok.Data;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

@Data
public class GrpcMetrics {
  private boolean enabled;
  private boolean provideLatencyHistograms;
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
