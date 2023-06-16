package build.buildfarm.common.config;

import io.grpc.ServerBuilder;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class GrpcMetricsTest {
  @Mock
  private ServerBuilder<?> serverBuilder;
  private final GrpcMetrics grpcMetrics = new GrpcMetrics();

  @Test
  public void testHandleGrpcMetricIntercepts_disabled() {
    grpcMetrics.setEnabled(false);

    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, grpcMetrics);
    verify(serverBuilder, never()).intercept(any(MonitoringServerInterceptor.class));
  }

  @Test
  public void testHandleGrpcMetricIntercepts_withLatencyBucket() {
    grpcMetrics.setEnabled(true);
    grpcMetrics.setProvideLatencyHistograms(true);
    grpcMetrics.setLatencyBuckets(new double[]{1, 2, 3});

    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, grpcMetrics);
    verify(serverBuilder, times(1)).intercept(any(MonitoringServerInterceptor.class));
  }
}
