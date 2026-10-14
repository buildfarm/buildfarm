// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.ServerBuilder;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GrpcMetricsTest {
  @Mock private ServerBuilder<?> serverBuilder;
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
    grpcMetrics.setLatencyBuckets(new double[] {1, 2, 3});
    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, grpcMetrics);
    verify(serverBuilder, times(1)).intercept(any(MonitoringServerInterceptor.class));
  }
}
