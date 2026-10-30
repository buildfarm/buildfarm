// Copyright 2021-2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.metrics.prometheus;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import lombok.extern.java.Log;

@Log
public class PrometheusPublisher {
  private HTTPServer server;
  private Thread metricsCollectorThread;

  public void startHttpServer(int port) {
    try {
      if (port > 0) {
        DefaultExports.initialize();
        server = new HTTPServer(port);
        log.info("Started Prometheus HTTP Server on port " + port);
      } else {
        log.info("Prometheus port is not configured. HTTP Server will not be started");
      }
    } catch (IOException e) {
      log.severe("Could not start Prometheus HTTP Server on port " + port);
    }
  }

  public void stopHttpServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  public void startMetricsCollector(Runnable collector) {
    metricsCollectorThread = new Thread(collector, "Prometheus Metrics Collector");
    metricsCollectorThread.start();
  }

  public void stopMetricsCollector() throws InterruptedException {
    if (metricsCollectorThread != null) {
      metricsCollectorThread.interrupt();
      metricsCollectorThread.join();
      metricsCollectorThread = null;
    }
  }
}
