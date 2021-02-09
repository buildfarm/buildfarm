package build.buildfarm.metrics.prometheus;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.util.logging.Logger;

public class PrometheusPublisher {
  private static final Logger logger = Logger.getLogger(PrometheusPublisher.class.getName());
  private static HTTPServer server;
  private static int numExecuteStages = 0;
  private static int numInputFetchStages = 0;

  private static final Gauge cpuQueueSize =
      Gauge.build().name("cpu_queue_size").help("[Server] CPU queue size.").register();
  private static final Gauge gpuQueueSize =
      Gauge.build().name("gpu_queue_size").help("[Server] GPU queue size.").register();
  private static final Gauge preQueueSize =
      Gauge.build().name("pre_queue_size").help("[Server] Pre queue size.").register();
  private static final Gauge dispatchedOperations =
      Gauge.build()
          .name("dispatched_operations_size")
          .help("[Server] Dispatched operations size.")
          .register();
  private static final Gauge workerPoolSize =
      Gauge.build().name("worker_pool_size").help("[Server] Active worker pool size.").register();
  private static final Gauge actionResults =
      Gauge.build().name("action_results").help("[Server] Action results.").register();
  private static final Gauge executionTime =
      Gauge.build().name("execution_time_ms").help("[Worker] Execution time in ms.").register();
  private static final Gauge executionStallTime =
      Gauge.build()
          .name("execution_stall_time_ms")
          .help("[Worker] Execution stall time in ms.")
          .register();
  private static final Gauge executionSlotUsage =
      Gauge.build().name("execution_slot_usage").help("[Worker] Execution slot Usage.").register();
  private static final Gauge inputFetchTime =
      Gauge.build().name("input_fetch_time_ms").help("[Worker] Input fetch time in ms.").register();
  private static final Gauge inputFetchStallTime =
      Gauge.build()
          .name("input_fetch_stall_time_ms")
          .help("[Worker] Input fetch stall time in ms.")
          .register();
  private static final Gauge inputFetchSlotUsage =
      Gauge.build()
          .name("input_fetch_slot_usage")
          .help("[Worker] Input fetch slot Usage.")
          .register();
  private static final Gauge clusterUtilization =
      Gauge.build().name("cluster_utilization").help("[Server] Cluster utilization.").register();
  private static final Gauge executionSuccess =
      Gauge.build().name("execution_success").help("[Server] Execution success.").register();
  private static final Gauge completedOperations =
      Gauge.build().name("completed_operations").help("[Worker] Completed operations.").register();
  private static final Gauge missingBlobs =
      Gauge.build().name("missing_blobs").help("[Server] Find missing blobs.").register();

  public static void startHttpServer(int port) {
    try {
      if (port > 0) {
        DefaultExports.initialize();
        server = new HTTPServer(port);
        logger.info("Started Prometheus HTTP Server on port " + port);
      } else {
        logger.info("Prometheus port is not configured. HTTP Server will not be started");
      }
    } catch (IOException e) {
      logger.severe("Could not start Prometheus HTTP Server on port " + port);
    }
  }

  public static void startHttpServer(int port, int exeStages, int ifStages) {
    numExecuteStages = exeStages;
    numInputFetchStages = ifStages;
    startHttpServer(port);
  }

  public static void stopHttpServer() {
    server.stop();
  }

  public static void updateCpuQueueSize(long val) {
    cpuQueueSize.set(val);
  }

  public static void updateGpuQueueSize(long val) {
    gpuQueueSize.set(val);
  }

  public static void updatePreQueueSize(long val) {
    preQueueSize.set(val);
  }

  public static void updateDispatchedOperationsSize(long val) {
    dispatchedOperations.set(val);
  }

  public static void updateWorkerPoolSize(int val) {
    workerPoolSize.set(val);
  }

  public static void updateExecutionSuccess() {
    executionSuccess.inc();
  }

  public static void updateCompletedOperations() {
    completedOperations.inc();
  }

  public static void updateActionResults(long val) {
    actionResults.inc(val);
  }

  public static void updateMissingBlobs(long val) {
    missingBlobs.inc(val);
  }

  public static void updateExecutionTime(double val) {
    executionTime.inc(val);
  }

  public static void updateExecutionStallTime(double val) {
    executionStallTime.inc(val);
  }

  public static void updateExecutionSlotUsage(int val) {
    executionSlotUsage.set(val);
  }

  public static void updateInputFetchTime(double val) {
    inputFetchTime.inc(val);
  }

  public static void updateInputFetchStallTime(double val) {
    inputFetchStallTime.inc(val);
  }

  public static void updateInputFetchSlotUsage(int val) {
    inputFetchSlotUsage.set(val);
  }

  public static void updateClusterUtilization() {
    try {
      clusterUtilization.set(
          dispatchedOperations.get()
              * 100.0
              / (workerPoolSize.get() * (numExecuteStages + numInputFetchStages)));
    } catch (Exception e) {
      clusterUtilization.set(0.0);
    }
  }
}
