package build.buildfarm.metrics.prometheus;

import io.prometheus.client.Counter;
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

  private static final Gauge cpuQueueSize = Gauge.build().name("cpu_queue_size").help("CPU queue size.").register();
  private static final Gauge gpuQueueSize = Gauge.build().name("gpu_queue_size").help("GPU queue size.").register();
  private static final Gauge preQueueSize = Gauge.build().name("pre_queue_size").help("Pre queue size.").register();
  private static final Gauge dispatchedOperations = Gauge.build().name("dispatched_operations_size").help("Dispatched operations size.").register();
  private static final Gauge workerPoolSize = Gauge.build().name("worker_pool_size").help("Active worker pool size.").register();
  private static final Gauge actionResults =  Gauge.build().name("action_results").help("Action results.").register();
  private static final Gauge executionTime = Gauge.build().name("execution_time_ms").help("Execution time in ms.").register();
  private static final Gauge executionStallTime = Gauge.build().name("execution_stall_time_ms").help("Execution stall time in ms.").register();
  private static final Gauge executionSlotUsage = Gauge.build().name("execution_slot_usage").help("Execution slot Usage.").register();
  private static final Gauge inputFetchTime = Gauge.build().name("input_fetch_time_ms").help("Input fetch time in ms.").register();
  private static final Gauge inputFetchStallTime = Gauge.build().name("input_fetch_stall_time_ms").help("Input fetch stall time in ms.").register();
  private static final Gauge inputFetchSlotUsage = Gauge.build().name("input_fetch_slot_usage").help("Input fetch slot Usage.").register();
  private static final Gauge clusterUtilization = Gauge.build().name("cluster_utilization").help("Cluster utilization.").register();

  private static final Counter executionSuccess =  Counter.build().name("execution_success").help("Execution success.").register();
  private static final Counter completedOperations =  Counter.build().name("completed_operations").help("Completed operations.").register();
  private static final Counter missingBlobs =  Counter.build().name("missing_blobs").help("Find missing blobs.").register();

  public static void startHttpServer(int port) {
    try {
      DefaultExports.initialize();
      server = new HTTPServer(port);
      logger.info("Started Prometheus HTTP Server on port " + port);
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
    executionTime.set(val);
  }

  public static void updateExecutionStallTime(double val) {
    executionStallTime.set(val);
  }

  public static void updateExecutionSlotUsage(int val) {
    executionSlotUsage.set(val);
  }

  public static void updateInputFetchTime(double val) {
    inputFetchTime.set(val);
  }

  public static void updateInputFetchStallTime(double val) {
    inputFetchStallTime.set(val);
  }

  public static void updateInputFetchSlotUsage(int val) {
    inputFetchSlotUsage.set(val);
  }

  public static void updateClusterUtilization() {
    try {
      clusterUtilization.set(dispatchedOperations.get() * 100.0 / (workerPoolSize.get() * (numExecuteStages + numInputFetchStages)));
      } catch (Exception e) {
      clusterUtilization.set(0.0);
    }
  }
}
