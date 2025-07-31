/**
 * Performs specialized operation based on method logic
 * @param requestMetadata the requestMetadata parameter
 * @return the boolean result
 */
package build.buildfarm.server;

import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ToolDetails;
import build.buildfarm.instance.shard.ServerInstance;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.java.Log;

@Log
class InvocationsCollector extends LinkedBlockingQueue<RequestMetadata> implements Runnable {
  private static final int UUID_STRING_LENGTH = 36;

  // needs to be fixed when we support multiple instances again
  // prerequisite - should all invocationIds have the same correlatedInvocationsId?
  // cache for invocationId
  // cache for correlatedInvocationsId
  private final ServerInstance instance;
  private final Set<String> seen =
      Collections.newSetFromMap(
          CacheBuilder.newBuilder()
              .expireAfterAccess(10, TimeUnit.MINUTES)
              .maximumSize(100)
              .<String, Boolean>build()
              .asMap());
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  private volatile boolean running = false;

  InvocationsCollector(ServerInstance instance) {
    this.instance = instance;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public boolean add(RequestMetadata requestMetadata) {
    if (running) {
      return super.add(requestMetadata);
    }
    return false;
  }

  @Override
  public void run() {
    running = true;
    try {
      loop();
    } finally {
      running = false;
    }
  }

  /**
   * Creates and initializes a new instance
   * @param toolInvocationId the toolInvocationId parameter
   * @param correlatedInvocationsId the correlatedInvocationsId parameter
   * @param toolDetails the toolDetails parameter
   */
  private void loop() {
    for (; ; ) {
      RequestMetadata meta;
      try {
        // pop from queue
        meta = take();
      } catch (InterruptedException e) {
        break;
      }

      try {
        iterate(meta);
      } catch (Exception e) {
        log.log(
            WARNING,
            "error handling invocation "
                + meta.getCorrelatedInvocationsId()
                + " => "
                + meta.getToolInvocationId(),
            e);
      }
    }
  }

  /**
   * Processes the next operation in the pipeline stage
   * @param meta the meta parameter
   */
  private void addNewToolInvocationId(
      String toolInvocationId, String correlatedInvocationsId, ToolDetails toolDetails)
      throws IOException {
    try {
      URI uri = new URI(correlatedInvocationsId);
      // reassigns id to index invocationIds
      correlatedInvocationsId = instance.indexCorrelatedInvocations(uri);
    } catch (Exception e) {
      // non-url correlated name, use entire
    }

    // associate correlated id with toolInvocationId
    if (!Strings.isNullOrEmpty(correlatedInvocationsId)) {
      instance.addToolInvocationId(toolInvocationId, correlatedInvocationsId, toolDetails);
    }
  }

  private void iterate(RequestMetadata meta) throws IOException {
    // check the cache
    String toolInvocationId = meta.getToolInvocationId();

    if (!Strings.isNullOrEmpty(toolInvocationId)) {
      if (seen.add(toolInvocationId)) {
        addNewToolInvocationId(
            toolInvocationId, meta.getCorrelatedInvocationsId(), meta.getToolDetails());
      }

      instance.addRequest(
          meta.getActionId(), toolInvocationId, meta.getActionMnemonic(), meta.getTargetId());
    }
  }
}
