package build.buildfarm.server;

import static com.google.common.base.Preconditions.checkState;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ToolDetails;
import build.buildfarm.instance.shard.ServerInstance;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
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
  private final Set<UUID> seen =
      Collections.newSetFromMap(
          CacheBuilder.newBuilder()
              .expireAfterAccess(10, TimeUnit.MINUTES)
              .maximumSize(100)
              .<UUID, Boolean>build()
              .asMap());

  InvocationsCollector(ServerInstance instance) {
    this.instance = instance;
  }

  public void run() {
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
      } catch (IOException e) {
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

  private void addNewToolInvocationId(
      UUID toolInvocationId, String correlatedInvocations, ToolDetails toolDetails)
      throws IOException {
    int len = correlatedInvocations.length();
    checkState(len >= UUID_STRING_LENGTH);
    UUID correlatedInvocationsId;
    if (len > UUID_STRING_LENGTH) {
      try {
        URI uri = new URI(correlatedInvocations);
        // reassigns id to index invocationIds
        correlatedInvocationsId = instance.indexCorrelatedInvocations(uri);
      } catch (Exception e) {
        // non-url correlated name, pull suffix
        correlatedInvocationsId =
            UUID.fromString(correlatedInvocations.substring(len - UUID_STRING_LENGTH));
      }
    } else {
      correlatedInvocationsId = UUID.fromString(correlatedInvocations);
    }

    // associate correlated id with toolInvocationId
    instance.addToolInvocationId(toolInvocationId, correlatedInvocationsId, toolDetails);
  }

  private void iterate(RequestMetadata meta) throws IOException {
    // check the cache
    UUID toolInvocationId = UUID.fromString(meta.getToolInvocationId());

    if (seen.add(toolInvocationId)) {
      addNewToolInvocationId(
          toolInvocationId, meta.getCorrelatedInvocationsId(), meta.getToolDetails());
    }

    instance.addRequest(
        meta.getActionId(), toolInvocationId, meta.getActionMnemonic(), meta.getTargetId());
  }
}
