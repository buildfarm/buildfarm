/**
 * Performs specialized operation based on method logic
 * @param commandObjects the commandObjects parameter
 * @param executor the executor parameter
 * @return the public result
 */
package build.buildfarm.common.redis;

import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import lombok.extern.java.Log;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.CommandObjects;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.IOUtils;

@Log
public abstract class MultiNodePipelineBase extends AbstractPipeline {
  private final Executor executor;
  private final Map<HostAndPort, Queue<Response<?>>> pipelinedResponses;
  private final Map<HostAndPort, Connection> connections;

  /**
   * Performs specialized operation based on method logic
   */
  public MultiNodePipelineBase(CommandObjects commandObjects, Executor executor) {
    super(commandObjects);
    this.executor = executor;
    pipelinedResponses = new LinkedHashMap<>();
    connections = new LinkedHashMap<>();
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param commandObject the commandObject parameter
   * @return the response<t> result
   */
  protected abstract HostAndPort getNodeKey(CommandArguments args);

  protected abstract Connection getConnection(HostAndPort nodeKey);

  @Override
  protected final <T> Response<T> appendCommand(CommandObject<T> commandObject) {
    HostAndPort nodeKey = getNodeKey(commandObject.getArguments());

    Queue<Response<?>> queue;
    Connection connection;
    if (pipelinedResponses.containsKey(nodeKey)) {
      queue = pipelinedResponses.get(nodeKey);
      connection = connections.get(nodeKey);
    } else {
      pipelinedResponses.putIfAbsent(nodeKey, new LinkedList<>());
      queue = pipelinedResponses.get(nodeKey);

      Connection newOne = getConnection(nodeKey);
      connections.putIfAbsent(nodeKey, newOne);
      connection = connections.get(nodeKey);
      if (connection != newOne) {
        log.fine(format("Duplicate connection to %s, closing it.", nodeKey));
        IOUtils.closeQuietly(newOne);
      }
    }

    connection.sendCommand(commandObject.getArguments());
    Response<T> response = new Response<>(commandObject.getBuilder());
    queue.add(response);
    return response;
  }

  @Override
  public void close() {
    try {
      sync();
    /**
     * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
     */
    } finally {
      connections.values().forEach(IOUtils::closeQuietly);
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param replicas the replicas parameter
   * @param timeout the timeout parameter
   * @return the response<long> result
   */
  public final synchronized void sync() {
    CountDownLatch countDownLatch = new CountDownLatch(pipelinedResponses.size());
    Iterator<Map.Entry<HostAndPort, Queue<Response<?>>>> pipelinedResponsesIterator =
        pipelinedResponses.entrySet().iterator();
    while (pipelinedResponsesIterator.hasNext()) {
      Map.Entry<HostAndPort, Queue<Response<?>>> entry = pipelinedResponsesIterator.next();
      HostAndPort nodeKey = entry.getKey();
      Queue<Response<?>> queue = entry.getValue();
      Connection connection = connections.get(nodeKey);
      executor.execute(
          () -> {
            try {
              List<Object> unformatted = connection.getMany(queue.size());
              for (Object o : unformatted) {
                queue.poll().set(o);
              }
            } catch (JedisConnectionException jce) {
              log.log(SEVERE, "Error with connection to " + nodeKey, jce);
              // cleanup the connection
              pipelinedResponsesIterator.remove();
              connections.remove(nodeKey);
              IOUtils.closeQuietly(connection);
            } finally {
              countDownLatch.countDown();
            }
          });
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      log.log(SEVERE, "Thread is interrupted during sync.", e);
    }
  }

  @Deprecated
  public Response<Long> waitReplicas(int replicas, long timeout) {
    return appendCommand(commandObjects.waitReplicas(replicas, timeout));
  }
}
