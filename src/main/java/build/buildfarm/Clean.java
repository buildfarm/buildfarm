package build.buildfarm;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.StubInstance;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

class Clean {
  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    } else if (operation.getMetadata().is(CompletedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(CompletedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    } else {
      Preconditions.checkState(
          operation.getMetadata().is(ExecuteOperationMetadata.class));
      try {
        return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      } catch(InvalidProtocolBufferException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  private static JsonFormat.Parser operationParser =
      JsonFormat.parser().usingTypeRegistry(
          JsonFormat.TypeRegistry.newBuilder()
              .add(CompletedOperationMetadata.getDescriptor())
              .add(ExecuteOperationMetadata.getDescriptor())
              .add(QueuedOperationMetadata.getDescriptor())
              .build());

  private static Operation parseOperationJson(String operationJson) {
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      operationParser.merge(operationJson, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static Operation getOperation(Jedis jedis, String operationName) {
    String json = jedis.hget("Operations", operationName);
    if (json == null) {
      return null;
    }
    return parseOperationJson(json);
  }

  private static ActionResult parseActionResult(String actionResultJson) throws InvalidProtocolBufferException {
    ActionResult.Builder builder = ActionResult.newBuilder();
    JsonFormat.parser().merge(actionResultJson, builder);
    return builder.build();
  }

  private static void purgeCASKeys(Jedis jedis, Instance instance) {
    boolean done = false;
    ImmutableList.Builder<Digest> builder = new ImmutableList.Builder<>();
    String scanToken = SCAN_POINTER_START;

    long total = 0;
    long size = 0;
    long total_under_400 = 0;
    long size_under_400 = 0;
    do {
      ScanParams scanParams = new ScanParams()
          .count(100000)
          .match("ContentAddressableStorage:*");
      ScanResult<String> scanResult = jedis.scan(scanToken, scanParams);
      total += scanResult.getResult().size();

      // System.out.println("scan: " + total + " " + size);

      for (String key : scanResult.getResult()) {
        Digest digest = DigestUtil.parseDigest(key.split(":")[1]);
        size += digest.getSizeBytes();
        if (digest.getSizeBytes() < 400) {
          total_under_400++;
          size_under_400 += digest.getSizeBytes();
        }

        if (digest.getSizeBytes() > 10000000) {
          System.out.println(digest.getSizeBytes() + " " + digest.getHash());
        }
      }

      // builder.addAll(instance.findMissingBlobs(Iterables.transform(scanResult.getResult(), (key) -> DigestUtil.parseDigest(key.split(":")[1]))));

      scanToken = scanResult.getStringCursor();
    } while (!scanToken.equals(SCAN_POINTER_START));

    System.out.println("CAS Holds " + size + "/" + total + " with < 400 " + size_under_400 + "/" + total_under_400);

    /*
    List<Digest> deletedKeys = builder.build();
    // System.out.println("Deleting " + deletedKeys.size() + " keys...");

    Pipeline p = jedis.pipelined();
    for (Iterable<Digest> multiChunk : Iterables.partition(deletedKeys, 18000)) {
      for (Iterable<Digest> digestKeyChunk : Iterables.partition(multiChunk, 18)) {
        p.del(Iterables.toArray(Iterables.transform(digestKeyChunk, (digestKey) -> "ContentAddressableStorage:" + DigestUtil.toString(digestKey)), String.class));
      }
      System.out.print(".");
      System.out.flush();
    }
    p.sync();
    */
  }

  private static void expireActionCacheEntries(Jedis jedis, Instance instance) {
    List<Digest> outputDigests = new ArrayList<>();
    Map<Digest, Set<Digest>> outputDigestActionKeyDigests = new HashMap<>();
    ImmutableSet.Builder<Digest> expiredActionKeyDigestsBuilder = new ImmutableSet.Builder<>();

    String scanToken = SCAN_POINTER_START;

    do {
      ScanParams scanParams = new ScanParams().count(25000);
      ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan("ActionCache", scanToken, scanParams);

      for (Map.Entry<String, String> actionCacheEntry : scanResult.getResult()) {
        Digest actionKeyDigest = DigestUtil.parseDigest(actionCacheEntry.getKey());

        try {
          ActionResult actionResult = parseActionResult(actionCacheEntry.getValue());

          for (OutputFile outputFile : actionResult.getOutputFilesList()) {
            Digest outputDigest = outputFile.getDigest();
            outputDigests.add(outputDigest);
            if (!outputDigestActionKeyDigests.containsKey(outputDigest)) {
              outputDigestActionKeyDigests.put(outputDigest, new HashSet<>());
            }
            outputDigestActionKeyDigests.get(outputDigest).add(actionKeyDigest);
            
            if (outputDigests.size() == 25000) {
              for (Digest missingOutputDigest : instance.findMissingBlobs(outputDigests)) {
                expiredActionKeyDigestsBuilder.addAll(outputDigestActionKeyDigests.get(missingOutputDigest));
              }
              outputDigests.clear();
              outputDigestActionKeyDigests.clear();

              System.out.print(".");
              System.out.flush();
            }
          }
        } catch (InvalidProtocolBufferException e) {
          expiredActionKeyDigestsBuilder.add(actionKeyDigest);
        }
      }

      scanToken = scanResult.getStringCursor();
    } while (!scanToken.equals(SCAN_POINTER_START));

    if (outputDigests.size() > 0) {
      for (Digest missingOutputDigest : instance.findMissingBlobs(outputDigests)) {
        expiredActionKeyDigestsBuilder.addAll(outputDigestActionKeyDigests.get(missingOutputDigest));
      }

      System.out.println("done");
    }

    Set<Digest> expiredActionKeys = expiredActionKeyDigestsBuilder.build();

    System.out.println("Expiring " + expiredActionKeys.size() + " ActionKeys");

    Pipeline p = jedis.pipelined();
    for (Iterable<Digest> actionKeyChunk : Iterables.partition(expiredActionKeys, 18)) {
      p.hdel(
          "ActionCache",
          Iterables.toArray(Iterables.transform(
              actionKeyChunk,
              (actionKeyDigest) -> DigestUtil.toString(actionKeyDigest)), String.class));
      System.out.print(".");
      System.out.flush();
    }
    p.sync();

    System.out.println("done");
  }

  public static Map<String, Operation> getOperationsMap(Jedis jedis) {
    ImmutableMap.Builder<String, Operation> builder = new ImmutableMap.Builder<>();
    for (Map.Entry<String, String> entry : jedis.hgetAll("Operations").entrySet()) {
      builder.put(entry.getKey(), parseOperationJson(entry.getValue()));
    }
    return builder.build();
  }

  private static void removeMissingCompletedOperations(Jedis jedis) {
    Set<String> operations = ImmutableSet.copyOf(jedis.hkeys("Operations"));
    List<String> completedOperations = jedis.lrange("CompletedOperations", 0, -1);
    Set<String> missingCompletedOperations = Sets.difference(
        operations,
        ImmutableSet.copyOf(completedOperations));
    Map<String, Operation> operationsMap = getOperationsMap(jedis);
    ImmutableList.Builder<String> addOperationNames = new ImmutableList.Builder<>();
    for (String operationName : missingCompletedOperations) {
      Operation operation = operationsMap.get(operationName);
      if (operation == null) {
        continue;
      }
      if (operation.getDone()) {
        System.out.println("Adding " + operationName);
        addOperationNames.add(operationName);
      } else {
        // System.out.println(operationName);
        // jedis.lpush("QueuedOperations", operationName);
      }
    }

    ImmutableList.Builder<String> removeOperationNames = new ImmutableList.Builder<>();
    Set<String> completedOperationsSet = new HashSet<>();
    for (String operationName : completedOperations) {
      if (completedOperationsSet.contains(operationName)) {
        System.out.println("Duplicated " + operationName);
        // removeOperationNames.add(operationName);
      } else if (!operations.contains(operationName)) {
        System.out.println("Missing " + operationName);
        // removeOperationNames.add(operationName);
      }
      completedOperationsSet.add(operationName);
    }

    Pipeline p = jedis.pipelined();
    for (String operationName : addOperationNames.build()) {
      p.lpush("CompletedOperations", operationName);
    }
    for (String operationName : removeOperationNames.build()) {
      System.out.println("Removing " + operationName);
      p.lrem("CompletedOperations", 1, operationName);
    }
    p.sync();
  }

  public static void main(String[] args) throws Exception {
    String hostPort = args[0];
    String instanceName = args[1];
    DigestUtil digestUtil = DigestUtil.forHash(args[2]);
    ManagedChannel channel = createChannel(hostPort);
    Instance instance = new StubInstance(
        instanceName,
        digestUtil,
        channel,
        10, TimeUnit.SECONDS,
        Retrier.NO_RETRIES,
        new ByteStreamUploader("", channel, null, 300, Retrier.NO_RETRIES, null));

    URI redisURI = new URI("redis://" + hostPort.split(":")[0] + ":6379");

    JedisPool pool = new JedisPool(new JedisPoolConfig(), redisURI, /* connectionTimeout=*/ 30000, /* soTimeout=*/ 30000);

    try (Jedis jedis = pool.getResource()) {
      /*
      long n = jedis.llen("CompletedOperations") - 1000000;

      for (long i = 0; i < n; i++) {
        String operationName = jedis.lpop("CompletedOperations");
        Operation operation = getOperation(jedis, operationName);
        if (operation == null) {
          System.out.println("Operation is already null...");
        } else {
          ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
          Digest actionDigest = metadata.getActionDigest();
          System.out.println("Removed " + operationName);
          if (jedis.hexists("ActionCache", DigestUtil.toString(actionDigest)) &&
              instance.getActionResult(DigestUtil.asActionKey(actionDigest)) == null) {
            System.out.println("Expired action cache for " + DigestUtil.toString(actionDigest));
          }
          jedis.hdel("Operations", operationName);
        }
      }
      */

      // removeMissingCompletedOperations(jedis);

      // expireActionCacheEntries(jedis, instance);

      purgeCASKeys(jedis, instance);
    } finally {
      pool.close();
    }
  }
};
