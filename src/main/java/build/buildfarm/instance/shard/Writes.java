package build.buildfarm.instance.shard;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.BlobWriteKey;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class Writes {
  private final Supplier<Instance> instanceSupplier;
  private final LoadingCache<BlobWriteKey, Instance> blobWriteInstances;

  Writes(Supplier<Instance> instanceSupplier) {
    this(
        instanceSupplier,
        /* writeExpiresAfter=*/ 1,
        /* writeExpiresUnit=*/ TimeUnit.HOURS);
  }

  Writes(
      Supplier<Instance> instanceSupplier,
      long writeExpiresAfter,
      TimeUnit writeExpiresUnit) {
    this.instanceSupplier = instanceSupplier;
    blobWriteInstances = CacheBuilder.newBuilder()
        .expireAfterWrite(writeExpiresAfter, writeExpiresUnit)
        .build(new CacheLoader<BlobWriteKey, Instance>() {
          @Override
          public Instance load(BlobWriteKey key) {
            return instanceSupplier.get();
          }
        });
  }

  public Write get(Digest digest, UUID uuid) {
    BlobWriteKey key = BlobWriteKey.newBuilder()
        .setDigest(digest)
        .setIdentifier(uuid.toString())
        .build();
    try {
      return blobWriteInstances.get(key).getBlobWrite(digest, uuid);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfInstanceOf(cause, RuntimeException.class);
      throw new UncheckedExecutionException(cause);
    }
  }
}
