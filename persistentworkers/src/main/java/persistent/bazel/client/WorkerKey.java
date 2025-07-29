package persistent.bazel.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.Objects;
import java.util.SortedMap;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

/**
 * The key of the {@link CommonsWorkerPool worker pool}.
 *
 * <p>See {@link BasicWorkerKey} for more information between this class and that one.
 */
@ToString(onlyExplicitlyIncluded = true)
public class WorkerKey {
  @Getter @ToString.Include private final BasicWorkerKey basicWorkerKey;

  /** The user the worker process is running under and the owner of the worker's files. */
  @Getter @Nullable private final UserPrincipal owner;

  /** Execution wrapper arguments to be prepended to the worker command. */
  @Getter private final ImmutableList<String> wrapperArguments;

  @Getter @ToString.Include private final Path execRoot;

  /**
   * These are used during validation whether a worker is still usable. They are not used to
   * uniquely identify a kind of worker, thus it is not to be used by the .equals() / .hashCode()
   * methods.
   */
  @Getter private final HashCode workerFilesCombinedHash;

  /**
   * Worker files with the corresponding hash code.
   *
   * <p>These paths should be stable, so use relative paths (unless it's a universal absolute path
   * like /tmp/my_tools/...)
   */
  @Getter private final SortedMap<Path, HashCode> workerFilesWithHashes;

  /**
   * Cached value for the hash of this key, because the value is expensive to calculate
   * (ImmutableMap and ImmutableList do not cache their hashcodes).
   */
  private final int hash;

  public WorkerKey(
      BasicWorkerKey basicWorkerKey,
      @Nullable UserPrincipal owner,
      ImmutableList<String> wrapperArguments,
      Path execRoot,
      HashCode workerFilesCombinedHash,
      SortedMap<Path, HashCode> workerFilesWithHashes) {
    // Part of hash
    this.basicWorkerKey = Preconditions.checkNotNull(basicWorkerKey);
    this.owner = owner;
    this.wrapperArguments = Preconditions.checkNotNull(wrapperArguments);
    this.execRoot = Preconditions.checkNotNull(execRoot);

    // Not part of hash
    this.workerFilesCombinedHash = Preconditions.checkNotNull(workerFilesCombinedHash);
    this.workerFilesWithHashes = Preconditions.checkNotNull(workerFilesWithHashes);

    this.hash = calculateHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    WorkerKey otherWorkerKey = (WorkerKey) other;

    if (!basicWorkerKey.equals(otherWorkerKey.basicWorkerKey)) {
      return false;
    }

    if (!((owner == null && otherWorkerKey.owner == null)
        || (owner != null && otherWorkerKey.owner != null && owner.equals(otherWorkerKey.owner)))) {
      return false;
    }

    if (!wrapperArguments.equals(otherWorkerKey.wrapperArguments)) {
      return false;
    }

    return execRoot.equals(otherWorkerKey.execRoot);
  }

  public ImmutableList<String> getArgs() {
    return basicWorkerKey.getArgs();
  }

  public ImmutableList<String> getCmd() {
    return basicWorkerKey.getCmd();
  }

  public ImmutableMap<String, String> getEnv() {
    return basicWorkerKey.getEnv();
  }

  public String getMnemonic() {
    return basicWorkerKey.getMnemonic();
  }

  @Override
  public int hashCode() {
    return hash;
  }

  public boolean isCancellable() {
    return basicWorkerKey.isCancellable();
  }

  public boolean isSandboxed() {
    return basicWorkerKey.isSandboxed();
  }

  private int calculateHashCode() {
    return Objects.hash(basicWorkerKey, owner, wrapperArguments, execRoot);
  }
}
