package persistent.bazel.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import java.nio.file.Path;
import java.util.Objects;
import java.util.SortedMap;
import lombok.Getter;
import lombok.ToString;

/**
 * Based off of copy-pasting from Bazel's WorkerKey. Has less dependencies, but only ProtoBuf and
 * non-multiplex support.
 *
 * <p>Data container that uniquely identifies a kind of worker process.
 */
@ToString(onlyExplicitlyIncluded = true)
public final class WorkerKey {

  @Getter @ToString.Include private final ImmutableList<String> cmd;

  @Getter @ToString.Include private final ImmutableList<String> args;

  @Getter @ToString.Include private final ImmutableMap<String, String> env;

  @Getter @ToString.Include private final Path execRoot;

  /** Mnemonic of the worker; but we don't actually have the real action mnemonic */
  @Getter @ToString.Include private final String mnemonic;

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

  /** If true, the workers run inside a sandbox. Returns true if workers are sandboxed. */
  @Getter private final boolean sandboxed;

  /** If true, the workers for this key are able to cancel work requests. */
  @Getter private final boolean cancellable;

  /**
   * Cached value for the hash of this key, because the value is expensive to calculate
   * (ImmutableMap and ImmutableList do not cache their hashcodes).
   */
  private final int hash;

  public WorkerKey(
      ImmutableList<String> cmd,
      ImmutableList<String> args,
      ImmutableMap<String, String> env,
      Path execRoot,
      String mnemonic,
      HashCode workerFilesCombinedHash,
      SortedMap<Path, HashCode> workerFilesWithHashes,
      boolean sandboxed,
      boolean cancellable) {
    // Part of hash
    this.cmd = Preconditions.checkNotNull(cmd);
    this.args = Preconditions.checkNotNull(args);
    this.env = Preconditions.checkNotNull(env);
    this.execRoot = Preconditions.checkNotNull(execRoot);
    this.mnemonic = Preconditions.checkNotNull(mnemonic);
    this.sandboxed = sandboxed;
    this.cancellable = cancellable;
    // Not part of hash
    this.workerFilesCombinedHash = Preconditions.checkNotNull(workerFilesCombinedHash);
    this.workerFilesWithHashes = Preconditions.checkNotNull(workerFilesWithHashes);

    this.hash = calculateHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkerKey workerKey = (WorkerKey) o;
    if (this.hash != workerKey.hash) {
      return false;
    }
    if (!cmd.equals(workerKey.cmd)) {
      return false;
    }
    if (!args.equals(workerKey.args)) {
      return false;
    }
    if (!cancellable == workerKey.cancellable) {
      return false;
    }
    if (!sandboxed == workerKey.sandboxed) {
      return false;
    }
    if (!env.equals(workerKey.env)) {
      return false;
    }
    if (!execRoot.equals(workerKey.execRoot)) {
      return false;
    }
    return mnemonic.equals(workerKey.mnemonic);
  }

  /** Since all fields involved in the {@code hashCode} are final, we cache the result. */
  @Override
  public int hashCode() {
    return hash;
  }

  private int calculateHashCode() {
    // Use the string representation of the protocolFormat because the hash of the same enum value
    // can vary across instances.
    return Objects.hash(cmd, args, env, execRoot, mnemonic, cancellable, sandboxed);
  }
}
