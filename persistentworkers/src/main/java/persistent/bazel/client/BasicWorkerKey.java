package persistent.bazel.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Objects;
import lombok.Getter;
import lombok.ToString;

/**
 * Based off of copy-pasting from Bazel's WorkerKey. Has less dependencies, but only ProtoBuf and
 * non-multiplex support.
 *
 * <p>Data container that uniquely identifies a kind of worker process.
 */
@ToString(onlyExplicitlyIncluded = true)
public final class BasicWorkerKey {
  @Getter @ToString.Include private final ImmutableList<String> cmd;

  @Getter @ToString.Include private final ImmutableList<String> args;

  @Getter @ToString.Include private final ImmutableMap<String, String> env;

  /** Mnemonic of the worker; but we don't actually have the real action mnemonic */
  @Getter @ToString.Include private final String mnemonic;

  /** If true, the workers run inside a sandbox. Returns true if workers are sandboxed. */
  @Getter private final boolean sandboxed;

  /** If true, the workers for this key are able to cancel work requests. */
  @Getter private final boolean cancellable;

  /**
   * Cached value for the hash of this key, because the value is expensive to calculate
   * (ImmutableMap and ImmutableList do not cache their hashcodes).
   */
  private final int hash;

  public BasicWorkerKey(
      ImmutableList<String> cmd,
      ImmutableList<String> args,
      ImmutableMap<String, String> env,
      String mnemonic,
      boolean sandboxed,
      boolean cancellable) {
    this.cmd = Preconditions.checkNotNull(cmd);
    this.args = Preconditions.checkNotNull(args);
    this.env = Preconditions.checkNotNull(env);
    this.mnemonic = Preconditions.checkNotNull(mnemonic);
    this.sandboxed = sandboxed;
    this.cancellable = cancellable;

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

    BasicWorkerKey workerKey = (BasicWorkerKey) o;
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
    return Objects.hash(cmd, args, env, mnemonic, cancellable, sandboxed);
  }
}
