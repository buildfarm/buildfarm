package build.buildfarm.metrics.cas;

import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * This class records read and write metrics, updating the information to central storage at regular
 * intervals. The collected data can be utilized to determine the read count of any CAS entry, query
 * the top "n%" of frequently accessed CAS entries, and rank CAS entries based on their read
 * frequency, among other uses.
 */
@Log
public final class CASAccessMetricsRecorder {
  private final ScheduledExecutorService scheduler;

  private final Deque<Map<Digest, AtomicInteger>> readIntervalCountQueue;

  private volatile Map<Digest, AtomicInteger> currentIntervalReadCount;

  private final Duration casEntryReadCountWindow;

  private final Duration casEntryReadCountUpdateInterval;

  private final int queueLength;

  private boolean running = false;

  private final Backplane backplane;

  final ReadWriteLock lock = new ReentrantReadWriteLock();

  int intervalNumber = 0; // Assists in unit test.

  public CASAccessMetricsRecorder(
      ScheduledExecutorService scheduler,
      Backplane backplane,
      Duration casEntryReadCountWindow,
      Duration casEntryReadCountUpdateInterval) {
    Preconditions.checkArgument(
        casEntryReadCountUpdateInterval.toMillis() > 0,
        "CASEntryReadCountUpdateInterval must be greater than 0");
    Preconditions.checkArgument(
        casEntryReadCountWindow.toMillis() % casEntryReadCountUpdateInterval.toMillis() == 0,
        "CASEntryReadCountWindow must be divisible by CASEntryReadCountUpdateInterval");
    this.scheduler = scheduler;
    this.backplane = backplane;
    this.casEntryReadCountWindow = casEntryReadCountWindow;
    this.casEntryReadCountUpdateInterval = casEntryReadCountUpdateInterval;
    this.readIntervalCountQueue = new LinkedList<>();
    this.queueLength = getQueueLength();
  }

  /**
   * Increments the read count by 1 for the given digest.
   *
   * @param digest Digest to which the read count is recorded.
   */
  public void recordRead(Digest digest) {
    if (!running) {
      throw new IllegalStateException("Metrics Recorder is not running");
    }
    runWithLock(
        lock.readLock(),
        () ->
            currentIntervalReadCount
                .computeIfAbsent(digest, d -> new AtomicInteger(0))
                .incrementAndGet());
  }

  /**
   * Records the write operation for a digest if it is new. If a read is recorded before for a newly
   * written digest, this method will have no effect.
   *
   * @param digest Digest for which the write operation is recorded.
   */
  public void recordWrite(Digest digest) {
    if (!running) {
      throw new IllegalStateException("Metrics Recorder is not running");
    }
    runWithLock(
        lock.readLock(), () -> currentIntervalReadCount.putIfAbsent(digest, new AtomicInteger(0)));
  }

  /**
   * Transitions the metrics recorder to the running state and sets up periodic read updates. Once
   * the metrics recorder is in the running state, calling this method will have no effect.
   */
  public synchronized void start() {
    if (running) {
      return;
    }
    currentIntervalReadCount = new ConcurrentHashMap<>();
    long initialDelay = getInitialDelayInMillis();
    scheduler.scheduleAtFixedRate(
        this::updateReadCount,
        initialDelay,
        casEntryReadCountUpdateInterval.toMillis(),
        TimeUnit.MILLISECONDS);
    running = true;
  }

  /** Stops the scheduler responsible to update read counts periodically. */
  public void stop() throws InterruptedException {
    running = false;
  }

  private int getQueueLength() {
    return (int) (casEntryReadCountWindow.toMillis() / casEntryReadCountUpdateInterval.toMillis());
  }

  private long getInitialDelayInMillis() {
    // Ensure synchronization among all workers, regardless of their start time.
    return casEntryReadCountUpdateInterval.toMillis()
        - (Instant.now().toEpochMilli() % casEntryReadCountUpdateInterval.toMillis());
  }

  private void updateReadCount() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    // Ensure all threads writes to new interval.
    Map<Digest, AtomicInteger> lastIntervalReadCount =
        callWithLock(lock.writeLock(), this::createNewReadCountInterval);
    readIntervalCountQueue.offer(lastIntervalReadCount);

    Map<Digest, Integer> effectiveReadCount = new HashMap<>();
    lastIntervalReadCount.forEach((k, v) -> effectiveReadCount.put(k, v.get()));

    Map<Digest, AtomicInteger> firstIntervalReadCount = new HashMap<>();
    if (queueLength > 0 && readIntervalCountQueue.size() > queueLength) {
      firstIntervalReadCount = readIntervalCountQueue.poll();
    }
    firstIntervalReadCount.forEach(
        (k, v) ->
            effectiveReadCount.compute(
                k, (digest, readCount) -> readCount == null ? -v.get() : readCount - v.get()));

    try {
      Map<String, Integer> updatedReadCount = backplane.updateCasReadCount(effectiveReadCount);
      expireUnreadEntries(updatedReadCount, firstIntervalReadCount);
    } catch (Exception e) {
      log.log(Level.WARNING, "Failed to update the cas read count to backplane", e);
    }

    long timeToUpdate = stopwatch.stop().elapsed().toMillis();
    log.fine(format("Took %d ms to update read count.", timeToUpdate));

    if (timeToUpdate >= casEntryReadCountUpdateInterval.toMillis()) {
      log.warning(
          "Consider increasing the update interval: read count update time exceeds the update interval.");
    }
  }

  private void expireUnreadEntries(
      Map<String, Integer> casReadCount, Map<Digest, AtomicInteger> firstIntervalReadCount) {
    Set<Digest> digestsToExpire = new HashSet<>();
    firstIntervalReadCount.forEach(
        (digest, readCount) -> {
          Integer totalReadCount = casReadCount.get(digest.getHash());
          if (totalReadCount != null && totalReadCount == 0) {
            digestsToExpire.add(digest);
            // If the readCount is 0 within an interval, it indicates that the entry was written
            // during that interval but remained unread.
            if (readCount.get() == 0) {
              log.info(
                  format(
                      "Digest %s was not read once after being written in last %s duration",
                      DigestUtil.toString(digest), casEntryReadCountWindow.toString()));
            }
          }
        });
    if (!digestsToExpire.isEmpty()) {
      try {
        int removedCount = backplane.removeCasReadCountEntries(digestsToExpire);
        log.fine(format("Number of cas read count entries removed : %d", removedCount));
      } catch (Exception e) {
        log.log(Level.WARNING, "Failed to remove cas read count entries", e);
      }
    }
  }

  @VisibleForTesting
  Deque<Map<Digest, AtomicInteger>> getReadIntervalCountQueue() {
    return readIntervalCountQueue;
  }

  private Map<Digest, AtomicInteger> createNewReadCountInterval() {
    Map<Digest, AtomicInteger> lastIntervalReadCount = currentIntervalReadCount;
    currentIntervalReadCount = new ConcurrentHashMap<>();
    intervalNumber++;
    return lastIntervalReadCount;
  }

  private static void runWithLock(Lock lock, Runnable task) {
    lock.lock();
    try {
      task.run();
    } finally {
      lock.unlock();
    }
  }

  private static <T> T callWithLock(Lock lock, Supplier<T> task) {
    lock.lock();
    try {
      return task.get();
    } finally {
      lock.unlock();
    }
  }
}