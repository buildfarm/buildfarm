package build.buildfarm.metrics.cas;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.backplane.Backplane;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class CASAccessMetricsRecorderTest {

  private CASAccessMetricsRecorder casAccessMetricsRecorder;
  private long delay;
  private long window;

  private Backplane backplane;

  @Before
  public void setup() throws IOException {
    this.delay = 10;
    this.window = 100;
    this.backplane = Mockito.mock(Backplane.class);
    casAccessMetricsRecorder =
        new CASAccessMetricsRecorder(
            newSingleThreadScheduledExecutor(),
            backplane,
            Duration.ofMillis(window),
            Duration.ofMillis(delay));
  }

  @After
  public void tearDown() throws InterruptedException {
    casAccessMetricsRecorder.stop();
  }

  @Test
  public void testRecordRead() throws IOException, InterruptedException {
    casAccessMetricsRecorder.start();
    int numberOfUniqueDigests = 100;
    List<Digest> testDigests = new ArrayList<>(numberOfUniqueDigests);
    Map<Digest, AtomicInteger> expectedReadCounts = new HashMap<>(numberOfUniqueDigests);
    for (int i = 0; i < numberOfUniqueDigests; i++) {
      Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
      testDigests.add(randomDigest);
      expectedReadCounts.put(randomDigest, new AtomicInteger(0));
    }

    // Start window from fifth delay window.
    int numberOfDelayCycles = 5;
    int numOfThreads = 5;
    CountDownLatch doneSignal = new CountDownLatch(numOfThreads);

    Thread[] threads = new Thread[numOfThreads];
    for (int i = 0; i < numOfThreads; i++) {
      threads[i] =
          new Thread(
              new RecordAndCountReads(
                  testDigests, expectedReadCounts, numberOfDelayCycles, doneSignal));
      threads[i].start();
    }

    try {
      doneSignal.await();
      Map<Digest, Integer> actualReadCounts =
          getActualReadCounts(numberOfDelayCycles + (int) (window / delay));

      actualReadCounts.forEach(
          (digest, readCount) -> {
            // assert with +1 error, window may change before expected digest read count is
            // updated.
            assertThat(readCount).isAtMost(expectedReadCounts.get(digest).get() + 1);
            assertThat(readCount).isAtLeast(expectedReadCounts.get(digest).get());
          });
    } catch (InterruptedException e) {
      fail("Unexpected exception was thrown.");
    }

    for (Thread thread : threads) {
      thread.join();
    }

    verify(backplane, atLeast(numberOfDelayCycles + (int) (window / delay)))
        .updateCasReadCount(any());
    verify(backplane, times(0)).removeCasReadCountEntries(any());
  }

  @Test
  public void testRecordRead_BeforeStart() {
    Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
    assertThrows(
        IllegalStateException.class, () -> casAccessMetricsRecorder.recordRead(randomDigest));
  }

  @Test
  public void testRecordWrite_BeforeStart() {
    Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
    assertThrows(
        IllegalStateException.class, () -> casAccessMetricsRecorder.recordWrite(randomDigest));
  }

  private Map<Digest, Integer> getActualReadCounts(int expectedIntervalNumber) {
    Map<Digest, Integer> actualReadCounts = new HashMap<>();

    casAccessMetricsRecorder.lock.writeLock().lock();
    try {
      // if due to some reason test threads were unable to stop the metrics recorder on time, skip
      // the test.
      assumeTrue(expectedIntervalNumber == casAccessMetricsRecorder.intervalNumber);
      for (Map<Digest, AtomicInteger> intervalReadCount :
          casAccessMetricsRecorder.getReadIntervalCountQueue()) {
        intervalReadCount.forEach(
            (digest, count) ->
                actualReadCounts.compute(
                    digest, (d, v) -> v == null ? count.get() : v + count.get()));
      }
      return actualReadCounts;
    } finally {
      casAccessMetricsRecorder.lock.writeLock().unlock();
    }
  }

  class RecordAndCountReads implements Runnable {
    List<Digest> testDigests;
    Random rand = new Random();
    Map<Digest, AtomicInteger> expectedDigestAndReadCount;
    int numberOfDelayCycles;
    CountDownLatch doneSignal;

    RecordAndCountReads(
        List<Digest> testDigests,
        Map<Digest, AtomicInteger> expectedDigestAndReadCount,
        int numberOfDelayCycles,
        CountDownLatch doneSignal) {
      this.numberOfDelayCycles = numberOfDelayCycles;
      this.testDigests = testDigests;
      this.expectedDigestAndReadCount = expectedDigestAndReadCount;
      this.doneSignal = doneSignal;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          if (casAccessMetricsRecorder.intervalNumber >= numberOfDelayCycles + window / delay) {
            casAccessMetricsRecorder.stop();
            break;
          }
          Digest digest = testDigests.get(rand.nextInt(testDigests.size()));
          casAccessMetricsRecorder.recordRead(digest);

          // record read counts for 1 full window.
          if (casAccessMetricsRecorder.intervalNumber >= numberOfDelayCycles
              && casAccessMetricsRecorder.intervalNumber < numberOfDelayCycles + window / delay) {
            expectedDigestAndReadCount.get(digest).incrementAndGet();
          }
        }
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      } finally {
        doneSignal.countDown(); // Ensure each thread calls doneSignal once.
      }
    }
  }
}
