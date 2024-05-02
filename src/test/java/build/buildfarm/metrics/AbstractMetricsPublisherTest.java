package build.buildfarm.metrics;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractMetricsPublisherTest {

  @Test
  public void toDurationMs() {
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(t1.getSeconds() + 120).setNanos(200).build();
    // t2 is 60sec+100ns after t1.
    assertThat(AbstractMetricsPublisher.toDurationMs(t1, t2)).isEqualTo(120000.0001);
  }

  @Test
  public void toDurationMsEndBeforeStart_Seconds() {
    // Seconds
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(111).setNanos(t1.getNanos()).build();
    // t2 is _before_ t1, which is a precondition failure
    assertThrows(IllegalStateException.class, () -> AbstractMetricsPublisher.toDurationMs(t1, t2));
  }

  @Test
  public void toDurationMsEndBeforeStart_Nanoseconds() {
    // Nanoseconds
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(777).build();
    Timestamp t2 = Timestamp.newBuilder().setSeconds(t1.getSeconds()).setNanos(222).build();
    // t2 is _before_ t1, which is a precondition failure
    assertThrows(IllegalStateException.class, () -> AbstractMetricsPublisher.toDurationMs(t1, t2));
  }

  /** "Borrow" from the seconds units into 1e9 more nanoseconds. */
  @Test
  public void toDurationBorrow() {
    Timestamp t1 =
        Timestamp.newBuilder()
            .setSeconds(123)
            .setNanos(999999800)
            .build(); // 200 nanoseconds short of a full second.
    Timestamp t2 = Timestamp.newBuilder().setSeconds(125).setNanos(0).build();
    assertThat(AbstractMetricsPublisher.toDurationMs(t1, t2)).isEqualTo(1000.0002);
  }

  @Test
  public void toDurationMsEqual() {
    Timestamp t1 = Timestamp.newBuilder().setSeconds(123).setNanos(100).build();
    Timestamp t2 = Timestamp.newBuilder(t1).build();
    assertThat(t1).isNotSameInstanceAs(t2);
    assertThat(t1.getSeconds()).isEqualTo(t2.getSeconds());
    assertThat(t1.getNanos()).isEqualTo(t2.getNanos());
    assertThat(AbstractMetricsPublisher.toDurationMs(t1, t2)).isEqualTo(0);
  }
}
