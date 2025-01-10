package persistent.common.processes;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.common.Coordinator;
import persistent.common.Coordinator.SimpleCoordinator;
import persistent.common.CtxAround.Id;
import persistent.common.MapPool;
import persistent.common.ObjectPool;
import persistent.common.Worker;

@RunWith(JUnit4.class)
public class CoordinatorTest {
  @SuppressWarnings("CheckReturnValue")
  @Test
  public void simpleTestWorks() throws Exception {
    // Creates an objectpool that uses Strings as a Key for its Workers
    // Workers increment an integer and returns its string value.
    ObjectPool<String, Worker<Integer, String>> spool =
        new MapPool<>(
            key ->
                new Worker<Integer, String>() {
                  @Override
                  public String doWork(Integer request) {
                    return String.valueOf(request + 1);
                  }
                });

    SimpleCoordinator<String, Integer, String, Worker<Integer, String>> pc =
        Coordinator.simple(spool);

    assertThat(pc.runRequest("someWorkerKey", Id.of(1))).isEqualTo(Id.of("2"));
  }
}
